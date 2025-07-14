package raftstore

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

var ErrProcessBeforePropose = errors.New("process before propose")

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	if peer.peerStorage.Engines.Kv != ctx.engine.Kv {
		log.Panicf("kv engine not equal")
	}

	if peer.peerStorage.Engines.Raft != ctx.engine.Raft {
		log.Panicf("raft engine not equal")
	}
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// clear
func (d *peerMsgHandler) matchProposal(entry *eraftpb.Entry) *proposal {
	for len(d.proposals) > 0 {
		p := d.proposals[0]

		if entry.Index < p.index {
			return nil
		}

		if entry.Index > p.index {
			p.cb.Done(ErrRespStaleCommand(p.term))
			d.proposals = d.proposals[1:]
			continue
		}

		if entry.Term == p.term {
			return p
		}

		p.cb.Done(ErrRespStaleCommand(p.term))
		d.proposals = d.proposals[1:]
	}
	return nil
}

func (d *peerMsgHandler) clearStaleAndGetTargetProsal(entry *eraftpb.Entry) *proposal {
	d.clearStaleProsal(entry)

	if len(d.proposals) > 0 && d.proposals[0].index == entry.Index {
		p := d.proposals[0]
		if p.term != entry.Term {
			NotifyStaleReq(entry.Term, p.cb)
			d.proposals = d.proposals[1:]
			return nil
		}
		return p

	}
	return nil
}

func (d *peerMsgHandler) clearStaleProsal(entry *eraftpb.Entry) {
	var i int
	for i = 0; i < len(d.proposals) && d.proposals[i].index < entry.Index; i++ {
		d.proposals[i].cb.Done(ErrResp(&util.ErrStaleCommand{}))
	}

	d.proposals = d.proposals[i:]
}

// 可以很明显看到这个方法和 HeartbeatScheduler() 很像，为什么这么做？
// 是为了快速刷新 scheduler 那里的 region 缓存，能有效的解决你在测试用例里面遇到的 no region 问题。这个方法等会在 split 那里也会派上用场。
func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}

	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) executeCompactLog(admin *raft_cmdpb.AdminRequest, resp *raft_cmdpb.RaftCmdResponse, processBeforePropose bool, _ *message.Callback) (ret_now bool, err error) {
	// compactLog 必须先进行 propose 之后才能进行。
	if processBeforePropose {
		return true, ErrProcessBeforePropose
	}

	compact := admin.CompactLog
	applyState := d.peerStorage.applyState

	// CompactLogRequest 修改数据，更新 RaftApplyState 中的 RaftTruncatedState
	// 向 raftlog-gc 工作者安排一个任务
	if compact.CompactIndex >= applyState.TruncatedState.Index {
		applyState.TruncatedState.Index = compact.CompactIndex
		applyState.TruncatedState.Term = compact.CompactTerm
		wb := &engine_util.WriteBatch{}
		wb.SetMeta(meta.ApplyStateKey(d.regionId), applyState)
		wb.WriteToDB(d.ctx.engine.Kv)

		d.ScheduleCompactLog(compact.CompactIndex)
	}

	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogResponse{},
	}
	resp.Header = &raft_cmdpb.RaftResponseHeader{}
	return false, nil
}

func (d *peerMsgHandler) executeTransferLeader(admin *raft_cmdpb.AdminRequest, resp *raft_cmdpb.RaftCmdResponse, processBeforePropose bool, _ *message.Callback) (ret_now bool, err error) {
	if !processBeforePropose {
		log.Panicf("transfer leader need not propose. it is a action, need not to replcation.")
	}

	d.RaftGroup.TransferLeader(admin.TransferLeader.Peer.Id)

	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
		TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
	}
	resp.Header = &raft_cmdpb.RaftResponseHeader{}
	return false, nil
}

func (d *peerMsgHandler) executeChangePeer(admin *raft_cmdpb.AdminRequest, resp *raft_cmdpb.RaftCmdResponse, processBeforePropose bool, cb *message.Callback) (ret_now bool, err error) {
	if processBeforePropose {
		// 只有两个节点的时候删除 leader，为了防止网络不稳定导致另一个节点不知道 leader 已经被删除。
		// 条件：ConfChange == Remove，d 是 leader，d 是被删除的那个
		if d.IsLeader() && len(d.Region().Peers) > 1 && !d.RaftGroup.Raft.IsConfChange() && admin.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode &&
			admin.ChangePeer.Peer.Id == d.PeerId() && admin.ChangePeer.Peer.StoreId == d.storeID() {

			newLeader, err1 := d.RaftGroup.Raft.TransferLeaderToBest()
			if err1 != nil {
				panic(err1)
			}
			log.DPrintfMsgHandler("[%s]: remove self, transfer leader to %d", d.Tag, newLeader)

			if cb != nil {
				cb.Done(ErrResp(errors.New("raft proposal dropped")))
			}
			return true, nil
		}

		return true, ErrProcessBeforePropose
	}

	err = nil
	// region := d.Region() // 后面操作的时候 region 可能会发生变化？
	// 修改 region.Peers，是删除就删除，是增加就增加一个 peer。如果删除的目标节点正好是自己本身，那么直接调用 d.destroyPeer() 方法销毁自己，并直接 return。后面的操作你都不用管了。
	switch admin.ChangePeer.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		defer log.DPrintfMsgHandler("[%s(%s)] process insert node: %v, after insert: %v", d.Tag, d.RaftGroup.Raft.State, admin.ChangePeer.Peer, d.Region().Peers)

		exist := false
		for _, peer := range d.Region().Peers {
			if peer.Id == admin.ChangePeer.Peer.Id && peer.StoreId == admin.ChangePeer.Peer.StoreId { // 已经存在
				log.DPrintfMsgHandler("[%s(%s)] process insert node: %v, has inserted %v", d.Tag, d.RaftGroup.Raft.State, admin.ChangePeer.Peer, d.Region().Peers)
				exist = true
				break
			}
		}

		if !exist {
			storemeta := d.ctx.storeMeta
			storemeta.Lock()
			d.Region().Peers = append(d.Region().Peers, admin.ChangePeer.Peer)
			d.Region().RegionEpoch.ConfVer++
			d.peerStorage.SetRegion(d.Region())
			// 持久化修改后的 Region，写到 kvDB 里面。使用 meta.WriteRegionState() 方法。注意使用的是 rspb.PeerState_Normal，因为其要正常服务请求的。
			wb := &engine_util.WriteBatch{}
			meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
			wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			wb.WriteToDB(d.peerStorage.Engines.Kv)

			// 调用 d.insertPeerCache() 或 d.removePeerCache() 方法，这决定了你的消息是否能够正常发送，peer.go 里面的 peerCache注释上说明了为什么这么做。
			d.insertPeerCache(admin.ChangePeer.Peer)
			// storemeta.regions[admin.ChangePeer.Peer.Id] = region
			storemeta.regions[d.regionId] = d.Region()
			storemeta.setRegion(d.Region(), d.peer)
			storemeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})

			d.RaftGroup.ApplyConfChange(eraftpb.ConfChange{
				ChangeType: admin.ChangePeer.ChangeType,
				NodeId:     admin.ChangePeer.Peer.Id,
			})
			storemeta.Unlock()
		}
	case eraftpb.ConfChangeType_RemoveNode:
		defer log.DPrintfMsgHandler("[%s(%s)] process remove node: %v after remove: %v", d.Tag, d.RaftGroup.Raft.State, admin.ChangePeer.Peer, d.Region().Peers)

		exist := true
		for i, peer := range d.Region().Peers {
			if peer.Id == admin.ChangePeer.Peer.Id && peer.StoreId == admin.ChangePeer.Peer.StoreId {
				break
			}

			if i == len(d.Region().Peers)-1 { // 到了最后一个，还没找到，不存在
				log.DPrintfMsgHandler("[%s(%s)] process remove node: %v, has removed: %v", d.Tag, d.RaftGroup.Raft.State, admin.ChangePeer.Peer, d.Region().Peers)
				exist = false
				break
			}
		}

		if exist {
			// 删除
			if admin.ChangePeer.Peer.Id == d.PeerId() && admin.ChangePeer.Peer.StoreId == d.storeID() {
				// wb := &engine_util.WriteBatch{}
				// wb.DeleteMeta(meta.ApplyStateKey(d.regionId))
				// wb.WriteToDB(d.peerStorage.Engines.Kv)
				d.destroyPeer() // 直接返回就好，peer 一整个都没了还有什么必要进行后面的操作，嘻嘻）
				return false, nil
			}

			d.ctx.storeMeta.Lock()

			tmp_peers := d.Region().Peers
			d.Region().Peers = make([]*metapb.Peer, 0, len(tmp_peers)-1)
			for _, peer := range tmp_peers {
				if peer.Id != admin.ChangePeer.Peer.Id || peer.StoreId != admin.ChangePeer.Peer.StoreId {
					d.Region().Peers = append(d.Region().Peers, peer)
				}
			}

			d.Region().RegionEpoch.ConfVer++

			// 持久化修改后的 Region，写到 kvDB 里面。使用 meta.WriteRegionState() 方法。注意使用的是 rspb.PeerState_Normal，因为其要正常服务请求的。
			wb := &engine_util.WriteBatch{}
			meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
			wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			wb.WriteToDB(d.peerStorage.Engines.Kv)

			d.removePeerCache(admin.ChangePeer.Peer.Id)

			d.RaftGroup.ApplyConfChange(eraftpb.ConfChange{
				ChangeType: admin.ChangePeer.ChangeType,
				NodeId:     admin.ChangePeer.Peer.Id,
			})
			d.ctx.storeMeta.Unlock()
		}
	default:
		log.Panicf("impossible changeType. %s", admin.ChangePeer.ChangeType)
	}

	if d.stopped {
		return true, nil
	}

	// 调用 notifyHeartbeatScheduler() 方法
	d.notifyHeartbeatScheduler(d.Region(), d.peer)

	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
		ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: d.Region()},
	}

	resp.Header = &raft_cmdpb.RaftResponseHeader{}
	return false, err
}

func (d *peerMsgHandler) executeSplitRegion(admin *raft_cmdpb.AdminRequest, resp *raft_cmdpb.RaftCmdResponse, processBeforePropose bool, _ *message.Callback) (ret_now bool, err error) {
	if processBeforePropose {
		return true, ErrProcessBeforePropose
	}

	// 拷贝得到新 region
	leftRegion := d.Region()
	rightRegion := &metapb.Region{}
	rawRegion := &metapb.Region{}
	err = util.CloneMsg(d.Region(), rightRegion)
	if err != nil {
		panic(err)
	}

	err = util.CloneMsg(d.Region(), rawRegion)
	if err != nil {
		panic(err)
	}

	// split.NewPeerIds 初始化 peers
	newPeers := make([]*metapb.Peer, 0)
	hasCurrentStore := false
	if len(leftRegion.Peers) != len(admin.Split.NewPeerIds) {
		return false, errors.Errorf("split.NewPeerIds length not equal to leftRegion.Peers length, split.NewPeerIds: %v, leftRegion.Peers: %v", admin.Split.NewPeerIds, leftRegion.Peers)
	}

	for i, peer := range leftRegion.Peers {
		if i < len(admin.Split.NewPeerIds) {
			newPeers = append(newPeers, &metapb.Peer{
				Id:      admin.Split.NewPeerIds[i],
				StoreId: peer.StoreId,
			})
			if peer.StoreId == d.ctx.store.GetId() {
				hasCurrentStore = true
			}
		}
	}

	if len(newPeers) == 0 || !hasCurrentStore {
		return false, errors.Errorf("new peers length is 0 or not contains current store, new peers: %v, current store: %v", newPeers, d.ctx.store.GetId())
	}

	// split.NewRegionId 初始化 id
	rightRegion.Id = admin.Split.NewRegionId

	// split.SplitKey 初始化 key
	rightRegion.StartKey = admin.Split.SplitKey
	leftRegion.EndKey = admin.Split.SplitKey

	// 修改 version
	leftRegion.RegionEpoch.Version++
	rightRegion.RegionEpoch.Version++

	// 持久化两个 region
	wb := &engine_util.WriteBatch{}
	meta.WriteRegionState(wb, leftRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(wb, rightRegion, rspb.PeerState_Normal)
	wb.WriteToDB(d.peerStorage.Engines.Kv)

	// 调用 createPeer 创建 newPeer，利用 d.ctx.router 注册和发送 MsgTypeStart 启动节点
	newpeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.peerStorage.Engines, rightRegion)
	if err != nil {
		log.Errorf("create new peer for %v err %v", rightRegion, err)
	}

	d.ctx.router.register(newpeer)
	_ = d.ctx.router.send(rightRegion.GetId(), message.Msg{Type: message.MsgTypeStart})

	// 修改 storeMeta
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	storeMeta.regionRanges.Delete(&regionItem{region: rawRegion})

	storeMeta.regions[leftRegion.Id] = leftRegion
	storeMeta.regions[rightRegion.Id] = rightRegion

	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: leftRegion})
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: rightRegion})

	storeMeta.setRegion(leftRegion, d.peer)
	storeMeta.setRegion(rightRegion, newpeer)
	storeMeta.Unlock()

	// 清理 SizeDiffHint 和 ApproximateSize
	d.SizeDiffHint = 0
	d.ApproximateSize = new(uint64)

	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_Split,
		Split: &raft_cmdpb.SplitResponse{
			Regions: []*metapb.Region{leftRegion, rightRegion},
		},
	}

	resp.Header = &raft_cmdpb.RaftResponseHeader{}

	// 两个 region 都调用并发送心跳。
	d.notifyHeartbeatScheduler(leftRegion, d.peer)
	d.notifyHeartbeatScheduler(rightRegion, newpeer)
	return false, nil
}

// 有的命令需要进行 propose 比如：压缩操作 CompactLog 需要先在 raft 中完成共识才能压缩，相当于先商量一个压缩点，在 propose 点以及之前的全部都被压缩，之后的没有。
// 有的命令不需要进行 propose 比如：TransferLeader 只需要在 raft 中完成共识即可，不需要写入数据。
func (d *peerMsgHandler) processAdminRequest(entry *eraftpb.Entry, cmd *raft_cmdpb.RaftCmdRequest, cb *message.Callback, processBeforePropose bool) error {
	admin := cmd.AdminRequest

	p := d.clearStaleAndGetTargetProsal(entry)
	if cb == nil && p != nil {
		cb = p.cb
	}

	resp := &raft_cmdpb.RaftCmdResponse{}
	err := util.CheckRegionEpoch(cmd, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		log.Errorf("epoch not match, %v", errEpochNotMatching)
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		err = errEpochNotMatching

		if cb != nil {
			cb.Done(ErrResp(err))
		}

		if p != nil {
			d.proposals = d.proposals[1:]
		}
		return err
	}

	switch admin.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		ret_now, perr := d.executeCompactLog(admin, resp, processBeforePropose, cb)

		if ret_now {
			return perr
		}

		err = perr
	case raft_cmdpb.AdminCmdType_TransferLeader:
		ret_now, perr := d.executeTransferLeader(admin, resp, processBeforePropose, cb)

		if ret_now {
			return perr
		}

		err = perr
	case raft_cmdpb.AdminCmdType_ChangePeer:
		ret_now, perr := d.executeChangePeer(admin, resp, processBeforePropose, cb)
		if ret_now {
			return perr
		}

		err = perr
	case raft_cmdpb.AdminCmdType_Split:
		ret_now, perr := d.executeSplitRegion(admin, resp, processBeforePropose, cb)
		if ret_now {
			return perr
		}

		err = perr
	default:
		log.Panicf("unimplemented admin cmd %s for %s", admin.CmdType, d.Tag)
	}

	if cb != nil {
		if err != nil {
			cb.Done(ErrResp(err))
		} else {
			cb.Done(resp)
		}
	}

	if p != nil {
		d.proposals = d.proposals[1:]
	}
	return nil
}

func (d *peerMsgHandler) process(entry *eraftpb.Entry) {
	if len(entry.Data) == 0 {
		return
	}

	var cmd raft_cmdpb.RaftCmdRequest
	var err error

	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		var cc eraftpb.ConfChange

		// 先解码成 ConfChange
		if err = cc.Unmarshal(entry.Data); err != nil {
			panic(err)
		}

		// 之后解码成 cmd 解码方式不一样
		if err = cmd.Unmarshal(cc.Context); err != nil {
			panic(err)
		}
	} else {
		if err = cmd.Unmarshal(entry.Data); err != nil {
			panic(err)
		}
	}

	// 检查 epoch
	err = util.CheckRegionEpoch(&cmd, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		log.Errorf("epoch not match, %v", errEpochNotMatching)
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		err = errEpochNotMatching

		p := d.matchProposal(entry)

		if p != nil {
			p.cb.Done(ErrResp(err))
			d.proposals = d.proposals[1:]
		}
		return
	}

	log.DPrintfMsgHandler("%s process %v", d.Tag, cmd)

	if cmd.AdminRequest != nil {
		err = d.processAdminRequest(entry, &cmd, nil, false)
		if err != nil {
			log.Panicf("unexpected error %v", err)
		}
	}

	// 处理普通指令。
	for _, req := range cmd.Requests {
		if d.stopped {
			return
		}

		resp := &raft_cmdpb.RaftCmdResponse{
			Header:    &raft_cmdpb.RaftResponseHeader{},
			Responses: []*raft_cmdpb.Response{},
		}

		wb := &engine_util.WriteBatch{}
		key := ([]byte)(nil)

		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			key = req.Put.GetKey()
			wb.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
		case raft_cmdpb.CmdType_Delete:
			key = req.Delete.GetKey()
			wb.DeleteCF(req.Delete.Cf, req.Delete.Key)
		case raft_cmdpb.CmdType_Get:
			key = req.Get.GetKey()
		case raft_cmdpb.CmdType_Snap:
			// do nothing
		}

		matchedProposal := d.matchProposal(entry)

		// 检查是否在 region 范围内
		if req.CmdType != raft_cmdpb.CmdType_Snap {
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				if matchedProposal != nil {
					matchedProposal.cb.Done(ErrResp(err))
					d.proposals = d.proposals[1:]
				}
				continue
			}
		}

		if matchedProposal != nil {
			switch req.CmdType {
			case raft_cmdpb.CmdType_Put:
				d.SizeDiffHint += uint64(len(req.Put.Key) + len(req.Put.Value))

				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				})
			case raft_cmdpb.CmdType_Delete:
				d.SizeDiffHint -= uint64(len(req.Delete.Key))

				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				})
			case raft_cmdpb.CmdType_Get:
				val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
				if err != nil {
					panic(err)
				}
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get:     &raft_cmdpb.GetResponse{Value: val},
				})
			case raft_cmdpb.CmdType_Snap:
				region := &metapb.Region{}
				err1 := util.CloneMsg(d.Region(), region)
				if err1 != nil {
					panic(err1)
				}

				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap:    &raft_cmdpb.SnapResponse{Region: region},
				})
				matchedProposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			default:
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Invalid,
				})
			}
		}

		d.peerStorage.applyState.AppliedIndex = entry.Index
		wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		wb.WriteToDB(d.peerStorage.Engines.Kv)

		if matchedProposal != nil {
			matchedProposal.cb.Done(resp)
			d.proposals = d.proposals[1:]
		}
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}

	if d.RaftGroup.HasReady() {
		ready := d.RaftGroup.Ready()

		applySnapResult, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			return
		}

		// 如果应用了快照，需要更新peer的region信息
		if applySnapResult != nil {
			if !reflect.DeepEqual(applySnapResult.PrevRegion, applySnapResult.Region) {
				log.DPrintfMsgHandler("[%s] apply snapshot, region changed from %v to %v", d.Tag, applySnapResult.PrevRegion, applySnapResult.Region)
				d.SetRegion(applySnapResult.Region)
				metaStore := d.ctx.storeMeta
				metaStore.Lock()
				metaStore.regions[applySnapResult.Region.GetId()] = applySnapResult.Region
				metaStore.regionRanges.Delete(&regionItem{region: applySnapResult.PrevRegion})
				metaStore.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapResult.Region})
				metaStore.Unlock()

				d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
			} else {
				log.DPrintfMsgHandler("[%s] apply snapshot, region not changed", d.Tag)
			}
		}

		if len(ready.Messages) > 0 {
			d.Send(d.ctx.trans, ready.Messages) // 发送消息。
		}

		for _, entry := range ready.CommittedEntries {
			if d.stopped {
				return
			}

			d.process(&entry)
		}

		d.RaftGroup.Advance(ready)
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.DPrintfMsgHandler("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	// Let Snap requests go through Raft to ensure read-after-write consistency
	// This ensures that Snap requests see all committed data
	// Your Code Here (2B).

	// 消息编码
	data, marErr := msg.Marshal()
	if marErr != nil {
		cb.Done(ErrResp(marErr))
		return
	}

	// 有的管理员命令不需要 propose
	if msg.AdminRequest != nil {
		err := d.processAdminRequest(&eraftpb.Entry{Data: data}, msg, cb, true)
		if err == nil {
			return
		} else if err != ErrProcessBeforePropose {
			log.Panicf("unexpected error %v", err)
		} else {
			// do nothing
		}
	}

	// Get the index that will be assigned to this proposal
	// We need to get this after marshaling but before proposing
	perr := error(nil)
	p := (*proposal)(nil)

	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer {
		p = &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		}

		// changePeer 编码方式不一样, 需要调用不同的 propose 方法
		perr = d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
			ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
			NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
			Context:    data,
		})
	} else {
		if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_Split {
			perr = util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region())
		}

		if perr == nil {
			p = &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				cb:    cb,
			}

			// 将解码的消息交给 rawnode 进行 propose
			perr = d.RaftGroup.Propose(data)
		}
	}

	if perr != nil {
		cb.Done(ErrResp(perr))
		return
	}

	d.proposals = append(d.proposals, p)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.DPrintfMsgHandler("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err1)
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		log.Errorf("%s step %s error %v", d.Tag, msg, err)
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.DPrintfMsgHandler("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.DPrintfMsgHandler("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.DPrintfMsgHandler("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.DPrintfMsgHandler("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.DPrintfMsgHandler("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.DPrintfMsgHandler("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.DPrintfMsgHandler("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.DPrintfMsgHandler("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.DPrintfMsgHandler("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.DPrintfMsgHandler("%s starts destroy, region: %v", d.Tag, d.ctx.storeMeta.regions)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil && isInitialized {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.DPrintfMsgHandler("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.DPrintfMsgHandler("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.DPrintfMsgHandler("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.DPrintfMsgHandler("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.DPrintfMsgHandler("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
