// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.

	// peers 包含 raft 集群中所有节点（包括自身）的 ID。
	// 仅在启动新 raft 集群时设置。
	// 如果在从先前配置重启 raft 时设置 peers，会导致 panic。
	// peers 是私有的，目前仅用于测试。
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.

	// ElectionTick 是两次选举之间必须经过的 Node.Tick 调用次数。
	// 也就是说，如果 follower 在 ElectionTick 时间内没有收到当前任期 leader 的任何消息，
	// 它将变为 candidate 并发起选举。ElectionTick 必须大于 HeartbeatTick。
	// 我们建议设置 ElectionTick = 10 * HeartbeatTick，以避免不必要的 leader 切换。
	ElectionTick int

	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	// HeartbeatTick 是两次心跳之间必须经过的 Node.Tick 调用次数。
	// 也就是说，leader 每隔 HeartbeatTick 个 tick 就会发送一次心跳消息以维持其领导地位。
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	// Storage 是 raft 的存储。raft 生成的日志条目和状态会存储在 Storage 中。
	// raft 需要时会从 Storage 读取持久化的日志条目和状态。
	// raft 重启时会从 Storage 读取之前的状态和配置信息。
	Storage Storage

	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	// Applied 是最后一次被应用的索引。仅在重启 raft 时设置。
	// raft 不会返回小于或等于 Applied 的日志条目给应用层。
	// 如果重启时未设置 Applied，raft 可能会返回之前已应用的日志条目。
	// 这是一个非常依赖于应用的配置项。
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
	id          uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send 由上层测试方进行调用发送信息，
	// 这里只需要把信息塞进这里面就相当于发了消息。
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int

	// baseline of election interval
	electionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	// 距离上次达到 heartbeatTimeout 已经过的 tick 数。
	// 只有 leader 会维护 heartbeatElapsed。
	// 并非随机
	heartbeatElapsed int

	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	// 当为 leader 或 candidate 时，距离上次达到 electionTimeout 的 tick 数。
	// 当为 follower 时，表示距离上次达到 electionTimeout 或收到当前 leader 的有效消息的 tick 数。
	// 是随机
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	// leadTransferee 在其值不为零时，表示 leader 转移目标的 id。
	// 遵循 Raft 博士论文第 3.10 节中定义的流程。
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// （用于 3A leader 转移）
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	// 一次只能有一个配置变更处于挂起状态（已写入日志但尚未应用）。
	// 通过 PendingConfIndex 强制执行这一点，
	// PendingConfIndex 被设置为最新挂起配置变更的日志索引（如果有的话）或更大值。
	// 只有当 leader 的 applied index 大于该值时，才允许提出配置变更。
	// （用于 3A 配置变更）
	PendingConfIndex uint64
	
	// AddingNode       uint64 // 新增加的节点。
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	prs := make(map[uint64]*Progress)
	hardstate, confstate, err := c.Storage.InitialState()

	if err != nil {
		panic(err.Error())
	}

	if len(c.peers) > 0 {
		for _, peer := range c.peers {
			prs[peer] = &Progress{
				Match: 0,
				Next:  1,
				id:    peer,
			}
		}
	} else {
		for _, peer := range confstate.Nodes {
			prs[peer] = &Progress{
				Match: 0,
				Next:  1,
				id:    peer,
			}
		}
	}

	raft := &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             None,
		RaftLog:          newLog(c.Storage),
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: c.HeartbeatTick,
		electionElapsed:  c.ElectionTick,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}

	raft.resetElapsed()
	raft.Term = hardstate.Term
	raft.Vote = hardstate.Vote

	// 如果Config中指定了Applied，则使用它来覆盖默认的applied值
	// 同时也需要设置committed，因为快照中的所有条目都已经被提交了
	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
		raft.RaftLog.committed = max(raft.RaftLog.committed, c.Applied)
	}

	raft.RaftLog.check()
	return raft
}

func (r *Raft) resetElapsed() {
	r.heartbeatElapsed = r.heartbeatTimeout
	r.electionElapsed = rand.Intn(r.electionTimeout) + r.electionTimeout
}

func maybeLeader(votes map[uint64]bool, tot int) bool {
	cnt := 0
	for _, v := range votes {
		if v {
			cnt++
		}
	}

	haveNotReceive := tot - len(votes)
	return cnt+haveNotReceive > tot/2
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed--
	}
	r.electionElapsed--

	if r.electionElapsed <= 0 {
		// 如果是leader且正在进行leader transfer，则中止transfer
		if r.State == StateLeader && r.leadTransferee != None {
			r.abortTransferLeader()
		}

		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}

	if r.heartbeatElapsed <= 0 && r.State == StateLeader {
		for _, peer := range r.Prs {
			if peer.id == r.id {
				continue
			}
			r.sendHeartbeat(peer.id)
		}

		r.resetElapsed()
		// 移除这里的 abortTransferLeader() 调用
		// r.abortTransferLeader()
	}
}

func appendLog(r *Raft, ents []*pb.Entry, preLogTerm uint64, preLogIndex uint64) {
	r.RaftLog.appendLog(ents, preLogTerm, preLogIndex)

	if peer, ok := r.Prs[r.id]; ok {
		peer.Match = max(peer.Match, r.RaftLog.LastIndex())
		peer.Next = peer.Match + 1
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Vote = None
	r.Lead = lead
	r.State = StateFollower
	r.resetElapsed()
	r.abortTransferLeader()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Vote = None
	r.State = StateCandidate
	r.Lead = None
	r.resetElapsed()

	r.votes = make(map[uint64]bool) // 清空
	r.abortTransferLeader()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	r.Vote = None
	r.State = StateLeader
	r.votes = make(map[uint64]bool) // 清空
	r.Lead = r.id

	r.resetElapsed()
	r.abortTransferLeader()

	for _, peer := range r.Prs {
		peer.Match = 0
		peer.Next = r.RaftLog.LastIndex() + 1
	}

	// 添加一个空条目
	appendLog(r, []*pb.Entry{{Term: r.Term, Index: r.RaftLog.LastIndex() + 1, Data: nil}}, r.Term, r.RaftLog.LastIndex())

	log.DPrintfRaft("[raft %d] become leader", r.id)
}

func (r *Raft) canBeLeader() bool {
	switch r.State {
	case StateFollower:
		return false
	case StateLeader:
		return true
	}

	cnt := 0
	learnerCnt := 0
	for _, v := range r.votes {
		if v {
			cnt++
		}
	}

	return cnt > (len(r.Prs)-learnerCnt)/2
}

// 判断是否可以成为 leader，如果可以，则成为 leader 并发出 propose，并返回 true，否则返回 false。
func becomeLeader(r *Raft, term uint64) bool {
	if !r.canBeLeader() {
		if !maybeLeader(r.votes, len(r.Prs)) {
			r.becomeFollower(term, None) // 选举失败，变成 follower
		}
		return false
	}

	r.becomeLeader()

	for _, peer := range r.Prs {
		if peer.id == r.id {
			continue
		}

		r.sendAppend(peer.id)
	}

	leaderCommit(r, r.RaftLog.LastIndex())
	return true
}

func leaderCommit(r *Raft, index uint64) bool {
	if r.State != StateLeader {
		log.DPrintfRaft("leaderCommit called in %s state", r.State.String())
		return false
	}

	if r.RaftLog.LastIndex() < index || r.RaftLog.committed > index {
		return false
	}

	log_term, err := r.RaftLog.Term(index)
	if err != nil {
		if err == ErrCompacted {
			// The index has been compacted or the entry doesn't exist,
			// which means it's already committed or unavailable
			// We can safely skip this commit request
			log.DPrintfRaft("leaderCommit: index %d is unavailable (%s), skipping", index, err.Error())
			return false
		}
		log.Panicf("leaderCommit called with invalid index: %d, err : %s", index, err.Error())
	}

	if log_term < r.Term {
		return false
	}

	cnt := 0
	learnerCnt := 0
	for _, peer := range r.Prs {
		if peer.Match >= index {
			cnt++
		}
	}

	if cnt > (len(r.Prs)-learnerCnt)/2 && r.RaftLog.committed < index {
		r.RaftLog.commitTo(index, index)

		for _, peer := range r.Prs {
			if peer.id == r.id {
				continue
			}
			r.sendAppend(peer.id) // 广播，通知已经提交了
		}

		return true
	}

	return false
}

func (r *Raft) TransferLeaderToBest() (uint64, error) {
	if r.State != StateLeader {
		log.Errorf("[raft %d] TransferLeaderToBest called on non-leader", r.id)
		return 0, nil
	}

	best := uint64(0)

	for id := range r.Prs {
		// 找到第一个不是自己的节点
		if id != r.id {
			best = id
			break
		}
	}

	for _, peer := range r.Prs {
		if peer.id == r.id {
			continue
		}

		// 寻找最优
		if peer.Match > r.Prs[best].Match {
			best = peer.id
		}
	}

	// 因为这个函数调用于 leader 转移，肯定至少有两个节点，所以 best 不可能为 0
	if best == r.id || best == 0 {
		log.Panicf("[raft %d] TransferLeaderToBest called with no best leader, prs: %v", r.id, r.Prs)
		return 0, nil
	}

	return best, r.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: best})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	log.DPrintfRaft("[raft[%d](state:%s)] from: %d, receive %s, term: %d, commit: %d, index: %d, reject: %v\n", r.id, r.State.String(), m.From, m.MsgType.String(), m.Term, m.Commit, m.Index, m.Reject)

	switch m.MsgType {
	// 'MessageType_MsgHup' is a local message used for election. If an election timeout happened,
	// the node should pass 'MessageType_MsgHup' to its Step method and start a new election.
	case pb.MessageType_MsgHup:
		{
			return r.handleHup(m)
		}
	// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
	// of the 'MessageType_MsgHeartbeat' type to its followers.
	case pb.MessageType_MsgBeat:
		{
			return r.handleBeat(m) // different from heartbeat, it is only used by leader
		}
		// 'MessageType_MsgHeartbeat' sends heartbeat from leader to its followers.
	case pb.MessageType_MsgHeartbeat:
		{
			return r.handleHeartbeat(m)
		}
	// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
	case pb.MessageType_MsgPropose:
		{
			return r.handlePropose(m)
		}
	// 'MessageType_MsgAppend' contains log entries to replicate.
	case pb.MessageType_MsgAppend:
		{
			return r.handleAppendEntries(m)
		}
	// 'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend').
	case pb.MessageType_MsgAppendResponse:
		{
			return r.handleAppendResponse(m)
		}
	// 'MessageType_MsgRequestVote' requests votes for election.
	case pb.MessageType_MsgRequestVote:
		{
			return r.handleRequestVote(m)
		}
	// 'MessageType_MsgRequestVoteResponse' contains responses from voting request.
	case pb.MessageType_MsgRequestVoteResponse:
		{
			return r.handleRequestVoteResponse(m)
		}
	// 'MessageType_MsgSnapshot' requests to install a snapshot message.
	case pb.MessageType_MsgSnapshot:
		{
			return r.handleSnapshot(m)
		}
	// 'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'.
	case pb.MessageType_MsgHeartbeatResponse:
		{
			return r.handleHeartbeatResponse(m)
		}
	// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
	case pb.MessageType_MsgTransferLeader:
		{
			return r.handleTransferLeader(m)
		}
	// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
	// the transfer target timeout immediately and start a new election.
	case pb.MessageType_MsgTimeoutNow:
		{
			return r.handleTimeoutNow(m)
		}
	default:
		{
			log.Panicf("unsupported message type: %v", m.MsgType)
		}
	}

	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) error {
	// Your Code Here (2A).
	if r.Term > m.Term {
		return nil
	}

	r.becomeFollower(m.Term, m.From)

	if m.Index > r.RaftLog.LastIndex() {
		// index 不存在
		return r.sendAppendResponse(m.From, false, 0, r.RaftLog.LastIndex(), nil, nil)
	}

	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		// term 不对应， 需要返回 firstindexof term
		conflictTerm := uint64(0)
		conflictIndex := m.Index

		if err == nil {
			conflictTerm = term
			conflictIndex = r.RaftLog.findFirstIndexOfTerm(conflictTerm)
		}
		return r.sendAppendResponse(m.From, false, conflictTerm, conflictIndex, nil, nil)
	}

	appendLog(r, m.Entries, m.LogTerm, m.Index)
	r.sendAppendResponse(m.From, true, r.Term, r.RaftLog.LastIndex(), m.Entries, nil)

	// 如果消息带了新日志条目，last new entry index 就是新日志的最大 index
	// 如果消息没带新日志条目，last new entry index 就是 prevLogIndex
	if len(m.Entries) == 0 {
		r.RaftLog.commitTo(m.Commit, m.Index)
	} else {
		r.RaftLog.commitTo(m.Commit, m.Entries[len(m.Entries)-1].Index)
	}

	return nil
}

// 给所有人发一个heartbeat，用于leader检查自己是否是最新。
func (r *Raft) handleBeat(m pb.Message) error {
	if r.State != StateLeader {
		log.DPrintfRaft("handleBeat called in %s state", r.State.String())
		return nil
	}

	for _, peer := range r.Prs {
		if peer.id == r.id {
			continue
		}

		r.sendHeartbeat(peer.id) // 忽视
	}

	return nil
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) error {
	// Your Code Here (2A).

	if r.Term > m.Term {
		log.Debugf("[raft %d] ignore heartbeat with lower term %d from %d, current term is %d", r.id, m.Term, m.From, r.Term)
		return r.sendHeartbeatResponse(m.From)
	}

	if r.Term <= m.Term {
		r.becomeFollower(m.Term, m.From)
	}

	return r.sendHeartbeatResponse(m.From)
}

func (r *Raft) handleHup(m pb.Message) error {
	if r.State == StateLeader {
		log.DPrintfRaft("handleHup called in leader state")
		return nil
	}

	r.becomeCandidate()

	for _, peer := range r.Prs {
		if peer.id == r.id {
			continue
		}

		r.sendRequestVote(peer.id) // 忽视
	}

	// 如果一个节点是刚被 add 进入集群的，那么它的 prs 信息为空，包括自己也是空，可以通过这个来拒绝这个情况成为 leader
	if _, ok := r.Prs[r.id]; ok {
		r.votes[r.id] = true
		r.Vote = r.id
	}

	becomeLeader(r, r.Term)

	return nil
}

func (r *Raft) handleRequestVote(m pb.Message) error {
	agree := false

	// 如果候选人收到其他候选人的拉票、而且拉票的任期编号不小于自己的任期编号，就会自认落选，成为追随者，并认定来拉票的候选人为领袖。from wiki
	if r.Term < m.Term {
		// 如果 term 小，说明发生了一个新的 election，那么直接成为这个候选者的 follower，并投票，相当于给第一个选举的节点投票
		r.Term = m.Term
		r.Vote = m.From
		r.Lead = None
		r.State = StateFollower
		// r.resetElapsed() // 不需要重置，假设这种情况：一个拥有较旧日志的 follower 先 timeout，它的 term 大导致其他节点一直没办法 timeout。
		r.abortTransferLeader()
		agree = true
	}

	if r.RaftLog.LastTerm() > m.LogTerm { // 如果拉票的任期编号大于自己的任期编号，则拒绝
		agree = false
		log.DPrintfRaft("raft %d reject vote from %d, because its last term %d is greater than %d", r.id, m.From, r.RaftLog.LastTerm(), m.LogTerm)
	} else if r.RaftLog.LastTerm() == m.LogTerm && r.RaftLog.LastIndex() > m.Index { // 相等的时候比较索引，如果索引不同，则拒绝。
		agree = false
		log.DPrintfRaft("raft %d reject vote from %d, because its last index %d is greater than %d", r.id, m.From, r.RaftLog.LastIndex(), m.Index)
	} else if r.Term > m.Term {
		// 如果 m.term 小于当前 r.term，则拒绝
		agree = false
		log.DPrintfRaft("raft %d reject vote from %d, because its term %d is greater than %d", r.id, m.From, r.Term, m.Term)
	} else if r.Vote != None && r.Vote != m.From {
		// 如果这个 term 已经投过票，并且不是给这个节点投票，则拒绝
		// 如果是当前状态 candidate， 会经过这个分支。
		agree = false
		log.DPrintfRaft("raft %d has vote %d, reject vote from %d", r.id, r.Vote, m.From)
	} else if r.State == StateFollower {
		// 现在 term 相同，同时没投过票
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
		agree = true
	} else {
		// 这里，term 相同，但是 state 不是 follower，也不是 candidate，断言，一定是 leader
		if r.State != StateLeader {
			log.Panicf("raft %d recive a request vote from %d, but state is %s", r.id, m.From, r.State.String())
		}

		// 投票延时到达可能是网络延迟，也可能是其他原因，这里直接拒绝
		log.DPrintfRaft("raft %d recive a request vote from %d, may be network latency", r.id, m.From)
		agree = false
	}

	r.sendRequestVoteResponse(m.From, r.Term, agree)
	return nil
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) error {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		return nil
	}

	if !r.hasPeer(m.From) {
		return nil
	}

	if r.State == StateCandidate {
		r.votes[m.From] = !m.Reject

		becomeLeader(r, m.Term)
	}

	return nil
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) error {
	// Your Code Here (2C).
	if r.Term > m.Term {
		log.Errorf("[raft %d] handleSnapshot called with term %d, but current term is %d", r.id, m.Term, r.Term)
		return nil
	}

	if r.RaftLog.pendingSnapshot != nil {
		log.Errorf("[raft %d] handleSnapshot called with pending snapshot", r.id)
		return nil
	}

	snapMeta := m.Snapshot.Metadata
	if r.RaftLog.committed >= snapMeta.Index {
		return r.sendAppendResponse(m.From, false, r.Term, r.RaftLog.committed, nil, nil)
	}

	// 接受一个快照，需要把所有东西都清空。
	log.DPrintfRaft("[raft %d] handle snapshot: %v, data: %v", r.id, m.Snapshot.Metadata, m.Snapshot.Data)
	r.becomeFollower(m.Term, m.From)

	r.RaftLog.commitTo(snapMeta.Index, snapMeta.Index)
	r.RaftLog.appliedTo(snapMeta.Index)
	r.RaftLog.stableTo(snapMeta.Index)
	r.RaftLog.firstIndexTo(snapMeta.Index + 1)
	r.RaftLog.entries = make([]pb.Entry, 0) // 清空
	r.RaftLog.pendingSnapshot = m.Snapshot

	r.Prs = make(map[uint64]*Progress)
	for _, id := range snapMeta.ConfState.Nodes {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  snapMeta.Index + 1,
			id:    id,
		}
	}

	return r.sendAppendResponse(m.From, true, r.Term, r.RaftLog.LastIndex(), nil, nil)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) error {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
	}

	if r.State == StateLeader {
		if m.Commit < r.RaftLog.committed && r.State == StateLeader {
			return r.sendAppend(m.From)
		}

		// 添加检查，即使响应
		if _, ok := r.Prs[m.From]; ok && r.Prs[m.From].Match < r.RaftLog.LastIndex() && r.State == StateLeader {
			return r.sendAppend(m.From)
		}
	}
	return nil
}

func (r *Raft) handlePropose(m pb.Message) error {
	if r.State != StateLeader || r.leadTransferee != None {
		return ErrProposalDropped
	}

	if len(m.Entries) == 0 {
		return nil
	}

	// 只需要拒绝后续的 confchange，不需要拒绝普通 propose
	if m.Entries[0].EntryType == pb.EntryType_EntryConfChange {
		if r.IsConfChange() {
			return ErrProposalDropped
		}
		r.PendingConfIndex = r.RaftLog.LastIndex() + 1
	}

	for _, ent := range m.Entries {
		ent.Term = r.Term
	}

	appendLog(r, m.Entries, r.Term, r.RaftLog.LastIndex())

	err := error(nil)
	for _, peer := range r.Prs {
		if peer.id == r.id {
			continue
		}

		err1 := r.sendAppend(peer.id) // 忽视
		if err1 != nil {
			err = err1
			if err1 != ErrSnapshotTemporarilyUnavailable {
				return err1
			}
		}
	}

	leaderCommit(r, r.RaftLog.LastIndex())
	return err
}

func (r *Raft) handleAppendResponse(m pb.Message) error {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		return nil
	}

	if r.State != StateLeader {
		return nil
	}

	if !r.hasPeer(m.From) {
		return nil
	}

	if m.Reject {
		if m.Term == 0 { // index 不存在
			r.Prs[m.From].Next = m.Index + 1
		} else { // term 不一致
			lastIndexOfTerm := r.RaftLog.findLastIndexOfTerm(m.Term)

			if lastIndexOfTerm != 0 {
				r.Prs[m.From].Next = lastIndexOfTerm + 1
			} else {
				r.Prs[m.From].Next = m.Index
			}
		}

		return r.sendAppend(m.From)
	}

	r.Prs[m.From].Match = max(r.Prs[m.From].Match, min(r.RaftLog.LastIndex(), m.Index))
	r.Prs[m.From].Next = r.Prs[m.From].Match + 1

	leaderCommit(r, r.Prs[m.From].Match)

	// 把新节点给扶持之后再推进。
	// if m.From == r.AddingNode && r.Prs[m.From].Match >= r.RaftLog.applied {
	// 	r.AddingNode = None
	// }

	if r.leadTransferee == m.From && r.Prs[m.From].Match >= r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
		r.sendTimeoutNow(m.From)
		log.DPrintfRaft("%d sends MsgTimeoutNow to %d, it's log has up-to-date.", r.id, m.From, m.From)
	}
	return nil
}

func (r *Raft) handleTimeoutNow(m pb.Message) error {
	if !r.hasPeer(m.From) || !r.hasPeer(r.id) {
		return nil
	}

	// 如果已经发过了 timeout，如果还有机会成为 leader 就进行尝试，否则再来一轮。
	if r.State == StateCandidate && maybeLeader(r.votes, len(r.Prs)) {
		for _, peer := range r.Prs {
			if peer.id == r.id {
				continue
			}

			r.sendRequestVote(peer.id)
		}
		return nil
	}

	return r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
}

func (r *Raft) handleTransferLeader(m pb.Message) error {
	if r.State != StateLeader {
		// 转发给 leader
		return r.sendLeaderTransfer(r.Lead, m.From)
	}

	if !r.hasPeer(m.From) {
		return nil
	}

	leadTransferee := m.From
	lastLeadTransferee := r.leadTransferee
	if lastLeadTransferee != None {
		if lastLeadTransferee == leadTransferee {
			log.DPrintfRaft("%d [term %d] transfer leadership to %d is in progress, msg maybe loss, try again",
				r.id, r.Term, leadTransferee)

			if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
				r.sendTimeoutNow(leadTransferee)
				r.sendTimeoutNow(leadTransferee)
				log.DPrintfRaft("%d sends MsgTimeoutNow to %d immediately as %d already has up-to-date log", r.id, leadTransferee, leadTransferee)
			} else {
				r.sendAppend(leadTransferee)
			}
			return nil
		}
		r.abortTransferLeader()
		log.DPrintfRaft("%d [term %d] abort previous transferring leadership to %d", r.id, r.Term, lastLeadTransferee)
	}
	if leadTransferee == r.id {
		log.DPrintfRaft("%d is already leader. Ignored transferring leadership to self", r.id)
		return nil
	}
	// Transfer leadership to third party.
	log.DPrintfRaft("%d [term %d] starts to transfer leadership to %d", r.id, r.Term, leadTransferee)

	// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
	r.electionElapsed = r.electionTimeout
	r.leadTransferee = leadTransferee

	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(leadTransferee)
		r.sendTimeoutNow(leadTransferee)
		log.DPrintfRaft("%d sends MsgTimeoutNow to %d immediately as %d already has up-to-date log", r.id, leadTransferee, leadTransferee)
	} else {
		r.sendAppend(leadTransferee)
	}
	return nil
}

func (r *Raft) abortTransferLeader() {
	// 如果 leaderTransferee == None，那么之前的工作只是在对日志，无害。
	// 如果是立刻已经发送了，那么也是无害的，因为 timeoutnow 只是 term 增加发起一次选举而已。
	r.leadTransferee = None
}

func (r *Raft) hasPeer(peer uint64) bool {
	_, ok := r.Prs[peer]
	return ok
}

func (r *Raft) IsConfChange() bool {
	return (r.PendingConfIndex != None && r.PendingConfIndex > r.RaftLog.applied)
}

func (r *Raft) IsAddingNode() bool {
	return false
	// return r.AddingNode != None
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	defer func() {
		// r.AddingNode = id
		r.PendingConfIndex = None
	}()

	if r.hasPeer(id) {
		return
	}

	r.Prs[id] = &Progress{
		id: id,
		Match: 0,
		Next:  1,
	}

	if r.State == StateLeader {
		r.sendHeartbeat(id) // 促进创建
	}
}

func shouldCommit(prs map[uint64]*Progress) uint64 {
	sl := make([]uint64, 0, len(prs))

	for _, pr := range prs {
		sl = append(sl, pr.Match)
	}

	sort.Sort(uint64Slice(sl)) // 排序，取中位数，这个就是可以提交的 index
	return sl[(len(sl)-1)/2]   // 较小的那个中位数 4 -> 1, 3 -> 1, 2 -> 0, 1 -> 0
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).

	defer func() {
		r.PendingConfIndex = None
	}()

	if !r.hasPeer(id) {
		return
	}

	delete(r.Prs, id)

	if r.State == StateLeader {
		leaderCommit(r, shouldCommit(r.Prs))
	}
}
