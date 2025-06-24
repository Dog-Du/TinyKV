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
	"log"
	"math/rand"

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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	prs := make(map[uint64]*Progress)
	for _, peer := range c.peers {
		prs[peer] = &Progress{
			Match: 0,
			Next:  0,
			id:    peer,
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
	hardstate, _, _ := raft.RaftLog.storage.InitialState()
	raft.Term = hardstate.Term
	raft.Vote = hardstate.Vote
	return raft
}

func (r *Raft) resetElapsed() {
	r.heartbeatElapsed = r.heartbeatTimeout
	//r.electionElapsed = r.electionTimeout

	//r.heartbeatElapsed = rand.Intn(r.heartbeatTimeout) + 1
	r.electionElapsed = rand.Intn(r.electionTimeout) + r.electionTimeout
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed--
	}
	r.electionElapsed--

	if r.electionElapsed <= 0 {
		r.handleHup(pb.Message{})
	}

	if r.heartbeatElapsed <= 0 && r.State == StateLeader {
		for _, peer := range r.Prs {
			if peer.id == r.id {
				continue
			}
			r.sendHeartbeat(peer.id)
			// r.sendAppend(peer.id)
		}

		r.resetElapsed()
	}
}

func appendLog(r *Raft, ents []*pb.Entry, preLogTerm uint64, preLogIndex uint64) {
	r.RaftLog.appendLog(ents, preLogTerm, preLogIndex)
	r.Prs[r.id].Match = max(r.Prs[r.id].Match, r.RaftLog.LastIndex())
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).

	r.Term = term
	r.Vote = None
	r.Lead = lead
	r.State = StateFollower
	r.resetElapsed()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).

	r.Term++
	r.Vote = None
	r.State = StateCandidate
	r.resetElapsed()

	r.votes = make(map[uint64]bool) // 清空
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	r.Vote = None
	r.State = StateLeader
	r.votes = make(map[uint64]bool) // 清空

	r.resetElapsed()

	for _, peer := range r.Prs {
		peer.Match = 0
		peer.Next = r.RaftLog.LastIndex() + 1
	}

	// 添加一个空条目
	appendLog(r, []*pb.Entry{{Term: r.Term, Index: r.RaftLog.LastIndex() + 1, Data: nil}}, r.Term, r.RaftLog.LastIndex())
}

func (r *Raft) canBeLeader() bool {
	switch r.State {
	case StateFollower:
		return false
	case StateLeader:
		return true
	}

	cnt := 0

	for _, v := range r.votes {
		if v {
			cnt++
		}
	}

	return cnt > len(r.Prs)/2
}

// 判断是否可以成为 leader，如果可以，则成为 leader 并发出 propose，并返回 true，否则返回 false。
func becomeLeader(r *Raft, term uint64) bool {
	if !r.canBeLeader() {
		if len(r.votes) >= len(r.Prs) {
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

	return true
}

func leaderCommit(r *Raft, index uint64) bool {
	if r.State != StateLeader {
		log.Panicf("leaderCommit called in %s state", r.State.String())
	}

	if r.RaftLog.LastIndex() < index {
		return false
	}

	log_term, err := r.RaftLog.Term(index)
	if err != nil {
		log.Panicf("leaderCommit called with invalid index: %d, err : %s", index, err.Error())
	}
	if log_term < r.Term {
		return false
	}

	cnt := 0
	for _, peer := range r.Prs {
		if peer.id == r.id {
			continue
		}

		if r.Prs[peer.id].Match >= index {
			cnt++
		}
	}

	if cnt >= len(r.Prs)/2 && r.RaftLog.committed < index {
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

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
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
			// return r.handleTransferLeader(m)
		}
	// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
	// the transfer target timeout immediately and start a new election.
	case pb.MessageType_MsgTimeoutNow:
		{
			// return r.handleTimeoutNow(m)
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
		r.sendAppendResponse(m.From, false, m.Term, nil, nil)
		return nil
	}

	r.becomeFollower(m.Term, m.From)

	if ent := r.RaftLog.Entries(m.Index, m.Index+1); r.State != StateLeader && m.Index > 0 && (len(ent) == 0 || ent[0].Term != m.LogTerm) {
		r.sendAppendResponse(m.From, false, r.Term, nil, nil)
		return nil
	}

	appendLog(r, m.Entries, m.LogTerm, m.Index)
	r.sendAppendResponse(m.From, true, r.Term, m.Entries, nil)

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
		log.Printf("handleBeat called in %s state", r.State.String())
		return nil
	}

	for _, peer := range r.Prs {
		if peer.id == r.id {
			continue
		}

		r.sendHeartbeat(peer.id)
		// r.sendAppend(peer.id)
	}

	return nil
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) error {
	// Your Code Here (2A).

	if r.Term > m.Term {
		return nil
	}

	if r.Term <= m.Term {
		r.becomeFollower(m.Term, m.From)
	}

	r.sendHeartbeatResponse(m.From)
	return nil
}

func (r *Raft) handleHup(m pb.Message) error {
	if r.State == StateLeader {
		log.Printf("handleHup called in leader state")
		return nil
	}

	r.becomeCandidate()

	for _, peer := range r.Prs {
		if peer.id == r.id {
			continue
		}

		r.sendRequestVote(peer.id)
	}

	r.votes[r.id] = true
	r.Vote = r.id

	becomeLeader(r, r.Term)

	return nil
}

func (r *Raft) handleRequestVote(m pb.Message) error {
	agree := false

	if r.Term < m.Term {
		// 如果 term 小，说明发生了一个新的 election，那么直接成为这个候选者的 follower，并投票，相当于给第一个选举的节点投票
		r.becomeFollower(m.Term, None)
	}

	// 如果候选人收到其他候选人的拉票、而且拉票的任期编号不小于自己的任期编号，就会自认落选，成为追随者，并认定来拉票的候选人为领袖。from wiki
	if r.RaftLog.LastTerm() > m.LogTerm { // 如果拉票的任期编号大于自己的任期编号，则拒绝
		agree = false
	} else if r.RaftLog.LastTerm() == m.LogTerm && r.RaftLog.LastIndex() > m.Index { // 相等的时候比较索引，如果索引不同，则拒绝。
		agree = false
	} else if r.Term > m.Term {
		// 如果 m.term 小于当前 r.term，则拒绝
		agree = false
	} else if r.Vote != None && r.Vote != m.From {
		// 如果这个 term 已经投过票，并且不是给这个节点投票，则拒绝
		// 如果是当前状态 candidate， 会经过这个分支。
		log.Printf("raft %d has vote %d, reject vote from %d", r.id, r.Vote, m.From)
		agree = false
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
		log.Printf("raft %d recive a request vote from %d, may be network latency", r.id, m.From)
		agree = false
	}

	r.sendRequestVoteResponse(m.From, r.Term, agree)
	return nil
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) error {
	if r.State == StateCandidate {
		r.votes[m.From] = !m.Reject

		becomeLeader(r, m.Term)
	}

	return nil
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) error {
	// Your Code Here (2C).
	return nil
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) error {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
	}

	if m.Commit < r.RaftLog.committed {
		r.sendAppend(m.From)
	}

	return nil
}

func (r *Raft) handlePropose(m pb.Message) error {
	if r.State != StateLeader {
		return ErrProposalDropped
	}

	for _, ent := range m.Entries {
		ent.Term = r.Term
	}

	appendLog(r, m.Entries, r.Term, r.RaftLog.LastIndex())

	for _, peer := range r.Prs {
		if peer.id == r.id {
			continue
		}

		r.sendAppend(peer.id)
	}

	leaderCommit(r, r.RaftLog.LastIndex())
	return nil
}

func (r *Raft) handleAppendResponse(m pb.Message) error {
	if r.Term > m.Term {
		return nil
	}

	if m.Reject {
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
		return nil
	}

	r.Prs[m.From].Match = max(r.Prs[m.From].Match, min(r.RaftLog.LastIndex(), m.Index))
	r.Prs[m.From].Next = r.Prs[m.From].Match + 1

	leaderCommit(r, m.Index)
	return nil
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
