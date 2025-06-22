package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// send 系列函数只用来发送消息，不进行任何处理，也不进行任何判断
func (r *Raft) send(m pb.Message) bool {
	r.msgs = append(r.msgs, m)
	return true
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	return r.send(pb.Message{
		To:      to,
		From:    r.id,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgAppend,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.LastTerm(),
		Entries: []*pb.Entry{r.RaftLog.LastEntry()},
		Commit:  r.RaftLog.committed,
	})
}

func (r *Raft) sendAppendResponse(to uint64, agree bool, ent []*pb.Entry, ent1 []pb.Entry) bool {
	if ent == nil {
		ent = make([]*pb.Entry, 0)
	}

	if ent1 == nil {
		ent1 = make([]pb.Entry, 0)
	}

	for _, entry := range ent1 {
		ent = append(ent, &entry)
	}

	return r.send(pb.Message{
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  !agree,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index:   r.RaftLog.LastIndex(),
	})
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) bool {
	// Your Code Here (2A).
	return r.send(pb.Message{
		To:      to,
		From:    r.id,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  min(r.Prs[to].Match, r.RaftLog.committed),
	})
}

func (r *Raft) sendHeartbeatResponse(to uint64) bool {
	return r.send(pb.Message{
		To:      to,
		From:    r.id,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	})
}

func (r *Raft) sendRequestVote(to uint64) bool {
	return r.send(pb.Message{
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		MsgType: pb.MessageType_MsgRequestVote,
		LogTerm: r.RaftLog.LastTerm(),
	})
}

func (r *Raft) sendRequestVoteResponse(to uint64, term uint64, voteGranted bool) bool {
	return r.send(pb.Message{
		To:      to,
		From:    r.id,
		Term:    term,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Reject:  !voteGranted,
	})
}

func (r *Raft) sendHup(to uint64) bool {
	return r.send(pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHup,
	})
}

func (r *Raft) sendPropose(to uint64, ent []*pb.Entry, ent1 []pb.Entry) bool {
	if ent == nil {
		ent = make([]*pb.Entry, 0)
	}

	if ent1 == nil {
		ent1 = make([]pb.Entry, 0)
	}

	for _, entry := range ent1 {
		ent = append(ent, &entry)
	}

	return r.send(pb.Message{
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Entries: ent,
		MsgType: pb.MessageType_MsgPropose,
	})
}

func (r *Raft) sendSnapshot(to uint64) bool {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return false
	}

	return r.send(pb.Message{
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
		MsgType:  pb.MessageType_MsgSnapshot,
	})
}
