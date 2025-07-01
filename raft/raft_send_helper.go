package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// send 系列函数只用来发送消息，不进行任何处理，也不进行任何判断
func (r *Raft) send(m pb.Message) error {
	r.msgs = append(r.msgs, m)
	return nil
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) error {
	// Your Code Here (2A).
	index := r.Prs[to].Next
	term, err := r.RaftLog.Term(index - 1)

	if r.RaftLog.firstIndex > index || err == ErrCompacted {
		return r.sendSnapshot(to)
	}

	if err != nil {
		return err
	}

	ents, _ := r.RaftLog.Entries(index, r.RaftLog.LastIndex()+1)

	return r.send(pb.Message{
		To:      to,
		From:    r.id,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgAppend,
		Index:   index - 1,
		LogTerm: term,
		Entries: ents,
		Commit:  r.RaftLog.committed,
	})
}

func (r *Raft) sendAppendResponse(to uint64, agree bool, term uint64, index uint64, ent []*pb.Entry, ent1 []pb.Entry) error {
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
		Term:    term,
		Reject:  !agree,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index:   index,
	})
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) error {
	// Your Code Here (2A).
	return r.send(pb.Message{
		To:      to,
		From:    r.id,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  r.RaftLog.committed,
	})
}

func (r *Raft) sendHeartbeatResponse(to uint64) error {
	return r.send(pb.Message{
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	})
}

func (r *Raft) sendRequestVote(to uint64) error {
	return r.send(pb.Message{
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		MsgType: pb.MessageType_MsgRequestVote,
		LogTerm: r.RaftLog.LastTerm(),
	})
}

func (r *Raft) sendRequestVoteResponse(to uint64, term uint64, voteGranted bool) error {
	return r.send(pb.Message{
		To:      to,
		From:    r.id,
		Term:    term,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Reject:  !voteGranted,
	})
}

func (r *Raft) sendHup(to uint64) error {
	return r.send(pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHup,
	})
}

func (r *Raft) sendPropose(to uint64, ent []*pb.Entry, ent1 []pb.Entry) error {
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

func (r *Raft) sendSnapshot(to uint64) error {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		log.DPrintfRaft("[raft %d] failed to send snapshot to %d: %v", r.id, to, err)
		if err == ErrSnapshotTemporarilyUnavailable {
			return nil
		}
		return err
	}

	log.DPrintfRaft("[raft %d] send snapshot to %d: %v, data: %v", r.id, to, snapshot.Metadata, snapshot.Data)
	err = r.send(pb.Message{
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
		MsgType:  pb.MessageType_MsgSnapshot,
	})

	if err != nil {
		panic(err)
	}

	// 立刻更新，避免 snapshot 发送的过于频繁
	r.Prs[to].Next = snapshot.Metadata.Index + 1
	return nil
}

func (r *Raft) sendTimeoutNow(to uint64) error {
	return r.send(pb.Message{
		From: r.id,
		To: to,
		MsgType: pb.MessageType_MsgTimeoutNow,
	})
}

func (r *Raft) sendLeaderTransfer(to uint64, leaderTransfer uint64) error {
	return r.send(pb.Message{
		From: leaderTransfer,
		To: to,
		MsgType: pb.MessageType_MsgTransferLeader,
	})
}