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
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated

// 所有没有 compact 的日志都在内存中。
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64 // 未被快照截断的第一个日志, 日志为空时为 1

	lastIndex uint64 // 最后一个索引， 日志为空时为 0
}

func (l *RaftLog) modifyCommitted(committed uint64) {
	if committed < l.committed || committed < l.applied {
		log.Panicf("committed(%d) < committed(%d) or committed(%d) < applied(%d)", committed, l.committed, committed, l.applied)
	}

	l.committed = committed
}

func (l *RaftLog) modifyApplied(applied uint64) {
	if applied < l.applied || applied > l.committed {
		log.Panicf("applied(%d) < applied(%d) or applied(%d) > committed(%d)", applied, l.applied, applied, l.committed)
	}

	l.applied = applied
}

func (l *RaftLog) modifyStabled(stabled uint64) {
	if stabled < l.stabled || stabled > l.lastIndex || stabled < l.firstIndex {
		log.Panicf("stabled(%d) < stabled(%d) or stabled(%d) > lastIndex(%d) or stabled(%d) < firstIndex(%d)", stabled, l.stabled, stabled, l.lastIndex, stabled, l.firstIndex)
	}

	l.stabled = stabled
}

func (l *RaftLog) appendLog(ents []*pb.Entry) {
	for _, ent := range ents {
		l.entries = append(l.entries, *ent)
		// l.lastIndex++
	}
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}

	return &RaftLog{
		storage:         storage,
		firstIndex:      firstIndex,
		lastIndex:       lastIndex,
		committed:       0,
		applied:         0,
		stabled:         lastIndex,
		entries:         entries,
		pendingSnapshot: nil,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	lastindex, err := l.storage.LastIndex()

	if err != nil {
		panic(err)
	}

	if lastindex < l.firstIndex {
		return nil
	}

	return l.entries[:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}

	return l.entries[l.stabled-l.firstIndex:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.entries[l.applied-l.firstIndex : l.committed-l.firstIndex]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.lastIndex
}

func (l *RaftLog) LastEntry() *pb.Entry {
	if len(l.entries) == 0 {
		lastindex, err := l.storage.LastIndex()
		if err != nil {
			return &pb.Entry{}
		}
		ents, err := l.storage.Entries(lastindex, lastindex+1)
		if err != nil {
			return &pb.Entry{}
		}
		return &ents[0]
	}

	return &l.entries[len(l.entries)-1]
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.lastIndex {
		log.Panicf("term(%d) > lastIndex(%d)", i, l.lastIndex)
	}

	if i >= l.firstIndex {
		return l.entries[i-l.firstIndex].Term, nil
	}

	return l.storage.Term(i)
}

func (l *RaftLog) LastTerm() uint64 {
	// Your Code Here (2A).
	term, err := l.Term(l.lastIndex)
	if err != nil {
		panic(err)
	}

	return term
}
