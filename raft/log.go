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
	"fmt"

	"github.com/pingcap-incubator/tinykv/log"
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
// firstIndex：该值取自storage.FirstIndex()，可以从MemoryStorage的实现看到，该值是MemoryStorage.ents数组的第一个数据索引，也就是MemoryStorage结构体中快照数据与日志条目数据的分界线。
// lastIndex：该值取自storage.LastIndex()，可以从MemoryStorage的实现看到，该值是MemoryStorage.ents数组的最后一个数据索引。
// unstable.offset：该值为lastIndex索引的下一个位置。
// committed、applied：在初始的情况下，这两个值是firstIndex的上一个索引位置，这是因为在firstIndex之前的数据既然已经是持久化数据了，说明都是已经被提交成功的数据了。
// applied：在初始的情况下，这两个值是firstIndex的上一个索引位置，这是因为在firstIndex之前的数据既然已经是持久化数据了，说明都是已经被提交成功的数据了。
// from etcd。
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
	stabled    uint64
	firstIndex uint64 // == storage.LastIndex() + 1 at init, and never change after init

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// applied <= committed
func (l *RaftLog) check() {
	if l.applied > l.committed {
		log.Panicf("applied > committed, applied: %d, committed: %d", l.applied, l.committed)
	}
}

// committed and applied are always increasing, so we can use max to update them
func (l *RaftLog) commitTo(leaderCommit uint64, lastNewIndex uint64) {
	log.Debugf("commitTo: %d, lastNewIndex: %d, lastcommit: %d", leaderCommit, lastNewIndex, l.committed)
	l.check()
	l.committed = max(min(leaderCommit, lastNewIndex), l.committed)
	l.check()
}

func (l *RaftLog) appliedTo(index uint64) {
	l.check()
	l.applied = max(l.applied, index)
	l.check()
}

// stabled maybe decrease
func (l *RaftLog) stableTo(index uint64) {
	l.check()
	l.stabled = index
	l.check()
}

// appendlog always after committo
func (l *RaftLog) appendLog(ents []*pb.Entry, preLogTerm uint64, preLogIndex uint64) {
	checkEnts, err := l.Entries(preLogIndex+1, preLogIndex+1+uint64(len(ents)))
	appended := true

	if err != nil {
		appended = false
	} else {
		for i, ent := range checkEnts {
			if ent.Term == ents[i].Term && ent.Index == ents[i].Index &&
				ent.Index == preLogIndex+1 {
				preLogIndex++
			} else {
				appended = false
				break
			}
		}
	}
	if len(ents) == 0 || (appended && len(checkEnts) > 0) {
		return
	}

	for len(l.entries) > 0 && preLogIndex < l.LastIndex() {
		l.entries = l.entries[:len(l.entries)-1]
	}

	l.stableTo(min(l.stabled, preLogIndex))

	for _, ent := range ents {
		preLogIndex++
		ent.Index = preLogIndex
		l.entries = append(l.entries, *ent)
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

	hardState, _, err := storage.InitialState()

	if err != nil {
		panic(err.Error())
	}

	log := &RaftLog{
		storage: storage,
		entries: entries,
	}

	log.committed = hardState.Commit
	log.applied = firstIndex - 1
	log.stabled = lastIndex
	log.firstIndex = firstIndex
	return log
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
	stableFirstIndex, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	entries, err := l.storage.Entries(stableFirstIndex, l.stabled+1)
	if err != nil {
		panic(err)
	}

	entries = append(entries, l.unstableEntries()...)

	return entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}

	return l.entries[l.stabled-l.firstIndex+1:]
}

// nextEnts returns all the committed but not applied entries
// 也就是 [applied + 1, committed] 之间的日志
func (l *RaftLog) nextEnts() []pb.Entry {
	// Your Code Here (2A).

	ents, err := l.Entries(l.applied+1, l.committed+1)
	if err != nil {
		return []pb.Entry{}
	}

	return pointer2entry(ents)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).

	if len(l.entries) == 0 {
		return l.stabled
	}

	return l.firstIndex - 1 + uint64(len(l.entries))
}

func (l *RaftLog) LastEntry() *pb.Entry {
	lastIndex := l.LastIndex()

	entry, err := l.Entries(lastIndex, lastIndex+1)

	if err != nil || len(entry) == 0 {
		return nil
	}

	return entry[0]
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i <= l.stabled { // 因为可能变成了快照
		return l.storage.Term(i)
	}

	ent, err := l.Entries(i, i+1)

	if err != nil {
		return 0, err
	}

	if len(ent) == 0 {
		return 0, ErrUnavailable
	}

	return ent[0].Term, nil
}

func (l *RaftLog) findFirstIndexOfTerm(term uint64) uint64 {
	ents := l.allEntries()
	length := len(ents)

	for i := 0; i < length; i++ {
		if ents[i].Term == term {
			return ents[i].Index
		}
	}

	return 0
}

func (l *RaftLog) findLastIndexOfTerm(term uint64) uint64 {
	ents := l.allEntries()
	length := len(ents)

	for i := length - 1; i >= 0; i-- {
		if ents[i].Term == term {
			return ents[i].Index
		}
	}

	return 0
}

func (l *RaftLog) Entries(lo, hi uint64) ([]*pb.Entry, error) {
	ents := make([]*pb.Entry, 0)

	if lo <= l.stabled {
		entries, err := l.storage.Entries(lo, min(hi, l.stabled+1))
		if err != nil {
			// log.Print(err.Error())
			return []*pb.Entry{}, err
		}

		for _, entry := range entries {
			ents = append(ents, &entry)
		}
	}

	if hi > l.stabled {
		err := mustCheckOutOfBounds(l.entries, max(lo, l.stabled+1), hi, l.firstIndex)
		if err != nil {
			// log.Printf("err: %s, maybe unexist entry", err.Error())
			return []*pb.Entry{}, err
		}

		entries := slice(l.entries, max(lo, l.stabled+1), hi, l.firstIndex)

		for _, entry := range entries {
			ents = append(ents, &entry)
		}
	}

	return ents, nil
}

func (l *RaftLog) LastTerm() uint64 {
	// Your Code Here (2A).
	term, err := l.Term(l.LastIndex())
	if err != nil {
		panic(err)
	}
	return term
}

func pointer2entry(ents []*pb.Entry) []pb.Entry {
	if ents == nil {
		return nil
	}

	entries := make([]pb.Entry, 0, 2)
	for _, ent := range ents {
		entries = append(entries, *ent)
	}
	return entries
}

func entry2pointer(ents []pb.Entry) []*pb.Entry {
	if ents == nil {
		return nil
	}

	entries := make([]*pb.Entry, 0, 2)
	for _, ent := range ents {
		entries = append(entries, &ent)
	}
	return entries
}

func slice(entries []pb.Entry, lo uint64, hi uint64, firstIndex uint64) []pb.Entry {
	if err := mustCheckOutOfBounds(entries, lo, hi, firstIndex); err != nil {
		log.Panic(err.Error())
	}
	// NB: use the full slice expression to limit what the caller can do with the
	// returned slice. For example, an append will reallocate and copy this slice
	// instead of corrupting the neighbouring u.entries.
	return entries[lo-firstIndex : hi-firstIndex : hi-firstIndex]
}

// u.offset <= lo <= hi <= u.offset+len(u.entries)
func mustCheckOutOfBounds(entries []pb.Entry, lo, hi uint64, firstIndex uint64) error {
	if lo > hi {
		return fmt.Errorf("invalid unstable.slice %d > %d", lo, hi)
	}

	upper := firstIndex + uint64(len(entries))

	if lo < firstIndex || hi > upper {
		return fmt.Errorf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, firstIndex, upper)
	}

	return nil
}
