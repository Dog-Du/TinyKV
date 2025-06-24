package raft

// import (
// 	"fmt"
// 	"log"

// 	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
// )

// // unstable contains "unstable" log entries and snapshot state that has
// // not yet been written to Storage. The type serves two roles. First, it
// // holds on to new log entries and an optional snapshot until they are
// // handed to a Ready struct for persistence. Second, it continues to
// // hold on to this state after it has been handed off to provide raftLog
// // with a view of the in-progress log entries and snapshot until their
// // writes have been stabilized and are guaranteed to be reflected in
// // queries of Storage. After this point, the corresponding log entries
// // and/or snapshot can be cleared from unstable.
// //
// // unstable.entries[i] has raft log position i+unstable.offset.
// // Note that unstable.offset may be less than the highest log
// // position in storage; this means that the next write to storage
// // might need to truncate the log before persisting unstable.entries.
// type unstable struct {
// 	// the incoming unstable snapshot, if any.
// 	// (Used in 2C)
// 	pendingSnapshot *pb.Snapshot

// 	// all entries that have not yet been written to storage.
// 	entries []pb.Entry

// 	// if true, snapshot is being written to storage.
// 	pendingSnapshotInProgress bool

// 	// entries[:offsetInProgress-offset] are being written to storage.
// 	// Like offset, offsetInProgress is exclusive, meaning that it
// 	// contains the index following the largest in-progress entry.
// 	// Invariant: offset <= offsetInProgress
// 	offsetInProgress uint64
// }

// // // maybeFirstIndex returns the index of the first possible entry in entries
// // // if it has a snapshot.
// // func (u *unstable) maybeFirstIndex() (uint64, bool) {
// // 	if u.pendingSnapshot != nil {
// // 		return u.pendingSnapshot.Metadata.Index + 1, true
// // 	}
// // 	return 0, false
// // }

// // maybeLastIndex returns the last index if it has at least one
// // unstable entry or snapshot.
// func (u *unstable) maybeLastIndex(offset uint64) (uint64, bool) {
// 	if l := len(u.entries); l != 0 {
// 		return offset + uint64(l) - 1, true
// 	}
// 	if u.pendingSnapshot != nil {
// 		return u.pendingSnapshot.Metadata.Index, true
// 	}
// 	return 0, false
// }

// // // maybeTerm returns the term of the entry at index i, if there
// // // is any.
// // func (u *unstable) maybeTerm(i uint64, offset uint64) (uint64, bool) {
// // 	if i < offset {
// // 		if u.pendingSnapshot != nil && u.pendingSnapshot.Metadata.Index == i {
// // 			return u.pendingSnapshot.Metadata.Term, true
// // 		}
// // 		return 0, false
// // 	}

// // 	last, ok := u.maybeLastIndex(offset)
// // 	if !ok {
// // 		return 0, false
// // 	}
// // 	if i > last {
// // 		return 0, false
// // 	}

// // 	return u.entries[i-offset].Term, true
// // }

// // // nextEntries returns the unstable entries that are not already in the process
// // // of being written to storage.
// // func (u *unstable) nextEntries(offset uint64) []pb.Entry {
// // 	inProgress := int(u.offsetInProgress - offset)
// // 	if len(u.entries) == inProgress {
// // 		return nil
// // 	}
// // 	return u.entries[inProgress:]
// // }

// // // nextSnapshot returns the unstable snapshot, if one exists that is not already
// // // in the process of being written to storage.
// // func (u *unstable) nextSnapshot() *pb.Snapshot {
// // 	if u.pendingSnapshot == nil || u.pendingSnapshotInProgress {
// // 		return nil
// // 	}
// // 	return u.pendingSnapshot
// // }

// // // acceptInProgress marks all entries and the snapshot, if any, in the unstable
// // // as having begun the process of being written to storage. The entries/snapshot
// // // will no longer be returned from nextEntries/nextSnapshot. However, new
// // // entries/snapshots added after a call to acceptInProgress will be returned
// // // from those methods, until the next call to acceptInProgress.
// // // acceptInProgress 会将 unstable 中的所有日志条目和快照（如果有的话）标记为
// // // 已经开始写入存储的过程。这些条目/快照将不会再通过 nextEntries/nextSnapshot 返回。
// // // 但是，在调用 acceptInProgress 之后新增的条目/快照，
// // // 仍然会通过这些方法返回，直到下次调用 acceptInProgress。
// // func (u *unstable) acceptInProgress() {
// // 	if len(u.entries) > 0 {
// // 		// NOTE: +1 because offsetInProgress is exclusive, like offset.
// // 		u.offsetInProgress = u.entries[len(u.entries)-1].Index
// // 	}
// // 	if u.pendingSnapshot != nil {
// // 		u.pendingSnapshotInProgress = true
// // 	}
// // }

// // // stableTo marks entries up to the entry with the specified (index, term) as
// // // being successfully written to stable storage.
// // //
// // // The method should only be called when the caller can attest that the entries
// // // can not be overwritten by an in-progress log append. See the related comment
// // // in newStorageAppendRespMsg.
// // func (u *unstable) stableTo(index uint64, term uint64, offset uint64) {
// // 	gt, ok := u.maybeTerm(index, offset)
// // 	if !ok {
// // 		// Unstable entry missing. Ignore.
// // 		log.Printf("entry at index %d missing from unstable log; ignoring", index)
// // 		return
// // 	}
// // 	if index < offset {
// // 		// Index matched unstable snapshot, not unstable entry. Ignore.
// // 		log.Printf("entry at index %d matched unstable snapshot; ignoring", index)
// // 		return
// // 	}
// // 	if gt != term {
// // 		// Term mismatch between unstable entry and specified entry. Ignore.
// // 		// This is possible if part or all of the unstable log was replaced
// // 		// between that time that a set of entries started to be written to
// // 		// stable storage and when they finished.
// // 		log.Printf("entry at (index,term)=(%d,%d) mismatched with "+
// // 			"entry at (%d,%d) in unstable log; ignoring", index, term, index, gt)
// // 		return
// // 	}
// // 	num := int(index + 1 - offset)
// // 	u.entries = u.entries[num:]
// // 	offset = index + 1
// // 	u.offsetInProgress = max(u.offsetInProgress, offset)
// // 	u.shrinkEntriesArray()
// // }

// // // shrinkEntriesArray discards the underlying array used by the entries slice
// // // if most of it isn't being used. This avoids holding references to a bunch of
// // // potentially large entries that aren't needed anymore. Simply clearing the
// // // entries wouldn't be safe because clients might still be using them.
// // func (u *unstable) shrinkEntriesArray() {
// // 	// We replace the array if we're using less than half of the space in
// // 	// it. This number is fairly arbitrary, chosen as an attempt to balance
// // 	// memory usage vs number of allocations. It could probably be improved
// // 	// with some focused tuning.
// // 	const lenMultiple = 2
// // 	if len(u.entries) == 0 {
// // 		u.entries = nil
// // 	} else if len(u.entries)*lenMultiple < cap(u.entries) {
// // 		newEntries := make([]pb.Entry, len(u.entries))
// // 		copy(newEntries, u.entries)
// // 		u.entries = newEntries
// // 	}
// // }

// // func (u *unstable) stableSnapTo(i uint64) {
// // 	if u.pendingSnapshot != nil && u.pendingSnapshot.Metadata.Index == i {
// // 		u.pendingSnapshot = nil
// // 		u.pendingSnapshotInProgress = false
// // 	}
// // }

// // func (u *unstable) restore(s pb.Snapshot) {
// // 	offset := s.Metadata.Index + 1
// // 	u.offsetInProgress = offset
// // 	u.entries = nil
// // 	u.pendingSnapshot = &s
// // 	u.pendingSnapshotInProgress = false
// // }

// // func (u *unstable) truncateAndAppend(ents []pb.Entry, offset uint64) {
// // 	fromIndex := ents[0].Index
// // 	switch {
// // 	case fromIndex == offset+uint64(len(u.entries)):
// // 		// fromIndex is the next index in the u.entries, so append directly.
// // 		u.entries = append(u.entries, ents...)
// // 	case fromIndex <= offset:
// // 		log.Printf("replace the unstable entries from index %d", fromIndex)
// // 		// The log is being truncated to before our current offset
// // 		// portion, so set the offset and replace the entries.
// // 		u.entries = ents
// // 		offset = fromIndex
// // 		u.offsetInProgress = offset
// // 	default:
// // 		// Truncate to fromIndex (exclusive), and append the new entries.
// // 		log.Printf("truncate the unstable entries before index %d", fromIndex)
// // 		keep := u.slice(offset, fromIndex, offset) // NB: appending to this slice is safe,
// // 		u.entries = append(keep, ents...)    // and will reallocate/copy it
// // 		// Only in-progress entries before fromIndex are still considered to be
// // 		// in-progress.
// // 		u.offsetInProgress = min(u.offsetInProgress, fromIndex)
// // 	}
// // }

// // slice returns the entries from the unstable log with indexes in the range
// // [lo, hi). The entire range must be stored in the unstable log or the method
// // will panic. The returned slice can be appended to, but the entries in it must
// // not be changed because they are still shared with unstable.
// //
// // TODO(pavelkalinnikov): this, and similar []pb.Entry slices, may bubble up all
// // the way to the application code through Ready struct. Protect other slices
// // similarly, and document how the client can use them.
// func (u *unstable) slice(lo uint64, hi uint64, offset uint64) []pb.Entry {
// 	if err := u.mustCheckOutOfBounds(lo, hi, offset); err != nil {
// 		log.Panicf(err.Error())
// 	}
// 	// NB: use the full slice expression to limit what the caller can do with the
// 	// returned slice. For example, an append will reallocate and copy this slice
// 	// instead of corrupting the neighbouring u.entries.
// 	return u.entries[lo-offset : hi-offset : hi-offset]
// }

// // u.offset <= lo <= hi <= u.offset+len(u.entries)
// func (u *unstable) mustCheckOutOfBounds(lo, hi uint64, offset uint64) error {
// 	if lo > hi {
// 		return fmt.Errorf("invalid unstable.slice %d > %d", lo, hi)
// 	}

// 	upper := offset + uint64(len(u.entries))

// 	if lo < offset || hi > upper {
// 		return fmt.Errorf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, offset, upper)
// 	}

// 	return nil
// }
