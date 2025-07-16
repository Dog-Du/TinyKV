package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}

	// Get storage reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	// Create MVCC transaction
	txn := mvcc.NewMvccTxn(reader, req.Version)

	// Check if key is locked
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.IsLockedFor(req.Key, req.Version, resp) {
		return resp, nil
	}

	// Get value
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}

	resp.Value = value
	resp.NotFound = (value == nil)

	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}

	// Get storage reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	// Create MVCC transaction
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// Collect keys to latch
	var keysToLatch [][]byte
	for _, mut := range req.Mutations {
		keysToLatch = append(keysToLatch, mut.Key)
	}

	// Acquire latches
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	// Process each mutation
	for _, mut := range req.Mutations {
		keyError := server.prewriteKey(txn, mut, req.PrimaryLock, req.StartVersion, req.LockTtl)
		if keyError != nil {
			resp.Errors = append(resp.Errors, keyError)
		}
	}

	// If no errors, write to storage
	if len(resp.Errors) == 0 {
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
	}

	// Validate latches
	server.Latches.Validate(txn, keysToLatch)

	return resp, nil
}

// prewriteKey handles prewriting a single key
func (server *Server) prewriteKey(txn *mvcc.MvccTxn, mut *kvrpcpb.Mutation, primaryLock []byte, startTS uint64, lockTtl uint64) *kvrpcpb.KeyError {
	// Check if key is already locked
	lock, err := txn.GetLock(mut.Key)
	if err != nil {
		return &kvrpcpb.KeyError{Abort: err.Error()}
	}
	if lock != nil {
		return &kvrpcpb.KeyError{Locked: lock.Info(mut.Key)}
	}

	// Check for write conflicts - look for any committed write after our start timestamp
	write, commitTS, err := txn.MostRecentWrite(mut.Key)
	if err != nil {
		return &kvrpcpb.KeyError{Abort: err.Error()}
	}
	if write != nil && commitTS >= startTS {
		return &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
			StartTs:    startTS,
			ConflictTs: commitTS,
			Key:        mut.Key,
			Primary:    primaryLock,
		}}
	}

	// Create lock
	lock = &mvcc.Lock{
		Primary: primaryLock,
		Ts:      startTS,
		Ttl:     lockTtl,
		Kind:    mvcc.WriteKindFromProto(mut.Op),
	}

	// Put lock
	txn.PutLock(mut.Key, lock)

	// Put value if it's a Put operation
	if mut.Op == kvrpcpb.Op_Put {
		txn.PutValue(mut.Key, mut.Value)
	} else if mut.Op == kvrpcpb.Op_Del {
		txn.DeleteValue(mut.Key)
	}

	return nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}

	// Get storage reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	// Create MVCC transaction
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// Acquire latches for all keys
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	// Process each key
	for _, key := range req.Keys {
		keyError := server.commitKey(txn, key, req.StartVersion, req.CommitVersion)
		if keyError != nil {
			resp.Error = keyError
			return resp, nil
		}
	}

	// Write to storage
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	// Validate latches
	server.Latches.Validate(txn, req.Keys)

	return resp, nil
}

// commitKey handles committing a single key
func (server *Server) commitKey(txn *mvcc.MvccTxn, key []byte, startTS uint64, commitTS uint64) *kvrpcpb.KeyError {
	// Check if there's already a write record for this transaction
	write, _, err := txn.CurrentWrite(key)
	if err != nil {
		return &kvrpcpb.KeyError{Abort: err.Error()}
	}
	if write != nil && write.StartTS == startTS {
		if write.Kind == mvcc.WriteKindRollback {
			// Transaction was already rolled back
			return &kvrpcpb.KeyError{Abort: "transaction was rolled back"}
		}
		// Already committed, this is a retry
		return nil
	}

	// Check if key is locked by this transaction
	lock, err := txn.GetLock(key)
	if err != nil {
		return &kvrpcpb.KeyError{Abort: err.Error()}
	}
	if lock == nil {
		// Key is not locked, this might be a retry or the prewrite was never done
		// This is not an error - just ignore it (idempotent commit)
		return nil
	}
	if lock.Ts != startTS {
		// Key is locked by a different transaction
		return &kvrpcpb.KeyError{Retryable: "key is locked by another transaction"}
	}

	// Create write record
	writeRecord := &mvcc.Write{
		StartTS: startTS,
		Kind:    lock.Kind,
	}

	// Put write record and delete lock
	txn.PutWrite(key, commitTS, writeRecord)
	txn.DeleteLock(key)

	return nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}

	// Get storage reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	// Create MVCC transaction
	txn := mvcc.NewMvccTxn(reader, req.Version)

	// Create scanner
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	// Scan keys up to the limit
	limit := req.Limit
	for limit > 0 {
		key, value, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if key == nil && value == nil {
			// Scanner exhausted
			break
		}

		// Check if key is locked
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.IsLockedFor(key, req.Version, &struct{}{}) {
			kvPair := &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{Locked: lock.Info(key)},
				Key:   key,
			}
			resp.Pairs = append(resp.Pairs, kvPair)
		} else {
			kvPair := &kvrpcpb.KvPair{
				Key:   key,
				Value: value,
			}
			resp.Pairs = append(resp.Pairs, kvPair)
		}
		limit--
	}

	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}

	// Get storage reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	// Create MVCC transaction
	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	// Acquire latch for primary key
	server.Latches.WaitForLatches([][]byte{req.PrimaryKey})
	defer server.Latches.ReleaseLatches([][]byte{req.PrimaryKey})

	// Check if transaction is already committed or rolled back
	write, commitTS, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if write != nil && write.StartTS == req.LockTs {
		if write.Kind == mvcc.WriteKindRollback {
			// Transaction was already rolled back
			resp.CommitVersion = 0
			resp.LockTtl = 0
			return resp, nil
		} else {
			// Transaction is committed
			resp.CommitVersion = commitTS
			return resp, nil
		}
	}

	// Check if key is locked
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lock == nil || lock.Ts != req.LockTs {
		// Lock doesn't exist or belongs to different transaction
		// This means the transaction was rolled back
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		// Put rollback record
		rollbackWrite := &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(req.PrimaryKey, req.LockTs, rollbackWrite)

		// Write to storage
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		server.Latches.Validate(txn, [][]byte{req.PrimaryKey})
		return resp, nil
	}

	// Check if lock has expired
	if mvcc.PhysicalTime(req.CurrentTs) >= mvcc.PhysicalTime(lock.Ts)+lock.Ttl {
		// Lock has expired, rollback the transaction
		resp.Action = kvrpcpb.Action_TTLExpireRollback

		// Put rollback record, delete lock and delete value
		rollbackWrite := &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(req.PrimaryKey, req.LockTs, rollbackWrite)
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)

		// Write to storage
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		server.Latches.Validate(txn, [][]byte{req.PrimaryKey})
		return resp, nil
	}

	// Lock is still valid
	resp.LockTtl = lock.Ttl - (mvcc.PhysicalTime(req.CurrentTs) - mvcc.PhysicalTime(lock.Ts))

	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}

	// Get storage reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	// Create MVCC transaction
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// Acquire latches for all keys
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	// Process each key
	for _, key := range req.Keys {
		keyError := server.rollbackKey(txn, key, req.StartVersion)
		if keyError != nil {
			resp.Error = keyError
			return resp, nil
		}
	}

	// Write to storage
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	// Validate latches
	server.Latches.Validate(txn, req.Keys)

	return resp, nil
}

// rollbackKey handles rolling back a single key
func (server *Server) rollbackKey(txn *mvcc.MvccTxn, key []byte, startTS uint64) *kvrpcpb.KeyError {
	// Check if transaction is already committed
	write, _, err := txn.CurrentWrite(key)
	if err != nil {
		return &kvrpcpb.KeyError{Abort: err.Error()}
	}
	if write != nil && write.StartTS == startTS {
		if write.Kind != mvcc.WriteKindRollback {
			// Transaction is already committed, cannot rollback
			return &kvrpcpb.KeyError{Abort: "transaction already committed"}
		}
		// Already rolled back
		return nil
	}

	// Check if key is locked by this transaction
	lock, err := txn.GetLock(key)
	if err != nil {
		return &kvrpcpb.KeyError{Abort: err.Error()}
	}
	if lock != nil && lock.Ts == startTS {
		// Delete the lock and value
		txn.DeleteLock(key)
		txn.DeleteValue(key)
	}

	// Put rollback record
	rollbackWrite := &mvcc.Write{
		StartTS: startTS,
		Kind:    mvcc.WriteKindRollback,
	}
	txn.PutWrite(key, startTS, rollbackWrite)

	return nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}

	// Get storage reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	// Create MVCC transaction
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// Find all locks for this transaction
	locks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return nil, err
	}

	// Collect keys to latch
	var keysToLatch [][]byte
	for _, lock := range locks {
		keysToLatch = append(keysToLatch, lock.Key)
	}

	// Acquire latches
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	// Process each lock
	for _, lock := range locks {
		if req.CommitVersion == 0 {
			// Rollback
			keyError := server.rollbackKey(txn, lock.Key, req.StartVersion)
			if keyError != nil {
				resp.Error = keyError
				return resp, nil
			}
		} else {
			// Commit
			keyError := server.commitKey(txn, lock.Key, req.StartVersion, req.CommitVersion)
			if keyError != nil {
				resp.Error = keyError
				return resp, nil
			}
		}
	}

	// Write to storage
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	// Validate latches
	server.Latches.Validate(txn, keysToLatch)

	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
