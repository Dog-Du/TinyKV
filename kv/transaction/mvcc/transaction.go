package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

// user key        ==            key
// key + ts        == encoded    key
// key + start_ts  == CF_DEFAULT key
// key + commit_ts == CF_WRITE   key
// key             == CF_LOCK    key

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	encodedKey := EncodeKey(key, ts)
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   encodedKey,
			Value: write.ToBytes(),
			Cf:    engine_util.CfWrite,
		},
	})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	value, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	lock, err := ParseLock(value)
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   key,
			Value: lock.ToBytes(),
			Cf:    engine_util.CfLock,
		},
	})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  engine_util.CfLock,
		},
	})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	// Iterate through the write CF to find the most recent write for this key
	// that was committed before our start timestamp
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	// Seek to the first key that matches our user key with timestamp <= StartTS
	seekKey := EncodeKey(key, txn.StartTS)
	iter.Seek(seekKey)

	for iter.Valid() {
		item := iter.Item()
		itemKey := item.Key()

		// Check if this key belongs to our user key
		// if not, then there are no more writes for this key
		userKey := DecodeUserKey(itemKey)
		if !bytes.Equal(userKey, key) {
			break
		}

		// Get the write record
		value, err := item.Value()
		if err != nil {
			return nil, err
		}

		write, err := ParseWrite(value)
		if err != nil {
			return nil, err
		}

		// If this is a Put, get the actual value from default CF
		if write.Kind == WriteKindPut {
			valueKey := EncodeKey(key, write.StartTS)
			actualValue, err := txn.Reader.GetCF(engine_util.CfDefault, valueKey)
			if err != nil {
				return nil, err
			}
			return actualValue, nil
		} else if write.Kind == WriteKindDelete {
			// Key was deleted, return nil
			return nil, nil
		}
		// For rollback, continue to next entry

		iter.Next()
	}

	// No valid write found
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	encodedKey := EncodeKey(key, txn.StartTS)
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   encodedKey,
			Value: value,
			Cf:    engine_util.CfDefault,
		},
	})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	encodedKey := EncodeKey(key, txn.StartTS)
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: encodedKey,
			Cf:  engine_util.CfDefault,
		},
	})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	// Iterate through the write CF to find a write for this key with our start timestamp
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	// Seek to the first key that matches our user key
	// TsMax will be turned into 00000000 by EncodeKey, so this will seek to the latest write for this key
	// because timestamps are sorted in descending order to find the latest
	seekKey := EncodeKey(key, TsMax)
	iter.Seek(seekKey)

	for iter.Valid() {
		item := iter.Item()
		itemKey := item.Key()

		// Check if this key belongs to our user key
		userKey := DecodeUserKey(itemKey)
		if !bytes.Equal(userKey, key) {
			break
		}

		// Get the write record
		value, err := item.Value()
		if err != nil {
			return nil, 0, err
		}

		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}

		// Check if this write has our start timestamp
		if write.StartTS == txn.StartTS {
			// Extract commit timestamp from the key
			commitTs := decodeTimestamp(itemKey)
			return write, commitTs, nil
		}

		iter.Next()
	}

	// No write found with our start timestamp
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	// Iterate through the write CF to find the most recent write for this key
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	// Seek to the first key that matches our user key (with highest timestamp)
	seekKey := EncodeKey(key, TsMax)
	iter.Seek(seekKey)

	for iter.Valid() {
		item := iter.Item()
		itemKey := item.Key()

		// Check if this key belongs to our user key
		userKey := DecodeUserKey(itemKey)
		if !bytes.Equal(userKey, key) {
			break
		}

		// Get the write record
		value, err := item.Value()
		if err != nil {
			return nil, 0, err
		}

		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}

		// This is the most recent write (since we iterate in descending timestamp order)
		commitTs := decodeTimestamp(itemKey)
		return write, commitTs, nil
	}

	// No write found
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
