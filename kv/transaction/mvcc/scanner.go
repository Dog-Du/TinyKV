package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn        *MvccTxn
	iter       engine_util.DBIterator
	currentKey []byte
	currentVal []byte
	err        error
	finished   bool
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		txn: txn,
	}

	// Create iterator for write CF
	scanner.iter = txn.Reader.IterCF(engine_util.CfWrite)

	// Seek to start key
	seekKey := EncodeKey(startKey, TsMax)
	scanner.iter.Seek(seekKey)

	// Prepare the first key-value pair
	scanner.prepareNext()

	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	if scan.iter != nil {
		scan.iter.Close()
	}
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.err != nil {
		return nil, nil, scan.err
	}
	if scan.finished {
		return nil, nil, nil
	}

	// Return current key-value pair
	key := scan.currentKey
	val := scan.currentVal

	// Prepare next key-value pair
	scan.prepareNext()

	return key, val, nil
}

// prepareNext prepares the next key-value pair for the scanner
func (scan *Scanner) prepareNext() {
	for scan.iter.Valid() {
		item := scan.iter.Item()
		itemKey := item.Key()
		userKey := DecodeUserKey(itemKey)

		// Skip to next user key if we've already processed this one
		if scan.currentKey != nil && bytes.Equal(userKey, scan.currentKey) {
			scan.skipToNextUserKey(userKey)
			continue
		}

		// Get value for this key (this will handle MVCC visibility)
		value, err := scan.txn.GetValue(userKey)
		if err != nil {
			scan.err = err
			return
		}

		// Only return keys that have values (not deleted)
		// The caller (KvScan) will handle lock checking
		if value != nil {
			scan.currentKey = userKey
			scan.currentVal = value
			scan.skipToNextUserKey(userKey)
			return
		}

		// Skip to next user key if value is nil
		scan.skipToNextUserKey(userKey)
	}

	// No more keys
	scan.finished = true
	scan.currentKey = nil
	scan.currentVal = nil
}

// skipToNextUserKey advances the iterator to the next user key
func (scan *Scanner) skipToNextUserKey(currentKey []byte) {
	for scan.iter.Valid() {
		scan.iter.Next()
		if !scan.iter.Valid() {
			break
		}
		nextKey := DecodeUserKey(scan.iter.Item().Key())
		if !bytes.Equal(nextKey, currentKey) {
			break
		}
	}
}
