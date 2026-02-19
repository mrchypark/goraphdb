// Package wasm provides an in-memory key-value store that mimics bbolt's API,
// enabling goraphdb to run as a WebAssembly module in the browser without
// any file system or OS dependencies.
package wasm

import (
	"bytes"
	"sort"
	"sync"
)

// ---------------------------------------------------------------------------
// MemDB — in-memory database (replaces bbolt for WASM builds)
// ---------------------------------------------------------------------------

// MemDB is an in-memory key-value database with an API modeled after bbolt.
// All data lives in sorted slices for ordered iteration and prefix scans.
// Thread-safety is provided via sync.RWMutex.
type MemDB struct {
	mu      sync.RWMutex
	buckets map[string]*bucketData
	closed  bool
}

type bucketData struct {
	entries []kv
}

type kv struct {
	key   []byte
	value []byte
}

// NewMemDB creates a new in-memory database.
func NewMemDB() *MemDB {
	return &MemDB{
		buckets: make(map[string]*bucketData),
	}
}

// View executes a read-only function within a managed read transaction.
func (db *MemDB) View(fn func(tx *MemTx) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	tx := &MemTx{db: db, writable: false}
	return fn(tx)
}

// Update executes a read-write function within a managed write transaction.
func (db *MemDB) Update(fn func(tx *MemTx) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	tx := &MemTx{db: db, writable: true}
	return fn(tx)
}

// Begin starts a transaction. The caller must call Commit or Rollback.
// For writable=true, an exclusive lock is held until Commit/Rollback.
// For writable=false, a shared read lock is held.
func (db *MemDB) Begin(writable bool) (*MemTx, error) {
	if writable {
		db.mu.Lock()
	} else {
		db.mu.RLock()
	}
	return &MemTx{db: db, writable: writable}, nil
}

// Close is a no-op for in-memory database.
func (db *MemDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.closed = true
	return nil
}

// Sync is a no-op for in-memory database.
func (db *MemDB) Sync() error {
	return nil
}

// CreateBucket ensures a named bucket exists (called during init).
func (db *MemDB) CreateBucket(name []byte) {
	key := string(name)
	if _, ok := db.buckets[key]; !ok {
		db.buckets[key] = &bucketData{}
	}
}

// ---------------------------------------------------------------------------
// MemTx — transaction
// ---------------------------------------------------------------------------

// MemTx is a transaction handle on a MemDB.
type MemTx struct {
	db       *MemDB
	writable bool
	done     bool
}

// Bucket returns a handle to a named bucket. Returns nil if not found.
func (tx *MemTx) Bucket(name []byte) *MemBucket {
	bd, ok := tx.db.buckets[string(name)]
	if !ok {
		return nil
	}
	return &MemBucket{data: bd, writable: tx.writable}
}

// CreateBucketIfNotExists creates a bucket if it doesn't exist.
func (tx *MemTx) CreateBucketIfNotExists(name []byte) (*MemBucket, error) {
	key := string(name)
	bd, ok := tx.db.buckets[key]
	if !ok {
		bd = &bucketData{}
		tx.db.buckets[key] = bd
	}
	return &MemBucket{data: bd, writable: tx.writable}, nil
}

// Commit finishes a writable transaction.
func (tx *MemTx) Commit() error {
	if tx.done {
		return nil
	}
	tx.done = true
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
	return nil
}

// Rollback aborts a transaction.
func (tx *MemTx) Rollback() error {
	if tx.done {
		return nil
	}
	tx.done = true
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
	return nil
}

// ---------------------------------------------------------------------------
// MemBucket — sorted key-value collection
// ---------------------------------------------------------------------------

// MemBucket is a named sorted key-value collection within a MemDB.
type MemBucket struct {
	data     *bucketData
	writable bool
}

// Get returns the value for a key, or nil if not found.
func (b *MemBucket) Get(key []byte) []byte {
	idx := b.search(key)
	if idx < len(b.data.entries) && bytes.Equal(b.data.entries[idx].key, key) {
		return b.data.entries[idx].value
	}
	return nil
}

// Put sets a key-value pair. Both key and value are copied.
func (b *MemBucket) Put(key, value []byte) error {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	var valCopy []byte
	if value != nil {
		valCopy = make([]byte, len(value))
		copy(valCopy, value)
	}

	idx := b.search(key)
	if idx < len(b.data.entries) && bytes.Equal(b.data.entries[idx].key, key) {
		// Update existing.
		b.data.entries[idx].value = valCopy
		return nil
	}
	// Insert at position to maintain sorted order.
	b.data.entries = append(b.data.entries, kv{})
	copy(b.data.entries[idx+1:], b.data.entries[idx:])
	b.data.entries[idx] = kv{key: keyCopy, value: valCopy}
	return nil
}

// Delete removes a key.
func (b *MemBucket) Delete(key []byte) error {
	idx := b.search(key)
	if idx < len(b.data.entries) && bytes.Equal(b.data.entries[idx].key, key) {
		b.data.entries = append(b.data.entries[:idx], b.data.entries[idx+1:]...)
	}
	return nil
}

// ForEach iterates over all key-value pairs in sorted order.
func (b *MemBucket) ForEach(fn func(k, v []byte) error) error {
	// Iterate over a snapshot of indices to handle concurrent modifications.
	for i := 0; i < len(b.data.entries); i++ {
		if err := fn(b.data.entries[i].key, b.data.entries[i].value); err != nil {
			return err
		}
	}
	return nil
}

// Cursor returns a cursor for ordered iteration over the bucket.
func (b *MemBucket) Cursor() *MemCursor {
	return &MemCursor{data: b.data, pos: -1}
}

// search returns the index where key would be inserted (binary search).
func (b *MemBucket) search(key []byte) int {
	return sort.Search(len(b.data.entries), func(i int) bool {
		return bytes.Compare(b.data.entries[i].key, key) >= 0
	})
}

// ---------------------------------------------------------------------------
// MemCursor — ordered iteration
// ---------------------------------------------------------------------------

// MemCursor iterates over a bucket's entries in sorted key order.
type MemCursor struct {
	data *bucketData
	pos  int
}

// First moves to the first entry.
func (c *MemCursor) First() ([]byte, []byte) {
	if len(c.data.entries) == 0 {
		c.pos = 0
		return nil, nil
	}
	c.pos = 0
	return c.data.entries[0].key, c.data.entries[0].value
}

// Last moves to the last entry.
func (c *MemCursor) Last() ([]byte, []byte) {
	if len(c.data.entries) == 0 {
		c.pos = 0
		return nil, nil
	}
	c.pos = len(c.data.entries) - 1
	return c.data.entries[c.pos].key, c.data.entries[c.pos].value
}

// Next advances to the next entry.
func (c *MemCursor) Next() ([]byte, []byte) {
	c.pos++
	if c.pos >= len(c.data.entries) {
		return nil, nil
	}
	return c.data.entries[c.pos].key, c.data.entries[c.pos].value
}

// Prev moves to the previous entry.
func (c *MemCursor) Prev() ([]byte, []byte) {
	c.pos--
	if c.pos < 0 {
		return nil, nil
	}
	return c.data.entries[c.pos].key, c.data.entries[c.pos].value
}

// Seek moves to the first entry with key >= seek.
func (c *MemCursor) Seek(seek []byte) ([]byte, []byte) {
	c.pos = sort.Search(len(c.data.entries), func(i int) bool {
		return bytes.Compare(c.data.entries[i].key, seek) >= 0
	})
	if c.pos >= len(c.data.entries) {
		return nil, nil
	}
	return c.data.entries[c.pos].key, c.data.entries[c.pos].value
}
