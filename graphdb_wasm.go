//go:build js

package graphdb

import (
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mstrYoda/goraphdb/wasm"
)

// DB is the main graph database instance (WASM in-memory version).
type DB struct {
	opts              Options
	dir               string
	shards            []*shard
	pool              *workerPool
	cache             queryCache
	ncache            *nodeCache
	log               *slog.Logger
	mu                sync.Mutex
	closed            atomic.Bool
	indexedProps      sync.Map
	compositeIndexes  sync.Map
	uniqueConstraints sync.Map
	edgeBloom         *bloomFilter
	metrics           *Metrics
	slowLog           *slowQueryLog
	governor          *queryGovernor
	compactQuit       chan struct{}
	wal               *WAL
}

// WAL is a no-op stub for WASM builds. The WAL is not needed for in-memory mode.
type WAL struct{}

// LastLSN returns 0 for WASM builds (no WAL).
func (w *WAL) LastLSN() uint64 { return 0 }

// Append is a no-op for WASM builds.
func (w *WAL) Append(op OpType, payload []byte) (uint64, error) { return 0, nil }

// Close is a no-op for WASM builds.
func (w *WAL) Close() error { return nil }

// NewReader returns a no-op WALReader for WASM builds.
func (w *WAL) NewReader(fromLSN uint64) (*WALReader, error) {
	return &WALReader{}, nil
}

// PruneSegmentsBefore is a no-op for WASM builds.
func (w *WAL) PruneSegmentsBefore(minLSN uint64) (int, error) { return 0, nil }

// WALReader is a no-op stub for WASM builds.
type WALReader struct{}

// Next always returns io.EOF for WASM builds.
func (r *WALReader) Next() (*WALEntry, error) { return nil, io.EOF }

// Close is a no-op for WASM builds.
func (r *WALReader) Close() error { return nil }

// Applier is a no-op stub for WASM builds.
type Applier struct {
	db *DB
}

// NewApplier creates an Applier stub for WASM builds.
func NewApplier(db *DB) *Applier {
	return &Applier{db: db}
}

// AppliedLSN returns 0 for WASM builds.
func (a *Applier) AppliedLSN() uint64 { return 0 }

// ResetLSN is a no-op for WASM builds.
func (a *Applier) ResetLSN() {}

// Apply is a no-op for WASM builds.
func (a *Applier) Apply(entry *WALEntry) error { return nil }

// ApplyOp is a no-op for WASM builds.
func (a *Applier) ApplyOp(entry *WALEntry) error { return nil }

// SetAppliedLSN is a no-op for WASM builds.
func (a *Applier) SetAppliedLSN(lsn uint64) {}

// Compact is a no-op for WASM builds (in-memory, no file fragmentation).
func (db *DB) Compact() (int64, error) { return 0, nil }

// bloomFilter stub for WASM — full implementation below.
type bloomFilter struct {
	bits []uint64
	size uint64
	k    int
	mu   sync.RWMutex
}

// Open creates an in-memory graph database (WASM build).
// The dir parameter is ignored since there is no filesystem.
func Open(dir string, opts Options) (*DB, error) {
	if opts.ShardCount <= 0 {
		opts.ShardCount = 1
	}
	if opts.WorkerPoolSize <= 0 {
		opts.WorkerPoolSize = 4
	}
	if opts.CacheBudget <= 0 {
		opts.CacheBudget = 32 * 1024 * 1024 // 32MB default for WASM
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	writeTimeout := opts.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = 5 * time.Second
	}

	db := &DB{
		opts:   opts,
		dir:    dir,
		shards: make([]*shard, opts.ShardCount),
		cache:  newQueryCache(defaultQueryCacheCapacity),
		ncache: newNodeCache(opts.CacheBudget),
		log:    logger,
	}

	// Open all in-memory shards.
	for i := 0; i < opts.ShardCount; i++ {
		s, err := openShard(fmt.Sprintf("mem_shard_%04d", i), opts.NoSync, writeTimeout)
		if err != nil {
			for j := 0; j < i; j++ {
				db.shards[j].close()
			}
			return nil, err
		}
		db.shards[i] = s
	}

	// Start the worker pool.
	db.pool = newWorkerPool(opts.WorkerPoolSize)

	// Discover existing indexes (from in-memory buckets).
	db.discoverIndexes()
	db.discoverCompositeIndexes()
	db.discoverUniqueConstraints()

	// Initialize bloom filter.
	db.initBloomFilter()

	db.metrics = newMetrics(db)
	db.slowLog = newSlowQueryLog(100)
	db.governor = &queryGovernor{
		maxRows:        opts.MaxResultRows,
		defaultTimeout: opts.DefaultQueryTimeout,
	}

	db.compactQuit = make(chan struct{})

	db.log.Info("database opened (WASM in-memory)",
		"shards", opts.ShardCount,
		"workers", opts.WorkerPoolSize,
		"cache_budget_bytes", opts.CacheBudget,
	)

	return db, nil
}

// Close gracefully shuts down the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return nil
	}
	db.closed.Store(true)

	if db.compactQuit != nil {
		close(db.compactQuit)
	}

	if db.pool != nil {
		db.pool.stop()
	}

	var firstErr error
	for _, s := range db.shards {
		if s != nil {
			if err := s.close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}

	if firstErr != nil {
		db.log.Error("database closed with error", "error", firstErr)
	} else {
		db.log.Info("database closed")
	}

	return firstErr
}

// ErrReadOnlyReplica is returned when a write operation is attempted on a follower.
var ErrReadOnlyReplica = fmt.Errorf("graphdb: this is a read-only replica")

// ErrWriteQueueFull is returned when the write semaphore times out.
var ErrWriteQueueFull = fmt.Errorf("graphdb: write queue full")

func (db *DB) isFollower() bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.opts.Role == "follower"
}

func (db *DB) writeGuard() error {
	if db.isFollower() {
		return ErrReadOnlyReplica
	}
	return nil
}

// walAppend is a no-op for WASM builds (no WAL needed in-memory).
func (db *DB) walAppend(op OpType, payloadStruct any) {}

// SetRole dynamically changes the node's role.
func (db *DB) SetRole(role string) {
	db.mu.Lock()
	old := db.opts.Role
	db.opts.Role = role
	db.mu.Unlock()
	db.log.Info("role changed", "old", old, "new", role)
}

// Role returns the current role.
func (db *DB) Role() string {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.opts.Role
}

// WALMethod returns nil for WASM builds.
func (db *DB) WAL() *WAL {
	return nil
}

// Metrics returns the operational metrics collector.
func (db *DB) Metrics() *Metrics {
	return db.metrics
}

func (db *DB) isClosed() bool {
	return db.closed.Load()
}

// IsClosed returns true if the database has been closed.
func (db *DB) IsClosed() bool {
	return db.closed.Load()
}

func (db *DB) shardFor(id NodeID) *shard {
	if len(db.shards) == 1 {
		return db.shards[0]
	}
	return db.shards[uint64(id)%uint64(len(db.shards))]
}

func (db *DB) shardForEdge(fromID NodeID) *shard {
	return db.shardFor(fromID)
}

func (db *DB) primaryShard() *shard {
	return db.shards[0]
}

// Stats returns aggregate statistics across all shards.
func (db *DB) Stats() (*GraphStats, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	stats := &GraphStats{
		ShardCount: len(db.shards),
	}

	for _, s := range db.shards {
		stats.NodeCount += s.nodeCount.Load()
		stats.EdgeCount += s.edgeCount.Load()
		size, err := s.fileSize()
		if err != nil {
			return nil, err
		}
		stats.DiskSizeBytes += size
	}

	return stats, nil
}

// HasIndex returns true if a secondary index exists for the given property name.
func (db *DB) HasIndex(propName string) bool {
	_, ok := db.indexedProps.Load(propName)
	return ok
}

// NodeCount returns the total number of nodes across all shards.
func (db *DB) NodeCount() uint64 {
	var total uint64
	for _, s := range db.shards {
		total += s.nodeCount.Load()
	}
	return total
}

// EdgeCount returns the total number of edges across all shards.
func (db *DB) EdgeCount() uint64 {
	var total uint64
	for _, s := range db.shards {
		total += s.edgeCount.Load()
	}
	return total
}

// ---------------------------------------------------------------------------
// Bloom filter (WASM version — same logic, different init)
// ---------------------------------------------------------------------------

func newBloomFilter(expectedItems uint64) *bloomFilter {
	if expectedItems < 100 {
		expectedItems = 100
	}
	bitsNeeded := expectedItems * 10
	if bitsNeeded < 1024 {
		bitsNeeded = 1024
	}
	words := (bitsNeeded + 63) / 64
	return &bloomFilter{
		bits: make([]uint64, words),
		size: words * 64,
		k:    4,
	}
}

func (bf *bloomFilter) Add(from, to NodeID) {
	h1, h2 := bloomHash(from, to)
	bf.mu.Lock()
	for i := 0; i < bf.k; i++ {
		idx := (h1 + uint64(i)*h2) % bf.size
		bf.bits[idx/64] |= 1 << (idx % 64)
	}
	bf.mu.Unlock()
}

func (bf *bloomFilter) Test(from, to NodeID) bool {
	h1, h2 := bloomHash(from, to)
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	for i := 0; i < bf.k; i++ {
		idx := (h1 + uint64(i)*h2) % bf.size
		if bf.bits[idx/64]&(1<<(idx%64)) == 0 {
			return false
		}
	}
	return true
}

func bloomHash(from, to NodeID) (h1, h2 uint64) {
	// Simple hash for WASM — FNV-like mixing.
	h1 = uint64(from)*0x9E3779B97F4A7C15 + uint64(to)*0x517CC1B727220A95
	h2 = uint64(to)*0x9E3779B97F4A7C15 + uint64(from)*0x517CC1B727220A95
	if h2 == 0 {
		h2 = 1
	}
	return
}

func (db *DB) initBloomFilter() {
	var totalEdges uint64
	for _, s := range db.shards {
		totalEdges += s.edgeCount.Load()
	}
	expected := totalEdges * 2
	if expected < 1000 {
		expected = 1000
	}
	db.edgeBloom = newBloomFilter(expected)

	// Populate from adj_out bucket.
	for _, s := range db.shards {
		_ = s.db.View(func(tx *wasmMemTx) error {
			b := tx.Bucket(bucketAdjOut)
			if b == nil {
				return nil
			}
			return b.ForEach(func(k, v []byte) error {
				if len(k) >= 16 && len(v) >= 8 {
					from := NodeID(decodeNodeIDRaw(k[:8]))
					to := NodeID(decodeNodeIDRaw(v[:8]))
					db.edgeBloom.Add(from, to)
				}
				return nil
			})
		})
	}
}

// decodeNodeIDRaw decodes a big-endian uint64 from raw bytes (for bloom init).
func decodeNodeIDRaw(b []byte) uint64 {
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

// discoverIndexes scans the idx_prop bucket for existing property indexes.
func (db *DB) discoverIndexes() {
	for _, s := range db.shards {
		_ = s.db.View(func(tx *wasmMemTx) error {
			b := tx.Bucket(bucketIdxProp)
			if b == nil {
				return nil
			}
			c := b.Cursor()
			k, _ := c.First()
			for k != nil {
				colonIdx := -1
				for i, ch := range k {
					if ch == ':' {
						colonIdx = i
						break
					}
				}
				if colonIdx > 0 {
					prop := string(k[:colonIdx])
					db.indexedProps.Store(prop, true)
					nextPrefix := make([]byte, colonIdx+1)
					copy(nextPrefix, k[:colonIdx])
					nextPrefix[colonIdx] = ':' + 1
					k, _ = c.Seek(nextPrefix)
				} else {
					k, _ = c.Next()
				}
			}
			return nil
		})
	}
}

// wasmMemTx is a type alias for the WASM MemTx to use in method signatures.
type wasmMemTx = wasm.MemTx
