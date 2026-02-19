//go:build js

package graphdb

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/mstrYoda/goraphdb/wasm"
)

// Bucket name constants — must match the native build.
var (
	bucketNodes        = []byte("nodes")
	bucketEdges        = []byte("edges")
	bucketAdjOut       = []byte("adj_out")
	bucketAdjIn        = []byte("adj_in")
	bucketIdxProp      = []byte("idx_prop")
	bucketIdxEdgeTyp   = []byte("idx_edge_type")
	bucketNodeLabels   = []byte("node_labels")
	bucketIdxNodeLabel = []byte("idx_node_label")
	bucketIdxComposite = []byte("idx_composite")
	bucketIdxUnique    = []byte("idx_unique")
	bucketUniqueMeta   = []byte("unique_meta")
	bucketMeta         = []byte("meta")
)

var allBuckets = [][]byte{
	bucketNodes, bucketEdges, bucketAdjOut, bucketAdjIn,
	bucketIdxProp, bucketIdxEdgeTyp, bucketNodeLabels,
	bucketIdxNodeLabel, bucketIdxComposite, bucketIdxUnique,
	bucketUniqueMeta, bucketMeta,
}

// shard is the WASM in-memory shard implementation, replacing the bbolt-based shard.
type shard struct {
	db           *wasm.MemDB
	path         string
	nodeCounter  atomic.Uint64
	edgeCounter  atomic.Uint64
	nodeCount    atomic.Uint64
	edgeCount    atomic.Uint64
	writeSem     chan struct{}
	writeTimeout time.Duration
	noSync       bool
}

// openShard creates a new in-memory shard.
func openShard(_ string, noSync bool, writeTimeout time.Duration) (*shard, error) {
	db := wasm.NewMemDB()

	// Create all buckets.
	for _, name := range allBuckets {
		db.CreateBucket(name)
	}

	s := &shard{
		db:           db,
		writeSem:     make(chan struct{}, 1),
		writeTimeout: writeTimeout,
		noSync:       noSync,
	}
	s.writeSem <- struct{}{} // prime semaphore

	return s, nil
}

// initBuckets is a no-op for in-memory shards (buckets created in openShard).
func (s *shard) initBuckets() error {
	return nil
}

// allocNodeID returns the next node ID.
func (s *shard) allocNodeID() NodeID {
	return NodeID(s.nodeCounter.Add(1))
}

// allocEdgeID returns the next edge ID.
func (s *shard) allocEdgeID() EdgeID {
	return EdgeID(s.edgeCounter.Add(1))
}

// acquireWrite acquires the write semaphore.
func (s *shard) acquireWrite(ctx context.Context) error {
	if s.writeTimeout > 0 {
		timer := time.NewTimer(s.writeTimeout)
		defer timer.Stop()
		select {
		case <-s.writeSem:
			return nil
		case <-timer.C:
			return errors.New("graphdb: write timeout")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case <-s.writeSem:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseWrite releases the write semaphore.
func (s *shard) releaseWrite() {
	s.writeSem <- struct{}{}
}

// writeUpdate executes a write transaction with the write semaphore.
func (s *shard) writeUpdate(ctx context.Context, fn func(tx *wasm.MemTx) error) error {
	if err := s.acquireWrite(ctx); err != nil {
		return err
	}
	defer s.releaseWrite()
	return s.db.Update(fn)
}

// loadCounters reads node/edge counters from the meta bucket.
func (s *shard) loadCounters() {
	_ = s.db.View(func(tx *wasm.MemTx) error {
		meta := tx.Bucket(bucketMeta)
		if meta == nil {
			return nil
		}
		if v := meta.Get([]byte("node_counter")); len(v) == 8 {
			s.nodeCounter.Store(metaDecodeUint64(v))
		}
		if v := meta.Get([]byte("edge_counter")); len(v) == 8 {
			s.edgeCounter.Store(metaDecodeUint64(v))
		}

		// Count entries.
		nodeCount := uint64(0)
		nodesBucket := tx.Bucket(bucketNodes)
		if nodesBucket != nil {
			_ = nodesBucket.ForEach(func(_, _ []byte) error {
				nodeCount++
				return nil
			})
		}
		s.nodeCount.Store(nodeCount)

		edgeCount := uint64(0)
		edgesBucket := tx.Bucket(bucketEdges)
		if edgesBucket != nil {
			_ = edgesBucket.ForEach(func(_, _ []byte) error {
				edgeCount++
				return nil
			})
		}
		s.edgeCount.Store(edgeCount)

		return nil
	})
}

// persistCounters writes counters to the meta bucket.
func (s *shard) persistCounters(tx *wasm.MemTx) error {
	meta := tx.Bucket(bucketMeta)
	if meta == nil {
		return nil
	}
	if err := meta.Put([]byte("node_counter"), metaEncodeUint64(s.nodeCounter.Load())); err != nil {
		return err
	}
	return meta.Put([]byte("edge_counter"), metaEncodeUint64(s.edgeCounter.Load()))
}

// close closes the in-memory shard.
func (s *shard) close() error {
	return s.db.Close()
}

// fileSize returns 0 for in-memory shards (no disk).
func (s *shard) fileSize() (int64, error) {
	return 0, nil
}

// hasPrefix checks if a byte slice has a given prefix (utility).
func hasPrefix(b, prefix []byte) bool {
	return bytes.HasPrefix(b, prefix)
}

// metaDecodeUint64 reads a big-endian uint64 from an 8-byte slice.
func metaDecodeUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

// metaEncodeUint64 writes a big-endian uint64 to an 8-byte slice.
func metaEncodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
	return b
}
