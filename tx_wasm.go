//go:build js

package graphdb

import (
	"fmt"
	"sync"

	"github.com/mstrYoda/goraphdb/wasm"
	"github.com/vmihailenco/msgpack/v5"
)

type Tx struct {
	db       *DB
	mu       sync.Mutex
	shardTxs map[int]*wasm.MemTx
	done     bool
}

func (db *DB) Begin() (*Tx, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	tx := &Tx{
		db:       db,
		shardTxs: make(map[int]*wasm.MemTx),
	}
	db.log.Debug("transaction started")
	return tx, nil
}

func (t *Tx) shardTx(shardIdx int) (*wasm.MemTx, error) {
	if tx, ok := t.shardTxs[shardIdx]; ok {
		return tx, nil
	}
	s := t.db.shards[shardIdx]
	btx, err := s.db.Begin(true)
	if err != nil {
		return nil, fmt.Errorf("graphdb: failed to begin shard tx %d: %w", shardIdx, err)
	}
	t.shardTxs[shardIdx] = btx
	return btx, nil
}

func (t *Tx) shardIdxFor(id NodeID) int {
	if len(t.db.shards) == 1 {
		return 0
	}
	return int(uint64(id) % uint64(len(t.db.shards)))
}

func (t *Tx) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return fmt.Errorf("graphdb: transaction already finished")
	}
	t.done = true
	var firstErr error
	for idx, btx := range t.shardTxs {
		s := t.db.shards[idx]
		if err := s.persistCounters(btx); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := btx.Commit(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	t.shardTxs = nil
	if firstErr != nil {
		t.db.log.Error("transaction commit failed", "error", firstErr)
	} else {
		t.db.log.Debug("transaction committed")
	}
	return firstErr
}

func (t *Tx) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return nil
	}
	t.done = true
	var firstErr error
	for _, btx := range t.shardTxs {
		if err := btx.Rollback(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	t.shardTxs = nil
	t.db.log.Debug("transaction rolled back")
	return firstErr
}

func (t *Tx) checkActive() error {
	if t.done {
		return fmt.Errorf("graphdb: transaction already finished")
	}
	return nil
}

func (t *Tx) AddNode(props Props) (NodeID, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkActive(); err != nil {
		return 0, err
	}
	s := t.db.primaryShard()
	id := s.allocNodeID()
	shardIdx := t.shardIdxFor(id)
	btx, err := t.shardTx(shardIdx)
	if err != nil {
		return 0, err
	}
	data, err := encodeProps(props)
	if err != nil {
		return 0, fmt.Errorf("graphdb: failed to encode node properties: %w", err)
	}
	if err := btx.Bucket(bucketNodes).Put(encodeNodeID(id), data); err != nil {
		return 0, err
	}
	if err := t.db.indexNodeProps(btx, id, props); err != nil {
		return 0, err
	}
	t.db.shards[shardIdx].nodeCount.Add(1)
	return id, nil
}

func (t *Tx) AddNodeWithLabels(labels []string, props Props) (NodeID, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkActive(); err != nil {
		return 0, err
	}
	s := t.db.primaryShard()
	id := s.allocNodeID()
	shardIdx := t.shardIdxFor(id)
	btx, err := t.shardTx(shardIdx)
	if err != nil {
		return 0, err
	}
	data, err := encodeProps(props)
	if err != nil {
		return 0, fmt.Errorf("graphdb: failed to encode node properties: %w", err)
	}
	if err := btx.Bucket(bucketNodes).Put(encodeNodeID(id), data); err != nil {
		return 0, err
	}
	if err := t.db.indexNodeProps(btx, id, props); err != nil {
		return 0, err
	}
	if len(labels) > 0 {
		labelData, err := msgpack.Marshal(labels)
		if err != nil {
			return 0, err
		}
		if err := btx.Bucket(bucketNodeLabels).Put(encodeNodeID(id), labelData); err != nil {
			return 0, err
		}
		idxBucket := btx.Bucket(bucketIdxNodeLabel)
		for _, label := range labels {
			if err := idxBucket.Put(encodeLabelIndexKey(label, id), nil); err != nil {
				return 0, err
			}
		}
	}
	t.db.shards[shardIdx].nodeCount.Add(1)
	return id, nil
}

func (t *Tx) GetNode(id NodeID) (*Node, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkActive(); err != nil {
		return nil, err
	}
	shardIdx := t.shardIdxFor(id)
	if btx, ok := t.shardTxs[shardIdx]; ok {
		data := btx.Bucket(bucketNodes).Get(encodeNodeID(id))
		if data == nil {
			return nil, fmt.Errorf("graphdb: node %d not found", id)
		}
		props, err := decodeProps(data)
		if err != nil {
			return nil, err
		}
		labels := loadLabels(btx, id)
		return &Node{ID: id, Labels: labels, Props: props}, nil
	}
	return t.db.getNode(id)
}

func (t *Tx) UpdateNode(id NodeID, props Props) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkActive(); err != nil {
		return err
	}
	shardIdx := t.shardIdxFor(id)
	btx, err := t.shardTx(shardIdx)
	if err != nil {
		return err
	}
	b := btx.Bucket(bucketNodes)
	key := encodeNodeID(id)
	existing := b.Get(key)
	if existing == nil {
		return fmt.Errorf("graphdb: node %d not found", id)
	}
	oldProps, err := decodeProps(existing)
	if err != nil {
		return err
	}
	if err := t.db.unindexNodeProps(btx, id, oldProps); err != nil {
		return err
	}
	for k, v := range props {
		oldProps[k] = v
	}
	data, err := encodeProps(oldProps)
	if err != nil {
		return err
	}
	if err := b.Put(key, data); err != nil {
		return err
	}
	return t.db.indexNodeProps(btx, id, oldProps)
}

func (t *Tx) DeleteNode(id NodeID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkActive(); err != nil {
		return err
	}
	shardIdx := t.shardIdxFor(id)
	btx, err := t.shardTx(shardIdx)
	if err != nil {
		return err
	}
	b := btx.Bucket(bucketNodes)
	key := encodeNodeID(id)
	existing := b.Get(key)
	if existing == nil {
		return fmt.Errorf("graphdb: node %d not found", id)
	}
	if props, err := decodeProps(existing); err == nil {
		_ = t.db.unindexNodeProps(btx, id, props)
	}
	labels := loadLabels(btx, id)
	if len(labels) > 0 {
		idxBucket := btx.Bucket(bucketIdxNodeLabel)
		for _, l := range labels {
			_ = idxBucket.Delete(encodeLabelIndexKey(l, id))
		}
		_ = btx.Bucket(bucketNodeLabels).Delete(key)
	}
	if err := b.Delete(key); err != nil {
		return err
	}
	t.db.shards[shardIdx].nodeCount.Add(^uint64(0))
	return nil
}

func (t *Tx) AddEdge(from, to NodeID, label string, props Props) (EdgeID, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkActive(); err != nil {
		return 0, err
	}
	id := t.db.primaryShard().allocEdgeID()
	edge := &Edge{ID: id, From: from, To: to, Label: label, Props: props}
	srcIdx := t.shardIdxFor(from)
	dstIdx := t.shardIdxFor(to)
	srcTx, err := t.shardTx(srcIdx)
	if err != nil {
		return 0, err
	}
	edgeData, err := encodeEdge(edge)
	if err != nil {
		return 0, err
	}
	if err := srcTx.Bucket(bucketEdges).Put(encodeEdgeID(id), edgeData); err != nil {
		return 0, err
	}
	if err := srcTx.Bucket(bucketAdjOut).Put(encodeAdjKey(from, id), encodeAdjValue(to, label)); err != nil {
		return 0, err
	}
	if err := srcTx.Bucket(bucketIdxEdgeTyp).Put(encodeIndexKey(label, uint64(id)), nil); err != nil {
		return 0, err
	}
	t.db.shards[srcIdx].edgeCount.Add(1)
	dstTx := srcTx
	if dstIdx != srcIdx {
		dstTx, err = t.shardTx(dstIdx)
		if err != nil {
			return 0, err
		}
	}
	if err := dstTx.Bucket(bucketAdjIn).Put(encodeAdjKey(to, id), encodeAdjValue(from, label)); err != nil {
		return 0, err
	}
	return id, nil
}

func (t *Tx) AddLabel(id NodeID, labels ...string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkActive(); err != nil {
		return err
	}
	if len(labels) == 0 {
		return nil
	}
	shardIdx := t.shardIdxFor(id)
	btx, err := t.shardTx(shardIdx)
	if err != nil {
		return err
	}
	if btx.Bucket(bucketNodes).Get(encodeNodeID(id)) == nil {
		return fmt.Errorf("graphdb: node %d not found", id)
	}
	existing := loadLabels(btx, id)
	labelSet := make(map[string]bool, len(existing))
	for _, l := range existing {
		labelSet[l] = true
	}
	var added []string
	for _, l := range labels {
		if !labelSet[l] {
			existing = append(existing, l)
			added = append(added, l)
		}
	}
	if len(added) == 0 {
		return nil
	}
	data, err := msgpack.Marshal(existing)
	if err != nil {
		return err
	}
	if err := btx.Bucket(bucketNodeLabels).Put(encodeNodeID(id), data); err != nil {
		return err
	}
	idxBucket := btx.Bucket(bucketIdxNodeLabel)
	for _, l := range added {
		if err := idxBucket.Put(encodeLabelIndexKey(l, id), nil); err != nil {
			return err
		}
	}
	return nil
}
