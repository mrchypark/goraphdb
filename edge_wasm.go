//go:build js

package graphdb

import (
	"bytes"
	"context"
	"fmt"

	"github.com/mstrYoda/goraphdb/wasm"
)

func (db *DB) AddEdge(from, to NodeID, label string, props Props) (EdgeID, error) {
	if db.isClosed() {
		return 0, fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return 0, err
	}
	if err := db.verifyNodeExists(from); err != nil {
		return 0, fmt.Errorf("graphdb: source node: %w", err)
	}
	if err := db.verifyNodeExists(to); err != nil {
		return 0, fmt.Errorf("graphdb: target node: %w", err)
	}

	id := db.primaryShard().allocEdgeID()
	edge := &Edge{ID: id, From: from, To: to, Label: label, Props: props}
	srcShard := db.shardForEdge(from)
	dstShard := db.shardFor(to)

	if srcShard == dstShard {
		err := srcShard.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
			edgeData, err := encodeEdge(edge)
			if err != nil {
				return err
			}
			if err := tx.Bucket(bucketEdges).Put(encodeEdgeID(id), edgeData); err != nil {
				return err
			}
			if err := tx.Bucket(bucketAdjOut).Put(encodeAdjKey(from, id), encodeAdjValue(to, label)); err != nil {
				return err
			}
			if err := tx.Bucket(bucketAdjIn).Put(encodeAdjKey(to, id), encodeAdjValue(from, label)); err != nil {
				return err
			}
			if err := tx.Bucket(bucketIdxEdgeTyp).Put(encodeIndexKey(label, uint64(id)), nil); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			db.log.Error("failed to add edge", "from", from, "to", to, "label", label, "error", err)
			return 0, fmt.Errorf("graphdb: failed to add edge: %w", err)
		}
		srcShard.edgeCount.Add(1)
		db.walAppend(OpAddEdge, WALAddEdge{ID: id, From: from, To: to, Label: label, Props: props})
		if db.edgeBloom != nil {
			db.edgeBloom.Add(from, to)
		}
		if db.metrics != nil {
			db.metrics.EdgesCreated.Add(1)
		}
		db.log.Debug("edge added", "id", id, "from", from, "to", to, "label", label)
		return id, nil
	}

	err := srcShard.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
		edgeData, err := encodeEdge(edge)
		if err != nil {
			return err
		}
		if err := tx.Bucket(bucketEdges).Put(encodeEdgeID(id), edgeData); err != nil {
			return err
		}
		if err := tx.Bucket(bucketAdjOut).Put(encodeAdjKey(from, id), encodeAdjValue(to, label)); err != nil {
			return err
		}
		if err := tx.Bucket(bucketIdxEdgeTyp).Put(encodeIndexKey(label, uint64(id)), nil); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("graphdb: failed to add edge (source shard): %w", err)
	}
	srcShard.edgeCount.Add(1)

	err = dstShard.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
		return tx.Bucket(bucketAdjIn).Put(encodeAdjKey(to, id), encodeAdjValue(from, label))
	})
	if err != nil {
		db.log.Error("failed to add edge (adj_in)", "id", id, "from", from, "to", to, "error", err)
		return 0, fmt.Errorf("graphdb: failed to add edge (target shard adj_in): %w", err)
	}

	db.walAppend(OpAddEdge, WALAddEdge{ID: id, From: from, To: to, Label: label, Props: props})
	if db.edgeBloom != nil {
		db.edgeBloom.Add(from, to)
	}
	if db.metrics != nil {
		db.metrics.EdgesCreated.Add(1)
	}
	db.log.Debug("edge added", "id", id, "from", from, "to", to, "label", label)
	return id, nil
}

func (db *DB) AddEdgeBatch(edges []Edge) ([]EdgeID, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return nil, err
	}

	ids := make([]EdgeID, len(edges))

	if len(db.shards) == 1 {
		s := db.shards[0]
		err := s.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
			edgeBucket := tx.Bucket(bucketEdges)
			adjOutBucket := tx.Bucket(bucketAdjOut)
			adjInBucket := tx.Bucket(bucketAdjIn)
			idxBucket := tx.Bucket(bucketIdxEdgeTyp)

			for i := range edges {
				id := s.allocEdgeID()
				ids[i] = id
				e := &edges[i]
				e.ID = id
				edgeData, err := encodeEdge(e)
				if err != nil {
					return err
				}
				if err := edgeBucket.Put(encodeEdgeID(id), edgeData); err != nil {
					return err
				}
				if err := adjOutBucket.Put(encodeAdjKey(e.From, id), encodeAdjValue(e.To, e.Label)); err != nil {
					return err
				}
				if err := adjInBucket.Put(encodeAdjKey(e.To, id), encodeAdjValue(e.From, e.Label)); err != nil {
					return err
				}
				if err := idxBucket.Put(encodeIndexKey(e.Label, uint64(id)), nil); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("graphdb: batch edge add failed: %w", err)
		}
		s.edgeCount.Add(uint64(len(edges)))
		walEdges := make([]WALBatchEdge, len(edges))
		for i, e := range edges {
			walEdges[i] = WALBatchEdge{ID: e.ID, From: e.From, To: e.To, Label: e.Label, Props: e.Props}
		}
		db.walAppend(OpAddEdgeBatch, WALAddEdgeBatch{Edges: walEdges})
		if db.edgeBloom != nil {
			for _, e := range edges {
				db.edgeBloom.Add(e.From, e.To)
			}
		}
		return ids, nil
	}

	type adjInEntry struct {
		to     NodeID
		edgeID EdgeID
		from   NodeID
		label  string
	}
	srcGroups := make(map[*shard][]*Edge)
	dstGroups := make(map[*shard][]adjInEntry)

	for i := range edges {
		id := db.primaryShard().allocEdgeID()
		ids[i] = id
		edges[i].ID = id
		srcShard := db.shardForEdge(edges[i].From)
		dstShard := db.shardFor(edges[i].To)
		srcGroups[srcShard] = append(srcGroups[srcShard], &edges[i])
		dstGroups[dstShard] = append(dstGroups[dstShard], adjInEntry{
			to: edges[i].To, edgeID: id, from: edges[i].From, label: edges[i].Label,
		})
	}

	for s, batch := range srcGroups {
		err := s.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
			edgeBucket := tx.Bucket(bucketEdges)
			adjOutBucket := tx.Bucket(bucketAdjOut)
			idxBucket := tx.Bucket(bucketIdxEdgeTyp)
			for _, e := range batch {
				edgeData, err := encodeEdge(e)
				if err != nil {
					return err
				}
				if err := edgeBucket.Put(encodeEdgeID(e.ID), edgeData); err != nil {
					return err
				}
				if err := adjOutBucket.Put(encodeAdjKey(e.From, e.ID), encodeAdjValue(e.To, e.Label)); err != nil {
					return err
				}
				if err := idxBucket.Put(encodeIndexKey(e.Label, uint64(e.ID)), nil); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("graphdb: batch edge add failed: %w", err)
		}
		s.edgeCount.Add(uint64(len(batch)))
	}

	for s, batch := range dstGroups {
		err := s.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
			adjInBucket := tx.Bucket(bucketAdjIn)
			for _, entry := range batch {
				if err := adjInBucket.Put(encodeAdjKey(entry.to, entry.edgeID), encodeAdjValue(entry.from, entry.label)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("graphdb: batch edge add failed (adj_in): %w", err)
		}
	}

	walEdges := make([]WALBatchEdge, len(edges))
	for i, e := range edges {
		walEdges[i] = WALBatchEdge{ID: e.ID, From: e.From, To: e.To, Label: e.Label, Props: e.Props}
	}
	db.walAppend(OpAddEdgeBatch, WALAddEdgeBatch{Edges: walEdges})
	if db.edgeBloom != nil {
		for _, e := range edges {
			db.edgeBloom.Add(e.From, e.To)
		}
	}
	return ids, nil
}

func (db *DB) GetEdge(id EdgeID) (*Edge, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	return db.getEdge(id)
}

func (db *DB) getEdge(id EdgeID) (*Edge, error) {
	for _, s := range db.shards {
		var edge *Edge
		err := s.db.View(func(tx *wasm.MemTx) error {
			data := tx.Bucket(bucketEdges).Get(encodeEdgeID(id))
			if data == nil {
				return nil
			}
			var err error
			edge, err = decodeEdge(data)
			return err
		})
		if err != nil {
			return nil, err
		}
		if edge != nil {
			return edge, nil
		}
	}
	return nil, fmt.Errorf("graphdb: edge %d not found", id)
}

func (db *DB) DeleteEdge(id EdgeID) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}
	edge, err := db.getEdge(id)
	if err != nil {
		return err
	}
	err = db.deleteEdgeInternal(edge)
	if err != nil {
		db.log.Error("failed to delete edge", "id", id, "error", err)
	} else {
		if db.metrics != nil {
			db.metrics.EdgesDeleted.Add(1)
		}
		db.log.Debug("edge deleted", "id", id, "from", edge.From, "to", edge.To, "label", edge.Label)
	}
	return err
}

func (db *DB) deleteEdgeInternal(edge *Edge) error {
	srcShard := db.shardForEdge(edge.From)
	dstShard := db.shardFor(edge.To)

	err := srcShard.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
		if err := tx.Bucket(bucketEdges).Delete(encodeEdgeID(edge.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(bucketAdjOut).Delete(encodeAdjKey(edge.From, edge.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(bucketIdxEdgeTyp).Delete(encodeIndexKey(edge.Label, uint64(edge.ID))); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	srcShard.edgeCount.Add(^uint64(0))

	var adjErr error
	if dstShard == srcShard {
		adjErr = srcShard.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
			return tx.Bucket(bucketAdjIn).Delete(encodeAdjKey(edge.To, edge.ID))
		})
	} else {
		adjErr = dstShard.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
			return tx.Bucket(bucketAdjIn).Delete(encodeAdjKey(edge.To, edge.ID))
		})
	}
	if adjErr != nil {
		return adjErr
	}

	db.walAppend(OpDeleteEdge, WALDeleteEdge{ID: edge.ID, From: edge.From, To: edge.To, Label: edge.Label})
	return nil
}

func (db *DB) UpdateEdge(id EdgeID, props Props) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}
	edge, err := db.getEdge(id)
	if err != nil {
		return err
	}
	if edge.Props == nil {
		edge.Props = make(Props)
	}
	for k, v := range props {
		edge.Props[k] = v
	}
	s := db.shardForEdge(edge.From)
	err = s.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
		data, err := encodeEdge(edge)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketEdges).Put(encodeEdgeID(id), data)
	})
	if err != nil {
		db.log.Error("failed to update edge", "id", id, "error", err)
	} else {
		db.walAppend(OpUpdateEdge, WALUpdateEdge{ID: id, From: edge.From, Props: props})
		db.log.Debug("edge updated", "id", id)
	}
	return err
}

func (db *DB) OutEdges(id NodeID) ([]*Edge, error) {
	return db.getEdgesForNode(id, Outgoing)
}

func (db *DB) InEdges(id NodeID) ([]*Edge, error) {
	return db.getEdgesForNode(id, Incoming)
}

func (db *DB) Edges(id NodeID) ([]*Edge, error) {
	return db.getEdgesForNode(id, Both)
}

func (db *DB) OutEdgesLabeled(id NodeID, label string) ([]*Edge, error) {
	edges, err := db.getEdgesForNode(id, Outgoing)
	if err != nil {
		return nil, err
	}
	var filtered []*Edge
	for _, e := range edges {
		if e.Label == label {
			filtered = append(filtered, e)
		}
	}
	return filtered, nil
}

func (db *DB) InEdgesLabeled(id NodeID, label string) ([]*Edge, error) {
	edges, err := db.getEdgesForNode(id, Incoming)
	if err != nil {
		return nil, err
	}
	var filtered []*Edge
	for _, e := range edges {
		if e.Label == label {
			filtered = append(filtered, e)
		}
	}
	return filtered, nil
}

func (db *DB) Neighbors(id NodeID) ([]*Node, error) {
	return db.NeighborsDirection(id, Outgoing)
}

func (db *DB) NeighborsLabeled(id NodeID, label string) ([]*Node, error) {
	edges, err := db.OutEdgesLabeled(id, label)
	if err != nil {
		return nil, err
	}
	nodes := make([]*Node, 0, len(edges))
	for _, e := range edges {
		n, err := db.getNode(e.To)
		if err != nil {
			continue
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

func (db *DB) NeighborsDirection(id NodeID, dir Direction) ([]*Node, error) {
	edges, err := db.getEdgesForNode(id, dir)
	if err != nil {
		return nil, err
	}
	nodes := make([]*Node, 0, len(edges))
	seen := make(map[NodeID]bool)
	for _, e := range edges {
		targetID := e.To
		if dir == Incoming {
			targetID = e.From
		}
		if seen[targetID] {
			continue
		}
		seen[targetID] = true
		n, err := db.getNode(targetID)
		if err != nil {
			continue
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

func (db *DB) Degree(id NodeID, dir Direction) (int, error) {
	edges, err := db.getEdgesForNode(id, dir)
	if err != nil {
		return 0, err
	}
	return len(edges), nil
}

func (db *DB) HasEdge(from, to NodeID) (bool, error) {
	if db.edgeBloom != nil && !db.edgeBloom.Test(from, to) {
		if db.metrics != nil {
			db.metrics.BloomNegatives.Add(1)
		}
		return false, nil
	}
	edges, err := db.getEdgesForNode(from, Outgoing)
	if err != nil {
		return false, err
	}
	for _, e := range edges {
		if e.To == to {
			return true, nil
		}
	}
	return false, nil
}

func (db *DB) HasEdgeLabeled(from, to NodeID, label string) (bool, error) {
	edges, err := db.OutEdgesLabeled(from, label)
	if err != nil {
		return false, err
	}
	for _, e := range edges {
		if e.To == to {
			return true, nil
		}
	}
	return false, nil
}

func (db *DB) EdgesByLabel(label string) ([]*Edge, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	var edges []*Edge
	prefix := encodeIndexPrefix(label)
	for _, s := range db.shards {
		err := s.db.View(func(tx *wasm.MemTx) error {
			idxBucket := tx.Bucket(bucketIdxEdgeTyp)
			edgeBucket := tx.Bucket(bucketEdges)
			c := idxBucket.Cursor()
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				edgeIDBytes := k[len(prefix):]
				if len(edgeIDBytes) < 8 {
					continue
				}
				edgeID := decodeEdgeID(edgeIDBytes)
				data := edgeBucket.Get(encodeEdgeID(edgeID))
				if data == nil {
					continue
				}
				edge, err := decodeEdge(data)
				if err != nil {
					continue
				}
				edges = append(edges, edge)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return edges, nil
}

func (db *DB) verifyNodeExists(id NodeID) error {
	s := db.shardFor(id)
	return s.db.View(func(tx *wasm.MemTx) error {
		if tx.Bucket(bucketNodes).Get(encodeNodeID(id)) == nil {
			return fmt.Errorf("node %d not found", id)
		}
		return nil
	})
}

func (db *DB) getEdgesForNode(id NodeID, dir Direction) ([]*Edge, error) {
	s := db.shardFor(id)
	prefix := encodeNodeID(id)
	var edges []*Edge

	collectLocal := func(bucketName []byte) error {
		return s.db.View(func(tx *wasm.MemTx) error {
			b := tx.Bucket(bucketName)
			edgeBucket := tx.Bucket(bucketEdges)
			c := b.Cursor()
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				_, edgeID := decodeAdjKey(k)
				data := edgeBucket.Get(encodeEdgeID(edgeID))
				if data == nil {
					continue
				}
				edge, err := decodeEdge(data)
				if err != nil {
					continue
				}
				edges = append(edges, edge)
			}
			return nil
		})
	}

	collectIncoming := func() error {
		type adjEntry struct {
			edgeID   EdgeID
			sourceID NodeID
			label    string
		}
		var entries []adjEntry
		err := s.db.View(func(tx *wasm.MemTx) error {
			b := tx.Bucket(bucketAdjIn)
			c := b.Cursor()
			for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
				_, edgeID := decodeAdjKey(k)
				sourceID, label := decodeAdjValue(v)
				entries = append(entries, adjEntry{edgeID: edgeID, sourceID: sourceID, label: label})
			}
			return nil
		})
		if err != nil {
			return err
		}

		type shardGroup struct {
			shard   *shard
			edgeIDs []EdgeID
		}
		groups := make(map[*shard]*shardGroup)
		for _, e := range entries {
			srcShard := db.shardForEdge(e.sourceID)
			g, ok := groups[srcShard]
			if !ok {
				g = &shardGroup{shard: srcShard}
				groups[srcShard] = g
			}
			g.edgeIDs = append(g.edgeIDs, e.edgeID)
		}

		for _, g := range groups {
			err := g.shard.db.View(func(tx *wasm.MemTx) error {
				edgeBucket := tx.Bucket(bucketEdges)
				for _, eid := range g.edgeIDs {
					data := edgeBucket.Get(encodeEdgeID(eid))
					if data == nil {
						continue
					}
					edge, err := decodeEdge(data)
					if err != nil {
						continue
					}
					edges = append(edges, edge)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	}

	if dir == Outgoing || dir == Both {
		if err := collectLocal(bucketAdjOut); err != nil {
			return nil, err
		}
	}
	if dir == Incoming || dir == Both {
		if len(db.shards) == 1 {
			if err := collectLocal(bucketAdjIn); err != nil {
				return nil, err
			}
		} else {
			if err := collectIncoming(); err != nil {
				return nil, err
			}
		}
	}
	return edges, nil
}
