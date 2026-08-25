package graphdb

import (
	"bytes"
	"context"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

// AtomicTx is a single-shard graph transaction. It is intended for replicated
// state machines that must commit a graph mutation and their application
// metadata together. It is only valid during UpdateAtomic's callback.
type AtomicTx struct {
	db          *DB
	tx          *bolt.Tx
	invalidated map[NodeID]struct{}
	bloomAdds   [][2]NodeID
}

// GetMetadata returns a copy of application metadata without opening a write transaction.
func (db *DB) GetMetadata(key []byte) ([]byte, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	if len(db.shards) != 1 {
		return nil, fmt.Errorf("graphdb: application metadata requires ShardCount=1")
	}
	var value []byte
	err := db.shards[0].db.View(func(tx *bolt.Tx) error {
		value = bytes.Clone(tx.Bucket(bucketAppMeta).Get(key))
		return nil
	})
	return value, err
}

// CypherReadWithParams executes a parameterized read and rejects every mutation.
func (db *DB) CypherReadWithParams(ctx context.Context, query string, params map[string]any) (*CypherResult, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	parsed, err := parseCypher(query)
	if err != nil {
		return nil, err
	}
	if parsed.read == nil || parsed.write != nil || parsed.merge != nil || len(parsed.read.Set) > 0 || len(parsed.read.Delete) > 0 {
		return nil, fmt.Errorf("graphdb: Cypher read API rejects mutations")
	}
	if err := resolveParams(parsed.read, params); err != nil {
		return nil, err
	}
	return db.executeCypherRead(ctx, query, parsed.read)
}

// UpdateAtomic executes fn in one durable bbolt transaction. Multi-shard
// databases are rejected because bbolt cannot atomically commit across files.
func (db *DB) UpdateAtomic(ctx context.Context, fn func(*AtomicTx) error) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if len(db.shards) != 1 {
		return fmt.Errorf("graphdb: atomic update requires ShardCount=1")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}
	s := db.shards[0]
	if err := s.acquireWrite(ctx); err != nil {
		return err
	}
	defer s.releaseWrite()

	btx, err := s.db.Begin(true)
	if err != nil {
		return fmt.Errorf("graphdb: begin atomic update: %w", err)
	}
	atx := &AtomicTx{db: db, tx: btx, invalidated: make(map[NodeID]struct{})}
	if err := fn(atx); err != nil {
		_ = btx.Rollback()
		return err
	}
	counters, err := atx.persistCounters()
	if err != nil {
		_ = btx.Rollback()
		return err
	}
	if err := btx.Commit(); err != nil {
		return fmt.Errorf("graphdb: commit atomic update: %w", err)
	}
	s.nextNodeID.Store(counters.nextNodeID)
	s.nextEdgeID.Store(counters.nextEdgeID)
	s.nodeCount.Store(counters.nodeCount)
	s.edgeCount.Store(counters.edgeCount)
	for id := range atx.invalidated {
		db.ncache.Invalidate(id)
	}
	if db.edgeBloom != nil {
		for _, pair := range atx.bloomAdds {
			db.edgeBloom.Add(pair[0], pair[1])
		}
	}
	return nil
}

// CypherWithParams executes one supported Cypher mutation inside the current
// transaction. Reads should use DB.CypherWithParams outside UpdateAtomic.
func (t *AtomicTx) CypherWithParams(ctx context.Context, query string, params map[string]any) (*CypherResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	parsed, err := parseCypher(query)
	if err != nil {
		return nil, err
	}
	switch {
	case parsed.write != nil:
		if err := resolveCreateParams(parsed.write, params); err != nil {
			return nil, err
		}
		return t.executeCreate(ctx, parsed.write)
	case parsed.merge != nil:
		if err := resolveMergeParams(parsed.merge, params); err != nil {
			return nil, err
		}
		return t.executeMerge(ctx, parsed.merge)
	case parsed.read != nil && (len(parsed.read.Set) > 0 || len(parsed.read.Delete) > 0):
		if err := resolveParams(parsed.read, params); err != nil {
			return nil, err
		}
		for i := range parsed.read.Set {
			if err := resolveExprParams(&parsed.read.Set[i].Value, params); err != nil {
				return nil, err
			}
		}
		return t.executeMatchMutation(ctx, parsed.read)
	default:
		return nil, fmt.Errorf("graphdb: atomic Cypher requires CREATE, MERGE, SET, or DELETE")
	}
}

func resolvePropertyParams(props map[string]any, params map[string]any) error {
	for key, value := range props {
		ref, ok := value.(paramRef)
		if !ok {
			continue
		}
		resolved, found := params[string(ref)]
		if !found {
			return fmt.Errorf("graphdb: missing parameter $%s", ref)
		}
		props[key] = resolved
	}
	return nil
}

func resolveReturnParams(ret *ReturnClause, params map[string]any) error {
	if ret == nil {
		return nil
	}
	for i := range ret.Items {
		if err := resolveExprParams(&ret.Items[i].Expr, params); err != nil {
			return err
		}
	}
	return nil
}

func resolveCreateParams(write *CypherWrite, params map[string]any) error {
	for i := range write.Creates {
		for j := range write.Creates[i].Nodes {
			if err := resolvePropertyParams(write.Creates[i].Nodes[j].Props, params); err != nil {
				return err
			}
		}
		for j := range write.Creates[i].Rels {
			if err := resolvePropertyParams(write.Creates[i].Rels[j].Props, params); err != nil {
				return err
			}
		}
	}
	return resolveReturnParams(write.Return, params)
}

func resolveMergeParams(merge *CypherMerge, params map[string]any) error {
	if err := resolvePropertyParams(merge.Pattern.Props, params); err != nil {
		return err
	}
	for _, items := range [][]SetItem{merge.OnCreateSet, merge.OnMatchSet} {
		for i := range items {
			if err := resolveExprParams(&items[i].Value, params); err != nil {
				return err
			}
		}
	}
	return resolveReturnParams(merge.Return, params)
}

func (t *AtomicTx) executeCreate(ctx context.Context, write *CypherWrite) (*CypherResult, error) {
	bindings := make(map[string]any)
	for _, pattern := range write.Creates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ids := make([]NodeID, len(pattern.Nodes))
		for i, nodePattern := range pattern.Nodes {
			if bound, found := bindings[nodePattern.Variable]; found && nodePattern.Variable != "" {
				node, ok := bound.(*Node)
				if !ok {
					return nil, fmt.Errorf("graphdb: CREATE variable %s is not a node", nodePattern.Variable)
				}
				ids[i] = node.ID
				continue
			}
			node, err := t.addNode(nodePattern.Labels, nodePattern.Props)
			if err != nil {
				return nil, fmt.Errorf("cypher exec: CREATE node failed: %w", err)
			}
			ids[i] = node.ID
			if nodePattern.Variable != "" {
				bindings[nodePattern.Variable] = node
			}
		}
		for i, relation := range pattern.Rels {
			from, to := ids[i], ids[i+1]
			if relation.Dir == Incoming {
				from, to = to, from
			}
			if relation.Label == "" {
				return nil, fmt.Errorf("cypher exec: CREATE relationship requires a label (type)")
			}
			edge, err := t.addEdge(from, to, relation.Label, relation.Props)
			if err != nil {
				return nil, fmt.Errorf("cypher exec: CREATE edge failed: %w", err)
			}
			if relation.Variable != "" {
				bindings[relation.Variable] = edge
			}
		}
	}
	return projectAtomicReturn(write.Return, bindings), nil
}

func (t *AtomicTx) executeMerge(ctx context.Context, merge *CypherMerge) (*CypherResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pattern := merge.Pattern
	matched, err := t.findNode(pattern.Labels, pattern.Props)
	if err != nil {
		return nil, err
	}
	created := matched == nil
	if created {
		matched, err = t.addNode(pattern.Labels, pattern.Props)
		if err != nil {
			return nil, fmt.Errorf("cypher exec: MERGE create failed: %w", err)
		}
	}
	bindings := map[string]any{}
	if pattern.Variable != "" {
		bindings[pattern.Variable] = matched
	}
	setItems := merge.OnMatchSet
	if created {
		setItems = merge.OnCreateSet
	}
	if len(setItems) > 0 {
		updates := make(Props, len(setItems))
		for _, item := range setItems {
			value, err := evalExpr(&item.Value, bindings)
			if err != nil {
				return nil, err
			}
			updates[item.Property] = value
		}
		matched, err = t.updateNode(matched.ID, updates)
		if err != nil {
			return nil, fmt.Errorf("cypher exec: MERGE SET failed: %w", err)
		}
		if pattern.Variable != "" {
			bindings[pattern.Variable] = matched
		}
	}
	return projectAtomicReturn(merge.Return, bindings), nil
}

func (t *AtomicTx) findNode(labels []string, props Props) (*Node, error) {
	if len(labels) == 0 {
		return nil, nil
	}
	prefix := encodeLabelIndexPrefix(labels[0])
	cursor := t.tx.Bucket(bucketIdxNodeLabel).Cursor()
	for key, _ := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
		id := decodeNodeID(key[len(prefix):])
		data := t.tx.Bucket(bucketNodes).Get(encodeNodeID(id))
		if data == nil {
			continue
		}
		actualProps, err := decodeProps(data)
		if err != nil {
			return nil, err
		}
		actualLabels := loadLabels(t.tx, id)
		if matchLabels(actualLabels, labels) && matchProps(actualProps, props) {
			return &Node{ID: id, Labels: actualLabels, Props: actualProps}, nil
		}
	}
	return nil, nil
}

func (t *AtomicTx) executeMatchMutation(ctx context.Context, query *CypherQuery) (*CypherResult, error) {
	readQuery := *query
	readQuery.Set = nil
	readQuery.Delete = nil
	result, err := t.db.executeCypher(ctx, &readQuery)
	if err != nil {
		return nil, err
	}
	for _, row := range result.Rows {
		for _, item := range query.Set {
			node, ok := row[item.Variable].(*Node)
			if !ok {
				continue
			}
			value, err := evalExpr(&item.Value, row)
			if err != nil {
				return nil, err
			}
			updated, err := t.updateNode(node.ID, Props{item.Property: value})
			if err != nil {
				return nil, err
			}
			row[item.Variable] = updated
		}
		for _, variable := range query.Delete {
			switch value := row[variable].(type) {
			case *Node:
				if err := t.deleteNode(value.ID); err != nil {
					return nil, err
				}
			case *Edge:
				if err := t.deleteEdge(value.ID); err != nil {
					return nil, err
				}
			}
		}
	}
	return result, nil
}

func projectAtomicReturn(ret *ReturnClause, bindings map[string]any) *CypherResult {
	result := &CypherResult{}
	if ret == nil {
		return result
	}
	row := make(map[string]any, len(ret.Items))
	for _, item := range ret.Items {
		name := item.Alias
		if name == "" {
			name = returnItemName(item)
		}
		result.Columns = append(result.Columns, name)
		value, _ := evalExpr(&item.Expr, bindings)
		row[name] = value
	}
	result.Rows = append(result.Rows, row)
	return result
}

type atomicCounters struct {
	nextNodeID uint64
	nextEdgeID uint64
	nodeCount  uint64
	edgeCount  uint64
}

func (t *AtomicTx) persistCounters() (atomicCounters, error) {
	meta := t.tx.Bucket(bucketMeta)
	nodes := t.tx.Bucket(bucketNodes)
	edges := t.tx.Bucket(bucketEdges)
	counters := atomicCounters{
		nextNodeID: decodeUint64(meta.Get(metaNextNodeID)),
		nextEdgeID: decodeUint64(meta.Get(metaNextEdgeID)),
		nodeCount:  uint64(nodes.Stats().KeyN),
		edgeCount:  uint64(edges.Stats().KeyN),
	}
	if err := meta.Put(metaNodeCount, encodeUint64(counters.nodeCount)); err != nil {
		return atomicCounters{}, err
	}
	if err := meta.Put(metaEdgeCount, encodeUint64(counters.edgeCount)); err != nil {
		return atomicCounters{}, err
	}
	return counters, nil
}

// GetMetadata returns a copy of application metadata, or nil when absent.
func (t *AtomicTx) GetMetadata(key []byte) []byte {
	value := t.tx.Bucket(bucketAppMeta).Get(key)
	return bytes.Clone(value)
}

// PutMetadata stores application metadata in the same transaction as graph changes.
func (t *AtomicTx) PutMetadata(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("graphdb: metadata key is empty")
	}
	return t.tx.Bucket(bucketAppMeta).Put(key, value)
}

func (t *AtomicTx) nextID(metaKey, dataBucket []byte) (uint64, error) {
	meta := t.tx.Bucket(bucketMeta)
	last := decodeUint64(meta.Get(metaKey))
	if key, _ := t.tx.Bucket(dataBucket).Cursor().Last(); key != nil {
		if stored := decodeUint64(key); stored > last {
			last = stored
		}
	}
	id := last + 1
	if err := meta.Put(metaKey, encodeUint64(id)); err != nil {
		return 0, err
	}
	return id, nil
}

func (t *AtomicTx) addNode(labels []string, props Props) (*Node, error) {
	idValue, err := t.nextID(metaNextNodeID, bucketNodes)
	if err != nil {
		return nil, err
	}
	id := NodeID(idValue)
	if err := t.db.checkUniqueConstraintsInTx(t.tx, labels, props, 0); err != nil {
		return nil, err
	}
	data, err := encodeProps(props)
	if err != nil {
		return nil, err
	}
	if err := t.tx.Bucket(bucketNodes).Put(encodeNodeID(id), data); err != nil {
		return nil, err
	}
	if err := t.db.indexNodeProps(t.tx, id, props); err != nil {
		return nil, err
	}
	if len(labels) > 0 {
		encoded, err := msgpack.Marshal(labels)
		if err != nil {
			return nil, err
		}
		if err := t.tx.Bucket(bucketNodeLabels).Put(encodeNodeID(id), encoded); err != nil {
			return nil, err
		}
		for _, label := range labels {
			if err := t.tx.Bucket(bucketIdxNodeLabel).Put(encodeLabelIndexKey(label, id), nil); err != nil {
				return nil, err
			}
		}
		if err := t.db.indexUniqueConstraints(t.tx, id, labels, props); err != nil {
			return nil, err
		}
	}
	t.invalidated[id] = struct{}{}
	return &Node{ID: id, Labels: append([]string(nil), labels...), Props: cloneProps(props)}, nil
}

func cloneProps(props Props) Props {
	copy := make(Props, len(props))
	for key, value := range props {
		copy[key] = value
	}
	return copy
}

func (t *AtomicTx) updateNode(id NodeID, updates Props) (*Node, error) {
	bucket := t.tx.Bucket(bucketNodes)
	key := encodeNodeID(id)
	data := bucket.Get(key)
	if data == nil {
		return nil, fmt.Errorf("graphdb: node %d not found", id)
	}
	props, err := decodeProps(data)
	if err != nil {
		return nil, err
	}
	labels := loadLabels(t.tx, id)
	if err := t.db.unindexNodeProps(t.tx, id, props); err != nil {
		return nil, err
	}
	if err := t.db.unindexUniqueConstraints(t.tx, id, labels, props); err != nil {
		return nil, err
	}
	for name, value := range updates {
		props[name] = value
	}
	if err := t.db.checkUniqueConstraintsInTx(t.tx, labels, props, id); err != nil {
		return nil, err
	}
	encoded, err := encodeProps(props)
	if err != nil {
		return nil, err
	}
	if err := bucket.Put(key, encoded); err != nil {
		return nil, err
	}
	if err := t.db.indexNodeProps(t.tx, id, props); err != nil {
		return nil, err
	}
	if err := t.db.indexUniqueConstraints(t.tx, id, labels, props); err != nil {
		return nil, err
	}
	t.invalidated[id] = struct{}{}
	return &Node{ID: id, Labels: labels, Props: props}, nil
}

func (t *AtomicTx) addEdge(from, to NodeID, label string, props Props) (*Edge, error) {
	if t.tx.Bucket(bucketNodes).Get(encodeNodeID(from)) == nil {
		return nil, fmt.Errorf("graphdb: source node %d not found", from)
	}
	if t.tx.Bucket(bucketNodes).Get(encodeNodeID(to)) == nil {
		return nil, fmt.Errorf("graphdb: target node %d not found", to)
	}
	idValue, err := t.nextID(metaNextEdgeID, bucketEdges)
	if err != nil {
		return nil, err
	}
	edge := &Edge{ID: EdgeID(idValue), From: from, To: to, Label: label, Props: cloneProps(props)}
	encoded, err := encodeEdge(edge)
	if err != nil {
		return nil, err
	}
	if err := t.tx.Bucket(bucketEdges).Put(encodeEdgeID(edge.ID), encoded); err != nil {
		return nil, err
	}
	if err := t.tx.Bucket(bucketAdjOut).Put(encodeAdjKey(from, edge.ID), encodeAdjValue(to, label)); err != nil {
		return nil, err
	}
	if err := t.tx.Bucket(bucketAdjIn).Put(encodeAdjKey(to, edge.ID), encodeAdjValue(from, label)); err != nil {
		return nil, err
	}
	if err := t.tx.Bucket(bucketIdxEdgeTyp).Put(encodeIndexKey(label, uint64(edge.ID)), nil); err != nil {
		return nil, err
	}
	t.bloomAdds = append(t.bloomAdds, [2]NodeID{from, to})
	return edge, nil
}

func (t *AtomicTx) deleteEdge(id EdgeID) error {
	data := t.tx.Bucket(bucketEdges).Get(encodeEdgeID(id))
	if data == nil {
		return fmt.Errorf("graphdb: edge %d not found", id)
	}
	edge, err := decodeEdge(data)
	if err != nil {
		return err
	}
	if err := t.tx.Bucket(bucketEdges).Delete(encodeEdgeID(id)); err != nil {
		return err
	}
	if err := t.tx.Bucket(bucketAdjOut).Delete(encodeAdjKey(edge.From, id)); err != nil {
		return err
	}
	if err := t.tx.Bucket(bucketAdjIn).Delete(encodeAdjKey(edge.To, id)); err != nil {
		return err
	}
	return t.tx.Bucket(bucketIdxEdgeTyp).Delete(encodeIndexKey(edge.Label, uint64(id)))
}

func (t *AtomicTx) deleteNode(id NodeID) error {
	prefix := encodeNodeID(id)
	edgeIDs := make(map[EdgeID]struct{})
	for _, name := range [][]byte{bucketAdjOut, bucketAdjIn} {
		cursor := t.tx.Bucket(name).Cursor()
		for key, _ := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
			_, edgeID := decodeAdjKey(key)
			edgeIDs[edgeID] = struct{}{}
		}
	}
	for edgeID := range edgeIDs {
		if err := t.deleteEdge(edgeID); err != nil {
			return err
		}
	}
	bucket := t.tx.Bucket(bucketNodes)
	key := encodeNodeID(id)
	data := bucket.Get(key)
	if data == nil {
		return fmt.Errorf("graphdb: node %d not found", id)
	}
	props, err := decodeProps(data)
	if err != nil {
		return err
	}
	labels := loadLabels(t.tx, id)
	if err := t.db.unindexNodeProps(t.tx, id, props); err != nil {
		return err
	}
	if err := t.db.unindexUniqueConstraints(t.tx, id, labels, props); err != nil {
		return err
	}
	for _, label := range labels {
		if err := t.tx.Bucket(bucketIdxNodeLabel).Delete(encodeLabelIndexKey(label, id)); err != nil {
			return err
		}
	}
	if err := t.tx.Bucket(bucketNodeLabels).Delete(key); err != nil {
		return err
	}
	if err := bucket.Delete(key); err != nil {
		return err
	}
	t.invalidated[id] = struct{}{}
	return nil
}
