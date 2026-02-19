//go:build js

package graphdb

import (
	"context"
	"fmt"
	"sort"

	"github.com/mstrYoda/goraphdb/wasm"
)

type RowIterator interface {
	Next() bool
	Row() map[string]any
	Columns() []string
	Err() error
	Close()
}

func (db *DB) CypherStream(ctx context.Context, query string) (RowIterator, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	return safeExecuteResult(func() (RowIterator, error) {
		ctx, cancel := db.governor.wrapContext(ctx)
		_ = cancel
		ast := db.cache.get(query)
		if ast == nil {
			parsed, err := parseCypher(query)
			if err != nil {
				cancel()
				return nil, err
			}
			if parsed.write != nil {
				cancel()
				return nil, fmt.Errorf("graphdb: CypherStream does not support CREATE queries")
			}
			ast = parsed.read
			db.cache.put(query, ast)
		}
		return db.buildIterator(ctx, ast)
	})
}

func (db *DB) CypherStreamWithParams(ctx context.Context, query string, params map[string]any) (RowIterator, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	return safeExecuteResult(func() (RowIterator, error) {
		ctx, cancel := db.governor.wrapContext(ctx)
		_ = cancel
		ast := db.cache.get(query)
		if ast == nil {
			parsed, err := parseCypher(query)
			if err != nil {
				cancel()
				return nil, err
			}
			if parsed.write != nil {
				cancel()
				return nil, fmt.Errorf("graphdb: CypherStreamWithParams does not support CREATE queries")
			}
			ast = parsed.read
			db.cache.put(query, ast)
		}
		resolved := *ast
		if len(params) > 0 {
			if err := resolveParams(&resolved, params); err != nil {
				cancel()
				return nil, err
			}
		}
		return db.buildIterator(ctx, &resolved)
	})
}

func (db *DB) buildIterator(ctx context.Context, q *CypherQuery) (RowIterator, error) {
	if q.Explain != ExplainNone {
		result, err := db.executeCypher(ctx, q)
		if err != nil {
			return nil, err
		}
		return newSliceIterator(result.Columns, result.Rows), nil
	}
	if q.OptionalMatch != nil {
		result, err := db.executeCypherNormal(ctx, q)
		if err != nil {
			return nil, err
		}
		return newSliceIterator(result.Columns, result.Rows), nil
	}
	pat := q.Match.Pattern
	switch {
	case len(pat.Nodes) == 1 && len(pat.Rels) == 0:
		return db.buildNodeMatchIterator(q)
	case len(pat.Nodes) == 2 && len(pat.Rels) == 1:
		result, err := db.executeCypherNormal(ctx, q)
		if err != nil {
			return nil, err
		}
		return newSliceIterator(result.Columns, result.Rows), nil
	default:
		return nil, fmt.Errorf("cypher stream: unsupported pattern with %d nodes and %d relationships",
			len(pat.Nodes), len(pat.Rels))
	}
}

func (db *DB) buildNodeMatchIterator(q *CypherQuery) (RowIterator, error) {
	nodePat := q.Match.Pattern.Nodes[0]
	varName := nodePat.Variable
	if varName == "" {
		varName = "_n"
	}
	columns := make([]string, len(q.Return.Items))
	for i, item := range q.Return.Items {
		columns[i] = returnItemName(item)
	}
	if len(q.OrderBy) > 0 {
		result, err := db.executeCypherNormal(context.Background(), q)
		if err != nil {
			return nil, err
		}
		return newSliceIterator(result.Columns, result.Rows), nil
	}
	return db.newNodeScanIterator(q, nodePat, varName, columns)
}

type nodeScanIterator struct {
	db      *DB
	q       *CypherQuery
	nodePat NodePattern
	varName string
	columns []string
	limit   int
	emitted int
	row     map[string]any
	err     error
	closed  bool

	shardIdx int
	txs      []*wasm.MemTx
	cursor   *wasm.MemCursor
	curTx    *wasm.MemTx
}

func (db *DB) newNodeScanIterator(q *CypherQuery, nodePat NodePattern, varName string, columns []string) (*nodeScanIterator, error) {
	it := &nodeScanIterator{
		db:      db,
		q:       q,
		nodePat: nodePat,
		varName: varName,
		columns: columns,
		limit:   q.Limit,
		txs:     make([]*wasm.MemTx, len(db.shards)),
	}

	candidates, err := db.resolveNodeCandidates(q, nodePat, varName)
	if err != nil {
		return nil, err
	}
	if candidates != nil {
		var rows []map[string]any
		for _, n := range candidates {
			if q.Where != nil {
				bindings := map[string]any{varName: n}
				ok, evalErr := evalBool(q.Where, bindings)
				if evalErr != nil {
					return nil, evalErr
				}
				if !ok {
					continue
				}
			}
			row := make(map[string]any, len(q.Return.Items))
			bindings := map[string]any{varName: n}
			for _, item := range q.Return.Items {
				colName := returnItemName(item)
				val, evalErr := evalExpr(&item.Expr, bindings)
				if evalErr != nil {
					return nil, evalErr
				}
				row[colName] = val
			}
			rows = append(rows, row)
			if it.limit > 0 && len(rows) >= it.limit {
				break
			}
		}
		return nil, errUseFallback
	}

	for i, s := range db.shards {
		tx, txErr := s.db.Begin(false)
		if txErr != nil {
			for j := 0; j < i; j++ {
				it.txs[j].Rollback()
			}
			return nil, fmt.Errorf("graphdb: failed to begin read tx on shard %d: %w", i, txErr)
		}
		it.txs[i] = tx
	}

	it.shardIdx = 0
	it.advanceShard()
	return it, nil
}

var errUseFallback = fmt.Errorf("use fallback")

func (it *nodeScanIterator) advanceShard() {
	if it.shardIdx >= len(it.txs) {
		it.cursor = nil
		return
	}
	tx := it.txs[it.shardIdx]
	b := tx.Bucket(bucketNodes)
	c := b.Cursor()
	it.cursor = c
	it.curTx = tx
}

func (it *nodeScanIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	if it.limit > 0 && it.emitted >= it.limit {
		return false
	}
	for it.cursor != nil {
		var k, v []byte
		if it.row == nil && it.emitted == 0 {
			k, v = it.cursor.First()
		} else {
			k, v = it.cursor.Next()
		}
		for k == nil {
			it.shardIdx++
			if it.shardIdx >= len(it.txs) {
				it.cursor = nil
				return false
			}
			it.advanceShard()
			if it.cursor == nil {
				return false
			}
			k, v = it.cursor.First()
		}
		nodeID := decodeNodeID(k)
		props, decErr := decodeProps(v)
		if decErr != nil {
			continue
		}
		labels := loadLabels(it.curTx, nodeID)
		n := &Node{ID: nodeID, Labels: labels, Props: props}
		if len(it.nodePat.Labels) > 0 && !matchLabels(n.Labels, it.nodePat.Labels) {
			continue
		}
		if !matchProps(n.Props, it.nodePat.Props) {
			continue
		}
		if it.q.Where != nil {
			bindings := map[string]any{it.varName: n}
			ok, evalErr := evalBool(it.q.Where, bindings)
			if evalErr != nil {
				it.err = evalErr
				return false
			}
			if !ok {
				continue
			}
		}
		row := make(map[string]any, len(it.q.Return.Items))
		bindings := map[string]any{it.varName: n}
		for _, item := range it.q.Return.Items {
			colName := returnItemName(item)
			val, evalErr := evalExpr(&item.Expr, bindings)
			if evalErr != nil {
				it.err = evalErr
				return false
			}
			row[colName] = val
		}
		it.row = row
		it.emitted++
		return true
	}
	return false
}

func (it *nodeScanIterator) Row() map[string]any { return it.row }
func (it *nodeScanIterator) Columns() []string   { return it.columns }
func (it *nodeScanIterator) Err() error          { return it.err }

func (it *nodeScanIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	for _, tx := range it.txs {
		if tx != nil {
			tx.Rollback()
		}
	}
}

func (db *DB) resolveNodeCandidates(q *CypherQuery, nodePat NodePattern, varName string) ([]*Node, error) {
	if len(nodePat.Labels) > 0 {
		candidates, err := db.FindByLabel(nodePat.Labels[0])
		if err != nil {
			return nil, err
		}
		var filtered []*Node
		for _, n := range candidates {
			if !matchLabels(n.Labels, nodePat.Labels) {
				continue
			}
			if !matchProps(n.Props, nodePat.Props) {
				continue
			}
			filtered = append(filtered, n)
		}
		return filtered, nil
	}
	if len(nodePat.Props) >= 2 {
		propNames := make([]string, 0, len(nodePat.Props))
		for k := range nodePat.Props {
			propNames = append(propNames, k)
		}
		if db.HasCompositeIndex(propNames...) {
			filters := make(map[string]any, len(nodePat.Props))
			for k, v := range nodePat.Props {
				filters[k] = v
			}
			return db.FindByCompositeIndex(filters)
		}
	}
	if len(nodePat.Props) > 0 {
		for key, val := range nodePat.Props {
			if db.HasIndex(key) {
				candidates, err := db.FindByProperty(key, val)
				if err != nil {
					return nil, err
				}
				var filtered []*Node
				for _, n := range candidates {
					if matchProps(n.Props, nodePat.Props) {
						filtered = append(filtered, n)
					}
				}
				return filtered, nil
			}
		}
	}
	if q.Where != nil && len(nodePat.Props) == 0 {
		if prop, val, ok := extractWhereEquality(q.Where, varName); ok {
			if db.HasIndex(prop) {
				return db.FindByProperty(prop, val)
			}
		}
	}
	return nil, nil
}

type sliceIterator struct {
	columns []string
	rows    []map[string]any
	idx     int
}

func newSliceIterator(columns []string, rows []map[string]any) *sliceIterator {
	return &sliceIterator{columns: columns, rows: rows, idx: -1}
}

func (it *sliceIterator) Next() bool {
	it.idx++
	return it.idx < len(it.rows)
}

func (it *sliceIterator) Row() map[string]any {
	if it.idx < 0 || it.idx >= len(it.rows) {
		return nil
	}
	return it.rows[it.idx]
}

func (it *sliceIterator) Columns() []string { return it.columns }
func (it *sliceIterator) Err() error        { return nil }
func (it *sliceIterator) Close()            {}

type sortedIterator struct {
	inner *sliceIterator
}

func newSortedIterator(columns []string, rows []map[string]any, orderBy []OrderItem, limit int) *sortedIterator {
	sort.SliceStable(rows, func(i, j int) bool {
		for _, oi := range orderBy {
			vi := evalRowExpr(&oi.Expr, rows[i])
			vj := evalRowExpr(&oi.Expr, rows[j])
			cmp := compareValues(vi, vj)
			if oi.Desc {
				cmp = -cmp
			}
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return &sortedIterator{inner: newSliceIterator(columns, rows)}
}

func (it *sortedIterator) Next() bool          { return it.inner.Next() }
func (it *sortedIterator) Row() map[string]any { return it.inner.Row() }
func (it *sortedIterator) Columns() []string   { return it.inner.Columns() }
func (it *sortedIterator) Err() error          { return it.inner.Err() }
func (it *sortedIterator) Close()              { it.inner.Close() }

func collectIterator(iter RowIterator) (*CypherResult, error) {
	defer iter.Close()
	result := &CypherResult{Columns: iter.Columns()}
	for iter.Next() {
		result.Rows = append(result.Rows, iter.Row())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	if result.Rows == nil {
		result.Rows = []map[string]any{}
	}
	return result, nil
}
