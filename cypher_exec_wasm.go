//go:build js

package graphdb

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mstrYoda/goraphdb/wasm"
)

var errLimitReached = errors.New("graphdb: limit reached")

type CypherResult struct {
	Columns []string
	Rows    []map[string]any
	Plan    *QueryPlan
}

func (db *DB) Cypher(ctx context.Context, query string) (*CypherResult, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	return safeExecuteResult(func() (*CypherResult, error) {
		ctx, cancel := db.governor.wrapContext(ctx)
		defer cancel()
		ast := db.cache.get(query)
		if ast != nil {
			return db.executeCypherRead(ctx, query, ast)
		}
		parsed, err := parseCypher(query)
		if err != nil {
			return nil, err
		}
		if parsed.write != nil {
			start := time.Now()
			cr, err := db.executeCreate(ctx, parsed.write)
			elapsed := time.Since(start)
			if db.metrics != nil {
				db.metrics.QueriesTotal.Add(1)
				db.metrics.recordQueryDuration(elapsed)
				if err != nil {
					db.metrics.QueryErrorTotal.Add(1)
				}
			}
			if err != nil {
				return nil, err
			}
			return &CypherResult{Columns: cr.Columns, Rows: cr.Rows}, nil
		}
		if parsed.merge != nil {
			start := time.Now()
			result, err := db.executeMerge(ctx, parsed.merge)
			elapsed := time.Since(start)
			if db.metrics != nil {
				db.metrics.QueriesTotal.Add(1)
				db.metrics.recordQueryDuration(elapsed)
				if err != nil {
					db.metrics.QueryErrorTotal.Add(1)
				}
			}
			return result, err
		}
		if len(parsed.read.Set) > 0 || len(parsed.read.Delete) > 0 {
			if err := db.writeGuard(); err != nil {
				return nil, err
			}
			start := time.Now()
			result, err := db.executeCypherMutating(ctx, parsed.read)
			elapsed := time.Since(start)
			if db.metrics != nil {
				db.metrics.QueriesTotal.Add(1)
				db.metrics.recordQueryDuration(elapsed)
				if err != nil {
					db.metrics.QueryErrorTotal.Add(1)
				}
			}
			return result, err
		}
		db.cache.put(query, parsed.read)
		return db.executeCypherRead(ctx, query, parsed.read)
	})
}

func (db *DB) executeCypherRead(ctx context.Context, query string, ast *CypherQuery) (*CypherResult, error) {
	start := time.Now()
	result, err := db.executeCypher(ctx, ast)
	elapsed := time.Since(start)
	if db.metrics != nil {
		db.metrics.QueriesTotal.Add(1)
		db.metrics.recordQueryDuration(elapsed)
		if err != nil {
			db.metrics.QueryErrorTotal.Add(1)
		}
	}
	if err == nil {
		db.slowQueryCheck(query, elapsed, len(result.Rows))
	}
	return result, err
}

func (db *DB) executeCypherMutating(ctx context.Context, q *CypherQuery) (*CypherResult, error) {
	readQ := *q
	readQ.Set = nil
	readQ.Delete = nil
	result, err := db.executeCypher(ctx, &readQ)
	if err != nil {
		return nil, err
	}
	if len(q.Set) > 0 {
		for _, row := range result.Rows {
			for _, si := range q.Set {
				nodeVal, ok := row[si.Variable]
				if !ok {
					continue
				}
				node, ok := nodeVal.(*Node)
				if !ok {
					continue
				}
				val, err := evalExpr(&si.Value, row)
				if err != nil {
					return nil, fmt.Errorf("cypher exec: SET value eval: %w", err)
				}
				if err := db.UpdateNode(node.ID, Props{si.Property: val}); err != nil {
					return nil, fmt.Errorf("cypher exec: SET %s.%s failed: %w", si.Variable, si.Property, err)
				}
				node.Props[si.Property] = val
			}
		}
	}
	if len(q.Delete) > 0 {
		for _, row := range result.Rows {
			for _, varName := range q.Delete {
				val, ok := row[varName]
				if !ok {
					continue
				}
				switch v := val.(type) {
				case *Node:
					if err := db.DeleteNode(v.ID); err != nil {
						return nil, fmt.Errorf("cypher exec: DELETE node %d failed: %w", v.ID, err)
					}
				case *Edge:
					if err := db.DeleteEdge(v.ID); err != nil {
						return nil, fmt.Errorf("cypher exec: DELETE edge %d failed: %w", v.ID, err)
					}
				}
			}
		}
	}
	return result, nil
}

func (db *DB) CypherWithParams(ctx context.Context, query string, params map[string]any) (*CypherResult, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	return safeExecuteResult(func() (*CypherResult, error) {
		ctx, cancel := db.governor.wrapContext(ctx)
		defer cancel()
		ast := db.cache.get(query)
		if ast == nil {
			parsed, err := parseCypher(query)
			if err != nil {
				return nil, err
			}
			if parsed.write != nil {
				return nil, fmt.Errorf("cypher exec: parameterized CREATE queries are not supported")
			}
			ast = parsed.read
			db.cache.put(query, ast)
		}
		resolved := *ast
		if err := resolveParams(&resolved, params); err != nil {
			return nil, err
		}
		start := time.Now()
		result, err := db.executeCypher(ctx, &resolved)
		elapsed := time.Since(start)
		if db.metrics != nil {
			db.metrics.QueriesTotal.Add(1)
			db.metrics.recordQueryDuration(elapsed)
			if err != nil {
				db.metrics.QueryErrorTotal.Add(1)
			}
		}
		if err == nil {
			db.slowQueryCheck(query, elapsed, len(result.Rows))
		}
		return result, err
	})
}

func resolveParams(q *CypherQuery, params map[string]any) error {
	for i := range q.Match.Pattern.Nodes {
		np := &q.Match.Pattern.Nodes[i]
		for k, v := range np.Props {
			if ref, ok := v.(paramRef); ok {
				val, exists := params[string(ref)]
				if !exists {
					return fmt.Errorf("graphdb: missing parameter $%s", string(ref))
				}
				np.Props[k] = val
			}
		}
	}
	if q.Where != nil {
		if err := resolveExprParams(q.Where, params); err != nil {
			return err
		}
	}
	for i := range q.Return.Items {
		if err := resolveExprParams(&q.Return.Items[i].Expr, params); err != nil {
			return err
		}
	}
	return nil
}

func resolveExprParams(expr *Expression, params map[string]any) error {
	if expr == nil {
		return nil
	}
	switch expr.Kind {
	case ExprParam:
		val, exists := params[expr.ParamName]
		if !exists {
			return fmt.Errorf("graphdb: missing parameter $%s", expr.ParamName)
		}
		expr.Kind = ExprLiteral
		expr.LitValue = val
		expr.ParamName = ""
	case ExprComparison:
		if err := resolveExprParams(expr.Left, params); err != nil {
			return err
		}
		if err := resolveExprParams(expr.Right, params); err != nil {
			return err
		}
	case ExprAnd, ExprOr:
		for i := range expr.Operands {
			if err := resolveExprParams(&expr.Operands[i], params); err != nil {
				return err
			}
		}
	case ExprNot:
		if err := resolveExprParams(expr.Inner, params); err != nil {
			return err
		}
	case ExprFuncCall:
		for i := range expr.Args {
			if err := resolveExprParams(&expr.Args[i], params); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) executeCypher(ctx context.Context, q *CypherQuery) (*CypherResult, error) {
	if q.Explain == ExplainOnly {
		plan := buildPlan(q, db)
		return &CypherResult{Plan: &QueryPlan{Root: plan, Profile: false}}, nil
	}
	if q.Explain == ExplainProfile {
		plan := buildPlan(q, db)
		start := time.Now()
		normalQ := *q
		normalQ.Explain = ExplainNone
		result, err := db.executeCypherNormal(ctx, &normalQ)
		if err != nil {
			return nil, err
		}
		elapsed := time.Since(start)
		annotateProfileStats(plan, len(result.Rows), elapsed)
		result.Plan = &QueryPlan{Root: plan, Profile: true, Result: result}
		return result, nil
	}
	return db.executeCypherNormal(ctx, q)
}

func (db *DB) executeCypherNormal(ctx context.Context, q *CypherQuery) (*CypherResult, error) {
	if q.OptionalMatch != nil {
		return db.execWithOptionalMatch(ctx, q)
	}
	pat := q.Match.Pattern
	switch {
	case len(pat.Nodes) == 1 && len(pat.Rels) == 0:
		return db.execNodeMatch(ctx, q)
	case len(pat.Nodes) == 2 && len(pat.Rels) == 1:
		rel := pat.Rels[0]
		if rel.VarLength {
			return db.execVarLengthMatch(ctx, q)
		}
		return db.execSingleHopMatch(ctx, q)
	default:
		return nil, fmt.Errorf("cypher exec: unsupported pattern with %d nodes and %d relationships",
			len(pat.Nodes), len(pat.Rels))
	}
}

func annotateProfileStats(node *PlanNode, totalRows int, elapsed time.Duration) {
	node.ActualRows = totalRows
	node.ElapsedTime = elapsed
	for _, child := range node.Children {
		child.ActualRows = totalRows
	}
}

var errContextDone = errors.New("graphdb: context done")

func (db *DB) execNodeMatch(ctx context.Context, q *CypherQuery) (*CypherResult, error) {
	nodePat := q.Match.Pattern.Nodes[0]
	varName := nodePat.Variable
	if varName == "" {
		varName = "_n"
	}
	hasOrderBy := len(q.OrderBy) > 0
	limit := q.Limit

	if len(nodePat.Labels) > 0 {
		candidates, err := db.FindByLabel(nodePat.Labels[0])
		if err != nil {
			return nil, err
		}
		var nodes []*Node
		for _, n := range candidates {
			if !matchLabels(n.Labels, nodePat.Labels) {
				continue
			}
			if !matchProps(n.Props, nodePat.Props) {
				continue
			}
			if q.Where != nil {
				bindings := map[string]any{varName: n}
				ok, err := evalBool(q.Where, bindings)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}
			nodes = append(nodes, n)
			if limit > 0 && !hasOrderBy && len(nodes) >= limit {
				break
			}
		}
		return db.projectResults(q, nodes, nil, varName, "")
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
			candidates, err := db.FindByCompositeIndex(filters)
			if err != nil {
				return nil, err
			}
			var nodes []*Node
			for _, n := range candidates {
				if q.Where != nil {
					bindings := map[string]any{varName: n}
					ok, err := evalBool(q.Where, bindings)
					if err != nil {
						return nil, err
					}
					if !ok {
						continue
					}
				}
				nodes = append(nodes, n)
			}
			return db.projectResults(q, nodes, nil, varName, "")
		}
	}

	if len(nodePat.Props) > 0 {
		for key, val := range nodePat.Props {
			if db.HasIndex(key) {
				return db.execNodeMatchWithIndex(q, key, val, nodePat, varName)
			}
		}
	}

	if q.Where != nil && len(nodePat.Props) == 0 {
		if prop, val, ok := extractWhereEquality(q.Where, varName); ok {
			if db.HasIndex(prop) {
				return db.execNodeMatchWhereIndexed(q, prop, val, nodePat, varName)
			}
		}
	}

	var nodes []*Node
	err := db.forEachNode(func(n *Node) error {
		if err := ctx.Err(); err != nil {
			return errContextDone
		}
		if !matchProps(n.Props, nodePat.Props) {
			return nil
		}
		if q.Where != nil {
			bindings := map[string]any{varName: n}
			ok, err := evalBool(q.Where, bindings)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
		nodes = append(nodes, n)
		if limit > 0 && !hasOrderBy && len(nodes) >= limit {
			return errLimitReached
		}
		if err := db.governor.checkRowCount(len(nodes)); err != nil {
			return errRowLimitReached
		}
		return nil
	})
	if errors.Is(err, errContextDone) {
		return nil, ctx.Err()
	}
	if errors.Is(err, errRowLimitReached) {
		return nil, ErrResultTooLarge
	}
	if err != nil && !errors.Is(err, errLimitReached) {
		return nil, err
	}
	return db.projectResults(q, nodes, nil, varName, "")
}

func (db *DB) execNodeMatchWithIndex(q *CypherQuery, indexKey string, indexVal any, nodePat NodePattern, varName string) (*CypherResult, error) {
	candidates, err := db.FindByProperty(indexKey, indexVal)
	if err != nil {
		return nil, err
	}
	var nodes []*Node
	for _, n := range candidates {
		if !matchProps(n.Props, nodePat.Props) {
			continue
		}
		if q.Where != nil {
			bindings := map[string]any{varName: n}
			ok, err := evalBool(q.Where, bindings)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		nodes = append(nodes, n)
	}
	return db.projectResults(q, nodes, nil, varName, "")
}

func (db *DB) execNodeMatchWhereIndexed(q *CypherQuery, prop string, val any, nodePat NodePattern, varName string) (*CypherResult, error) {
	candidates, err := db.FindByProperty(prop, val)
	if err != nil {
		return nil, err
	}
	var nodes []*Node
	for _, n := range candidates {
		if !matchProps(n.Props, nodePat.Props) {
			continue
		}
		if q.Where != nil {
			bindings := map[string]any{varName: n}
			ok, err := evalBool(q.Where, bindings)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		nodes = append(nodes, n)
	}
	return db.projectResults(q, nodes, nil, varName, "")
}

func extractWhereEquality(where *Expression, nodeVar string) (string, any, bool) {
	if where == nil {
		return "", nil, false
	}
	if where.Kind == ExprComparison && where.Op == OpEq {
		if where.Left != nil && where.Left.Kind == ExprPropAccess && where.Left.Object == nodeVar &&
			where.Right != nil && where.Right.Kind == ExprLiteral {
			return where.Left.Property, where.Right.LitValue, true
		}
		if where.Right != nil && where.Right.Kind == ExprPropAccess && where.Right.Object == nodeVar &&
			where.Left != nil && where.Left.Kind == ExprLiteral {
			return where.Right.Property, where.Left.LitValue, true
		}
	}
	if where.Kind == ExprAnd {
		for _, op := range where.Operands {
			if prop, val, ok := extractWhereEquality(&op, nodeVar); ok {
				return prop, val, true
			}
		}
	}
	return "", nil, false
}

func (db *DB) execSingleHopMatch(ctx context.Context, q *CypherQuery) (*CypherResult, error) {
	pat := q.Match.Pattern
	aPat := pat.Nodes[0]
	rel := pat.Rels[0]
	bPat := pat.Nodes[1]
	aVar := aPat.Variable
	if aVar == "" {
		aVar = "_a"
	}
	bVar := bPat.Variable
	if bVar == "" {
		bVar = "_b"
	}
	rVar := rel.Variable

	if len(aPat.Props) == 0 && rel.Label != "" && rel.Dir == Outgoing {
		return db.execSingleHopByEdgeType(ctx, q, aPat, rel, bPat, aVar, rVar, bVar)
	}

	limit := q.Limit
	hasOrderBy := len(q.OrderBy) > 0
	aCandidates, err := db.findCandidates(aPat)
	if err != nil {
		return nil, err
	}
	var rows []resultRow
	for _, a := range aCandidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if limit > 0 && !hasOrderBy && len(rows) >= limit {
			break
		}
		edges, err := db.getEdgesForNode(a.ID, rel.Dir)
		if err != nil {
			return nil, err
		}
		for _, e := range edges {
			if rel.Label != "" && !strings.EqualFold(e.Label, rel.Label) {
				continue
			}
			targetID := e.To
			if rel.Dir == Incoming {
				targetID = e.From
			}
			bNode, err := db.getNode(targetID)
			if err != nil {
				continue
			}
			if !matchProps(bNode.Props, bPat.Props) {
				continue
			}
			if q.Where != nil {
				bindings := map[string]any{aVar: a, bVar: bNode}
				if rVar != "" {
					bindings[rVar] = e
				}
				ok, err := evalBool(q.Where, bindings)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}
			rows = append(rows, resultRow{a: a, r: e, b: bNode})
			if limit > 0 && !hasOrderBy && len(rows) >= limit {
				break
			}
			if err := db.governor.checkRowCount(len(rows)); err != nil {
				return nil, ErrResultTooLarge
			}
		}
	}
	return db.projectPatternResults(q, rows, aVar, rVar, bVar)
}

// execSingleHopByEdgeType — the only function that uses MemTx directly.
func (db *DB) execSingleHopByEdgeType(ctx context.Context, q *CypherQuery, aPat NodePattern, rel RelPattern, bPat NodePattern, aVar, rVar, bVar string) (*CypherResult, error) {
	prefix := encodeIndexPrefix(rel.Label)
	var rows []resultRow
	limit := q.Limit
	hasOrderBy := len(q.OrderBy) > 0

	for _, s := range db.shards {
		if limit > 0 && !hasOrderBy && len(rows) >= limit {
			break
		}
		err := s.db.View(func(tx *wasm.MemTx) error {
			idxBucket := tx.Bucket(bucketIdxEdgeTyp)
			edgeBucket := tx.Bucket(bucketEdges)
			nodeBucket := tx.Bucket(bucketNodes)
			c := idxBucket.Cursor()
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				if err := ctx.Err(); err != nil {
					return errContextDone
				}
				edgeIDBytes := k[len(prefix):]
				if len(edgeIDBytes) < 8 {
					continue
				}
				edgeID := decodeEdgeID(edgeIDBytes)
				edgeData := edgeBucket.Get(encodeEdgeID(edgeID))
				if edgeData == nil {
					continue
				}
				edge, err := decodeEdge(edgeData)
				if err != nil {
					continue
				}
				aData := nodeBucket.Get(encodeNodeID(edge.From))
				if aData == nil {
					continue
				}
				aProps, err := decodeProps(aData)
				if err != nil {
					continue
				}
				aLabels := loadLabels(tx, edge.From)
				aNode := &Node{ID: edge.From, Labels: aLabels, Props: aProps}
				if len(aPat.Labels) > 0 && !matchLabels(aNode.Labels, aPat.Labels) {
					continue
				}
				var bNode *Node
				bData := nodeBucket.Get(encodeNodeID(edge.To))
				if bData != nil {
					bProps, err := decodeProps(bData)
					if err == nil {
						bLabels := loadLabels(tx, edge.To)
						bNode = &Node{ID: edge.To, Labels: bLabels, Props: bProps}
					}
				}
				if bNode == nil {
					bNode, err = db.getNode(edge.To)
					if err != nil {
						continue
					}
				}
				if len(bPat.Labels) > 0 && !matchLabels(bNode.Labels, bPat.Labels) {
					continue
				}
				if !matchProps(bNode.Props, bPat.Props) {
					continue
				}
				if q.Where != nil {
					bindings := map[string]any{aVar: aNode, bVar: bNode}
					if rVar != "" {
						bindings[rVar] = edge
					}
					ok, err := evalBool(q.Where, bindings)
					if err != nil {
						return err
					}
					if !ok {
						continue
					}
				}
				rows = append(rows, resultRow{a: aNode, r: edge, b: bNode})
				if limit > 0 && !hasOrderBy && len(rows) >= limit {
					return errLimitReached
				}
				if db.governor.checkRowCount(len(rows)) != nil {
					return errRowLimitReached
				}
			}
			return nil
		})
		if errors.Is(err, errLimitReached) {
			break
		}
		if errors.Is(err, errContextDone) {
			return nil, ctx.Err()
		}
		if errors.Is(err, errRowLimitReached) {
			return nil, ErrResultTooLarge
		}
		if err != nil {
			return nil, err
		}
	}
	return db.projectPatternResults(q, rows, aVar, rVar, bVar)
}

func (db *DB) execVarLengthMatch(ctx context.Context, q *CypherQuery) (*CypherResult, error) {
	pat := q.Match.Pattern
	aPat := pat.Nodes[0]
	rel := pat.Rels[0]
	bPat := pat.Nodes[1]
	aVar := aPat.Variable
	if aVar == "" {
		aVar = "_a"
	}
	bVar := bPat.Variable
	if bVar == "" {
		bVar = "_b"
	}
	limit := q.Limit
	hasOrderBy := len(q.OrderBy) > 0
	aCandidates, err := db.findCandidates(aPat)
	if err != nil {
		return nil, err
	}
	maxDepth := rel.MaxHops
	if maxDepth < 0 {
		maxDepth = 50
	}
	minDepth := rel.MinHops
	var rows []resultRow
	for _, a := range aCandidates {
		if limit > 0 && !hasOrderBy && len(rows) >= limit {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var edgeFilter EdgeFilter
		if rel.Label != "" {
			label := rel.Label
			edgeFilter = func(e *Edge) bool {
				return strings.EqualFold(e.Label, label)
			}
		}
		err := db.BFS(a.ID, maxDepth, rel.Dir, edgeFilter, func(tr *TraversalResult) bool {
			if tr.Depth < minDepth {
				return true
			}
			bNode := tr.Node
			if len(bPat.Labels) > 0 && !matchLabels(bNode.Labels, bPat.Labels) {
				return true
			}
			if !matchProps(bNode.Props, bPat.Props) {
				return true
			}
			if q.Where != nil {
				bindings := map[string]any{aVar: a, bVar: bNode}
				ok, _ := evalBool(q.Where, bindings)
				if !ok {
					return true
				}
			}
			rows = append(rows, resultRow{a: a, b: bNode})
			if limit > 0 && !hasOrderBy && len(rows) >= limit {
				return false
			}
			if db.governor.checkRowCount(len(rows)) != nil {
				return false
			}
			return true
		})
		if err != nil {
			return nil, err
		}
		if db.governor.checkRowCount(len(rows)) != nil {
			return nil, ErrResultTooLarge
		}
	}
	return db.projectPatternResults(q, rows, aVar, "", bVar)
}

func (db *DB) findCandidates(np NodePattern) ([]*Node, error) {
	if len(np.Labels) > 0 {
		candidates, err := db.FindByLabel(np.Labels[0])
		if err != nil {
			return nil, err
		}
		if len(np.Labels) > 1 || len(np.Props) > 0 {
			var filtered []*Node
			for _, n := range candidates {
				if matchLabels(n.Labels, np.Labels) && matchProps(n.Props, np.Props) {
					filtered = append(filtered, n)
				}
			}
			return filtered, nil
		}
		return candidates, nil
	}
	if len(np.Props) == 0 {
		var nodes []*Node
		err := db.forEachNode(func(n *Node) error {
			nodes = append(nodes, n)
			return nil
		})
		return nodes, err
	}
	for key, val := range np.Props {
		if db.HasIndex(key) {
			candidates, err := db.FindByProperty(key, val)
			if err != nil {
				return nil, err
			}
			if len(np.Props) > 1 {
				var filtered []*Node
				for _, n := range candidates {
					if matchProps(n.Props, np.Props) {
						filtered = append(filtered, n)
					}
				}
				return filtered, nil
			}
			return candidates, nil
		}
	}
	return db.FindNodes(func(n *Node) bool {
		return matchProps(n.Props, np.Props)
	})
}

func matchLabels(nodeLabels, required []string) bool {
	if len(required) == 0 {
		return true
	}
	set := make(map[string]bool, len(nodeLabels))
	for _, l := range nodeLabels {
		set[l] = true
	}
	for _, r := range required {
		if !set[r] {
			return false
		}
	}
	return true
}

func (db *DB) projectResults(q *CypherQuery, nodes []*Node, edges []*Edge, nodeVar, edgeVar string) (*CypherResult, error) {
	result := &CypherResult{}
	for _, item := range q.Return.Items {
		result.Columns = append(result.Columns, returnItemName(item))
	}
	if len(q.OrderBy) > 0 && q.Limit > 0 {
		h := newTopKHeap(q.OrderBy, q.Limit)
		for _, n := range nodes {
			bindings := map[string]any{nodeVar: n}
			sortKey := evalSortKey(q.OrderBy, bindings)
			h.offer(topKItem{sortKey: sortKey, source: n})
		}
		sorted := h.sorted()
		for _, item := range sorted {
			n := item.source.(*Node)
			bindings := map[string]any{nodeVar: n}
			row := make(map[string]any)
			for _, ri := range q.Return.Items {
				colName := returnItemName(ri)
				val, err := evalExpr(&ri.Expr, bindings)
				if err != nil {
					return nil, err
				}
				row[colName] = val
			}
			result.Rows = append(result.Rows, row)
		}
		return result, nil
	}
	for _, n := range nodes {
		bindings := map[string]any{nodeVar: n}
		row := make(map[string]any)
		for _, item := range q.Return.Items {
			colName := returnItemName(item)
			val, err := evalExpr(&item.Expr, bindings)
			if err != nil {
				return nil, err
			}
			row[colName] = val
		}
		result.Rows = append(result.Rows, row)
	}
	if len(q.OrderBy) > 0 {
		sortRows(result.Rows, q.OrderBy)
	}
	if q.Limit > 0 && len(result.Rows) > q.Limit {
		result.Rows = result.Rows[:q.Limit]
	}
	return result, nil
}

type resultRow struct {
	a *Node
	r *Edge
	b *Node
}

func (db *DB) projectPatternResults(q *CypherQuery, rows []resultRow, aVar, rVar, bVar string) (*CypherResult, error) {
	result := &CypherResult{}
	for _, item := range q.Return.Items {
		result.Columns = append(result.Columns, returnItemName(item))
	}
	if len(q.OrderBy) > 0 && q.Limit > 0 {
		h := newTopKHeap(q.OrderBy, q.Limit)
		for _, rr := range rows {
			bindings := map[string]any{aVar: rr.a, bVar: rr.b}
			if rVar != "" && rr.r != nil {
				bindings[rVar] = rr.r
			}
			sortKey := evalSortKey(q.OrderBy, bindings)
			h.offer(topKItem{sortKey: sortKey, source: rr})
		}
		sorted := h.sorted()
		for _, item := range sorted {
			rr := item.source.(resultRow)
			bindings := map[string]any{aVar: rr.a, bVar: rr.b}
			if rVar != "" && rr.r != nil {
				bindings[rVar] = rr.r
			}
			row := make(map[string]any)
			for _, ri := range q.Return.Items {
				colName := returnItemName(ri)
				val, err := evalExpr(&ri.Expr, bindings)
				if err != nil {
					return nil, err
				}
				row[colName] = val
			}
			result.Rows = append(result.Rows, row)
		}
		return result, nil
	}
	for _, rr := range rows {
		bindings := map[string]any{aVar: rr.a, bVar: rr.b}
		if rVar != "" && rr.r != nil {
			bindings[rVar] = rr.r
		}
		row := make(map[string]any)
		for _, item := range q.Return.Items {
			colName := returnItemName(item)
			val, err := evalExpr(&item.Expr, bindings)
			if err != nil {
				return nil, err
			}
			row[colName] = val
		}
		result.Rows = append(result.Rows, row)
	}
	if len(q.OrderBy) > 0 {
		sortRows(result.Rows, q.OrderBy)
	}
	if q.Limit > 0 && len(result.Rows) > q.Limit {
		result.Rows = result.Rows[:q.Limit]
	}
	return result, nil
}

type topKItem struct {
	sortKey []any
	source  any
}

type topKHeap struct {
	items   []topKItem
	orderBy []OrderItem
	limit   int
}

func newTopKHeap(orderBy []OrderItem, limit int) *topKHeap {
	return &topKHeap{items: make([]topKItem, 0, limit), orderBy: orderBy, limit: limit}
}

func (h *topKHeap) Len() int { return len(h.items) }

func (h *topKHeap) Less(i, j int) bool {
	for idx, oi := range h.orderBy {
		cmp := compareValues(h.items[i].sortKey[idx], h.items[j].sortKey[idx])
		if cmp == 0 {
			continue
		}
		if oi.Desc {
			return cmp < 0
		}
		return cmp > 0
	}
	return false
}

func (h *topKHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *topKHeap) Push(x any)    { h.items = append(h.items, x.(topKItem)) }

func (h *topKHeap) Pop() any {
	n := len(h.items)
	item := h.items[n-1]
	h.items = h.items[:n-1]
	return item
}

func (h *topKHeap) offer(item topKItem) {
	if len(h.items) < h.limit {
		heap.Push(h, item)
		return
	}
	if h.isBetter(item.sortKey, h.items[0].sortKey) {
		h.items[0] = item
		heap.Fix(h, 0)
	}
}

func (h *topKHeap) isBetter(a, b []any) bool {
	for idx, oi := range h.orderBy {
		cmp := compareValues(a[idx], b[idx])
		if cmp == 0 {
			continue
		}
		if oi.Desc {
			return cmp > 0
		}
		return cmp < 0
	}
	return false
}

func (h *topKHeap) sorted() []topKItem {
	n := len(h.items)
	result := make([]topKItem, n)
	for i := n - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).(topKItem)
	}
	return result
}

func evalSortKey(orderBy []OrderItem, bindings map[string]any) []any {
	key := make([]any, len(orderBy))
	for i, oi := range orderBy {
		val, _ := evalExpr(&oi.Expr, bindings)
		key[i] = val
	}
	return key
}

func returnItemName(item ReturnItem) string {
	if item.Alias != "" {
		return item.Alias
	}
	return exprName(item.Expr)
}

func exprName(e Expression) string {
	switch e.Kind {
	case ExprVarRef:
		return e.Variable
	case ExprPropAccess:
		return e.Object + "." + e.Property
	case ExprFuncCall:
		args := make([]string, len(e.Args))
		for i, a := range e.Args {
			args[i] = exprName(a)
		}
		return e.FuncName + "(" + strings.Join(args, ", ") + ")"
	default:
		return "expr"
	}
}

func evalExpr(e *Expression, bindings map[string]any) (any, error) {
	switch e.Kind {
	case ExprLiteral:
		return e.LitValue, nil
	case ExprVarRef:
		val, ok := bindings[e.Variable]
		if !ok {
			return nil, fmt.Errorf("cypher exec: unbound variable %q", e.Variable)
		}
		return val, nil
	case ExprPropAccess:
		obj, ok := bindings[e.Object]
		if !ok {
			return nil, fmt.Errorf("cypher exec: unbound variable %q", e.Object)
		}
		if obj == nil {
			return nil, nil
		}
		return getProperty(obj, e.Property), nil
	case ExprFuncCall:
		return evalFunc(e.FuncName, e.Args, bindings)
	case ExprComparison:
		return evalComparison(e, bindings)
	case ExprAnd:
		for _, op := range e.Operands {
			v, err := evalBool(&op, bindings)
			if err != nil {
				return nil, err
			}
			if !v {
				return false, nil
			}
		}
		return true, nil
	case ExprOr:
		for _, op := range e.Operands {
			v, err := evalBool(&op, bindings)
			if err != nil {
				return nil, err
			}
			if v {
				return true, nil
			}
		}
		return false, nil
	case ExprNot:
		v, err := evalBool(e.Inner, bindings)
		if err != nil {
			return nil, err
		}
		return !v, nil
	default:
		return nil, fmt.Errorf("cypher exec: unsupported expression kind %d", e.Kind)
	}
}

func evalBool(e *Expression, bindings map[string]any) (bool, error) {
	val, err := evalExpr(e, bindings)
	if err != nil {
		return false, err
	}
	return toBool(val), nil
}

func evalComparison(e *Expression, bindings map[string]any) (any, error) {
	left, err := evalExpr(e.Left, bindings)
	if err != nil {
		return nil, err
	}
	right, err := evalExpr(e.Right, bindings)
	if err != nil {
		return nil, err
	}
	cmp := compareValues(left, right)
	switch e.Op {
	case OpEq:
		return cmp == 0, nil
	case OpNeq:
		return cmp != 0, nil
	case OpLt:
		return cmp < 0, nil
	case OpGt:
		return cmp > 0, nil
	case OpLte:
		return cmp <= 0, nil
	case OpGte:
		return cmp >= 0, nil
	default:
		return false, nil
	}
}

func evalFunc(name string, args []Expression, bindings map[string]any) (any, error) {
	switch strings.ToLower(name) {
	case "type":
		if len(args) != 1 {
			return nil, fmt.Errorf("cypher exec: type() requires exactly 1 argument")
		}
		val, err := evalExpr(&args[0], bindings)
		if err != nil {
			return nil, err
		}
		if e, ok := val.(*Edge); ok {
			return e.Label, nil
		}
		return nil, nil
	case "id":
		if len(args) != 1 {
			return nil, fmt.Errorf("cypher exec: id() requires exactly 1 argument")
		}
		val, err := evalExpr(&args[0], bindings)
		if err != nil {
			return nil, err
		}
		switch v := val.(type) {
		case *Node:
			return int64(v.ID), nil
		case *Edge:
			return int64(v.ID), nil
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("cypher exec: unknown function %q", name)
	}
}

func getProperty(obj any, prop string) any {
	switch v := obj.(type) {
	case *Node:
		if v.Props != nil {
			return v.Props[prop]
		}
	case *Edge:
		switch prop {
		case "label", "type":
			return v.Label
		default:
			if v.Props != nil {
				return v.Props[prop]
			}
		}
	}
	return nil
}

func matchProps(actual Props, constraints map[string]any) bool {
	if constraints == nil {
		return true
	}
	for key, expected := range constraints {
		actual, ok := actual[key]
		if !ok {
			return false
		}
		if compareValues(actual, expected) != 0 {
			return false
		}
	}
	return true
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case uint64:
		return float64(n), true
	}
	return 0, false
}

func compareValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	af, aOk := toFloat64(a)
	bf, bOk := toFloat64(b)
	if aOk && bOk {
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}
	as, aStr := a.(string)
	bs, bStr := b.(string)
	if aStr && bStr {
		return strings.Compare(as, bs)
	}
	ab, aBool := a.(bool)
	bb, bBool := b.(bool)
	if aBool && bBool {
		if ab == bb {
			return 0
		}
		if !ab {
			return -1
		}
		return 1
	}
	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
}

func toBool(v any) bool {
	if v == nil {
		return false
	}
	switch b := v.(type) {
	case bool:
		return b
	case int64:
		return b != 0
	case float64:
		return b != 0
	case string:
		return b != ""
	}
	return true
}

func sortRows(rows []map[string]any, orderItems []OrderItem) {
	sort.SliceStable(rows, func(i, j int) bool {
		for _, oi := range orderItems {
			vi := evalRowExpr(&oi.Expr, rows[i])
			vj := evalRowExpr(&oi.Expr, rows[j])
			cmp := compareValues(vi, vj)
			if cmp == 0 {
				continue
			}
			if oi.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
}

func evalRowExpr(e *Expression, row map[string]any) any {
	name := exprName(*e)
	if v, ok := row[name]; ok {
		return v
	}
	if e.Kind == ExprPropAccess {
		if obj, ok := row[e.Object]; ok {
			return getProperty(obj, e.Property)
		}
	}
	return nil
}
