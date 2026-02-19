//go:build js && wasm

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"syscall/js"
	"time"

	graphdb "github.com/mstrYoda/goraphdb"
)

var db *graphdb.DB

func main() {
	js.Global().Set("_graphdb", js.ValueOf(map[string]any{
		// Lifecycle
		"open":  safeCall(jsOpen),
		"close": safeCall(jsClose),

		// Stats
		"stats": safeCall(jsStats),

		// Cypher
		"cypher": safeCall(jsCypher),

		// Nodes
		"addNode":         safeCall(jsAddNode),
		"getNode":         safeCall(jsGetNode),
		"updateNode":      safeCall(jsUpdateNode),
		"deleteNode":      safeCall(jsDeleteNode),
		"listNodesCursor": safeCall(jsListNodesCursor),
		"listNodes":       safeCall(jsListNodes),
		"getNeighborhood": safeCall(jsGetNeighborhood),

		// Edges
		"addEdge":         safeCall(jsAddEdge),
		"getEdge":         safeCall(jsGetEdge),
		"deleteEdge":      safeCall(jsDeleteEdge),
		"listEdgesCursor": safeCall(jsListEdgesCursor),

		// Labels
		"addLabel":  safeCall(jsAddLabel),
		"getLabels": safeCall(jsGetLabels),

		// Indexes
		"listIndexes": safeCall(jsListIndexes),
		"createIndex": safeCall(jsCreateIndex),
		"dropIndex":   safeCall(jsDropIndex),
		"reIndex":     safeCall(jsReIndex),

		// Metrics & observability
		"getMetrics":     safeCall(jsGetMetrics),
		"getSlowQueries": safeCall(jsGetSlowQueries),

		// Cluster (stub for standalone WASM)
		"getClusterStatus": safeCall(jsGetClusterStatus),
		"getClusterNodes":  safeCall(jsGetClusterNodes),

		// Demo data
		"seedDemo": safeCall(jsSeedDemo),
	}))

	// Signal that WASM is ready.
	if cb := js.Global().Get("_graphdbReady"); cb.Truthy() {
		cb.Invoke()
	}

	// Block forever — WASM modules must not exit.
	select {}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func jsOpen(_ js.Value, args []js.Value) any {
	shardCount := 1
	if len(args) > 0 && args[0].Type() == js.TypeNumber {
		shardCount = args[0].Int()
	}
	var err error
	db, err = graphdb.Open("mem", graphdb.Options{ShardCount: shardCount})
	if err != nil {
		return jsErr(err)
	}
	return jsOK(nil)
}

func jsClose(_ js.Value, _ []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	err := db.Close()
	db = nil
	if err != nil {
		return jsErr(err)
	}
	return jsOK(nil)
}

// ---------------------------------------------------------------------------
// Stats  — mirrors GET /api/stats
// ---------------------------------------------------------------------------

func jsStats(_ js.Value, _ []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	stats, err := db.Stats()
	if err != nil {
		return jsErr(err)
	}
	return jsOK(map[string]any{
		"node_count":      stats.NodeCount,
		"edge_count":      stats.EdgeCount,
		"shard_count":     stats.ShardCount,
		"disk_size_bytes": stats.DiskSizeBytes,
	})
}

// ---------------------------------------------------------------------------
// Cypher — mirrors POST /api/cypher
// ---------------------------------------------------------------------------

func jsCypher(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	if len(args) < 1 {
		return jsErr(fmt.Errorf("missing query string"))
	}
	query := args[0].String()

	start := time.Now()
	result, err := db.Cypher(context.Background(), query)
	elapsed := time.Since(start)
	if err != nil {
		return jsErr(err)
	}

	rows := make([]any, len(result.Rows))
	graphNodes := map[uint64]bool{}
	var gNodes []any
	var gEdges []any

	for i, row := range result.Rows {
		safeRow := make(map[string]any)
		for k, v := range row {
			safeRow[k] = toJSSafe(v)
			// Extract graph nodes for visualization.
			if n, ok := v.(*graphdb.Node); ok && !graphNodes[uint64(n.ID)] {
				graphNodes[uint64(n.ID)] = true
				gNodes = append(gNodes, map[string]any{
					"id":    uint64(n.ID),
					"props": safeProps(n.Props),
					"label": nodeLabel(n.Props),
				})
			}
		}
		rows[i] = safeRow
	}
	if rows == nil {
		rows = []any{}
	}
	if gNodes == nil {
		gNodes = []any{}
	}

	// Discover edges between graph nodes.
	for nid := range graphNodes {
		outEdges, _ := db.OutEdges(graphdb.NodeID(nid))
		for _, e := range outEdges {
			if graphNodes[uint64(e.To)] {
				gEdges = append(gEdges, map[string]any{
					"id":    uint64(e.ID),
					"from":  uint64(e.From),
					"to":    uint64(e.To),
					"label": e.Label,
				})
			}
		}
	}
	if gEdges == nil {
		gEdges = []any{}
	}

	return jsOK(map[string]any{
		"columns":    result.Columns,
		"rows":       rows,
		"rowCount":   len(result.Rows),
		"execTimeMs": float64(elapsed.Microseconds()) / 1000.0,
		"graph": map[string]any{
			"nodes": gNodes,
			"edges": gEdges,
		},
	})
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

func jsAddNode(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	props, err := parseJSJSON(args, 0)
	if err != nil {
		return jsErr(err)
	}
	id, err := db.AddNode(props)
	if err != nil {
		return jsErr(err)
	}
	return jsOK(map[string]any{"id": uint64(id)})
}

func jsGetNode(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	if len(args) < 1 {
		return jsErr(fmt.Errorf("missing node ID"))
	}
	id := graphdb.NodeID(args[0].Int())
	node, err := db.GetNode(id)
	if err != nil {
		return jsErr(err)
	}
	return jsOK(map[string]any{
		"id":    uint64(node.ID),
		"props": safeProps(node.Props),
	})
}

func jsUpdateNode(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	if len(args) < 2 {
		return jsErr(fmt.Errorf("missing arguments: id, props"))
	}
	id := graphdb.NodeID(args[0].Int())
	props, err := parseJSJSON(args, 1)
	if err != nil {
		return jsErr(err)
	}
	if err := db.UpdateNode(id, props); err != nil {
		return jsErr(err)
	}
	return jsOK(map[string]any{"status": "updated"})
}

func jsDeleteNode(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	if len(args) < 1 {
		return jsErr(fmt.Errorf("missing node ID"))
	}
	id := graphdb.NodeID(args[0].Int())
	if err := db.DeleteNode(id); err != nil {
		return jsErr(err)
	}
	return jsOK(map[string]any{"status": "deleted"})
}

func jsListNodes(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	limit := 50
	offset := 0
	if len(args) > 0 && args[0].Type() == js.TypeNumber {
		limit = args[0].Int()
	}
	if len(args) > 1 && args[1].Type() == js.TypeNumber {
		offset = args[1].Int()
	}

	var nodes []any
	idx := 0
	_ = db.ForEachNode(func(n *graphdb.Node) error {
		if len(nodes) >= limit {
			return fmt.Errorf("stop")
		}
		if idx >= offset {
			nodes = append(nodes, map[string]any{
				"id":    uint64(n.ID),
				"props": safeProps(n.Props),
			})
		}
		idx++
		return nil
	})
	if nodes == nil {
		nodes = []any{}
	}
	return jsOK(map[string]any{
		"nodes":  nodes,
		"total":  db.NodeCount(),
		"limit":  limit,
		"offset": offset,
	})
}

func jsListNodesCursor(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	cursor := uint64(0)
	limit := 30
	if len(args) > 0 && args[0].Type() == js.TypeNumber {
		cursor = uint64(args[0].Int())
	}
	if len(args) > 1 && args[1].Type() == js.TypeNumber {
		limit = args[1].Int()
	}

	page, err := db.ListNodes(graphdb.NodeID(cursor), limit)
	if err != nil {
		return jsErr(err)
	}

	nodes := make([]any, len(page.Nodes))
	for i, n := range page.Nodes {
		labels, _ := db.GetLabels(n.ID)
		nodes[i] = map[string]any{
			"id":     uint64(n.ID),
			"labels": safeStringSlice(labels),
			"props":  safeProps(n.Props),
		}
	}

	return jsOK(map[string]any{
		"nodes":       nodes,
		"next_cursor": page.NextCursor,
		"has_more":    page.HasMore,
		"limit":       limit,
	})
}

func jsGetNeighborhood(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	if len(args) < 1 {
		return jsErr(fmt.Errorf("missing node ID"))
	}
	id := graphdb.NodeID(args[0].Int())

	center, err := db.GetNode(id)
	if err != nil {
		return jsErr(err)
	}

	allEdges, err := db.Edges(id)
	if err != nil {
		return jsErr(err)
	}

	edges := make([]any, 0, len(allEdges))
	neighborIDs := map[uint64]bool{}
	for _, e := range allEdges {
		edges = append(edges, map[string]any{
			"id":    uint64(e.ID),
			"from":  uint64(e.From),
			"to":    uint64(e.To),
			"label": e.Label,
		})
		if uint64(e.From) != uint64(id) {
			neighborIDs[uint64(e.From)] = true
		}
		if uint64(e.To) != uint64(id) {
			neighborIDs[uint64(e.To)] = true
		}
	}

	neighbors := make([]any, 0, len(neighborIDs))
	for nid := range neighborIDs {
		n, nerr := db.GetNode(graphdb.NodeID(nid))
		if nerr != nil {
			neighbors = append(neighbors, map[string]any{
				"id":    nid,
				"props": map[string]any{},
				"label": fmt.Sprintf("Node %d", nid),
			})
			continue
		}
		neighbors = append(neighbors, map[string]any{
			"id":    uint64(n.ID),
			"props": safeProps(n.Props),
			"label": nodeLabel(n.Props),
		})
	}

	return jsOK(map[string]any{
		"center": map[string]any{
			"id":    uint64(center.ID),
			"props": safeProps(center.Props),
			"label": nodeLabel(center.Props),
		},
		"neighbors": neighbors,
		"edges":     edges,
	})
}

// ---------------------------------------------------------------------------
// Edges
// ---------------------------------------------------------------------------

func jsAddEdge(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	if len(args) < 3 {
		return jsErr(fmt.Errorf("missing arguments: fromID, toID, label"))
	}
	from := graphdb.NodeID(args[0].Int())
	to := graphdb.NodeID(args[1].Int())
	label := args[2].String()
	var props graphdb.Props
	if len(args) > 3 && args[3].Type() == js.TypeString {
		var err error
		props, err = parseJSJSON(args, 3)
		if err != nil {
			return jsErr(err)
		}
	}
	id, err := db.AddEdge(from, to, label, props)
	if err != nil {
		return jsErr(err)
	}
	return jsOK(map[string]any{"id": uint64(id)})
}

func jsGetEdge(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	if len(args) < 1 {
		return jsErr(fmt.Errorf("missing edge ID"))
	}
	id := graphdb.EdgeID(args[0].Int())
	edge, err := db.GetEdge(id)
	if err != nil {
		return jsErr(err)
	}
	return jsOK(map[string]any{
		"id":    uint64(edge.ID),
		"from":  uint64(edge.From),
		"to":    uint64(edge.To),
		"label": edge.Label,
		"props": safeProps(edge.Props),
	})
}

func jsDeleteEdge(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	if len(args) < 1 {
		return jsErr(fmt.Errorf("missing edge ID"))
	}
	id := graphdb.EdgeID(args[0].Int())
	if err := db.DeleteEdge(id); err != nil {
		return jsErr(err)
	}
	return jsOK(map[string]any{"status": "deleted"})
}

func jsListEdgesCursor(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	cursor := uint64(0)
	limit := 30
	if len(args) > 0 && args[0].Type() == js.TypeNumber {
		cursor = uint64(args[0].Int())
	}
	if len(args) > 1 && args[1].Type() == js.TypeNumber {
		limit = args[1].Int()
	}

	page, err := db.ListEdges(graphdb.EdgeID(cursor), limit)
	if err != nil {
		return jsErr(err)
	}

	edges := make([]any, len(page.Edges))
	for i, e := range page.Edges {
		edges[i] = map[string]any{
			"id":    uint64(e.ID),
			"from":  uint64(e.From),
			"to":    uint64(e.To),
			"label": e.Label,
			"props": safeProps(e.Props),
		}
	}

	return jsOK(map[string]any{
		"edges":       edges,
		"next_cursor": page.NextCursor,
		"has_more":    page.HasMore,
		"limit":       limit,
	})
}

// ---------------------------------------------------------------------------
// Labels
// ---------------------------------------------------------------------------

func jsAddLabel(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	if len(args) < 2 {
		return jsErr(fmt.Errorf("missing arguments: nodeID, labels..."))
	}
	id := graphdb.NodeID(args[0].Int())
	labels := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		labels[i-1] = args[i].String()
	}
	if err := db.AddLabel(id, labels...); err != nil {
		return jsErr(err)
	}
	return jsOK(nil)
}

func jsGetLabels(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	if len(args) < 1 {
		return jsErr(fmt.Errorf("missing node ID"))
	}
	id := graphdb.NodeID(args[0].Int())
	labels, err := db.GetLabels(id)
	if err != nil {
		return jsErr(err)
	}
	return jsOK(map[string]any{"labels": safeStringSlice(labels)})
}

// ---------------------------------------------------------------------------
// Indexes — mirrors GET/POST/DELETE /api/indexes
// ---------------------------------------------------------------------------

func jsListIndexes(_ js.Value, _ []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	indexes := db.ListIndexes()
	return jsOK(map[string]any{"indexes": safeStringSlice(indexes)})
}

func jsCreateIndex(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	if len(args) < 1 {
		return jsErr(fmt.Errorf("missing property name"))
	}
	prop := args[0].String()
	if err := db.CreateIndex(prop); err != nil {
		return jsErr(err)
	}
	return jsOK(map[string]any{"status": "created", "property": prop})
}

func jsDropIndex(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	if len(args) < 1 {
		return jsErr(fmt.Errorf("missing property name"))
	}
	prop := args[0].String()
	if err := db.DropIndex(prop); err != nil {
		return jsErr(err)
	}
	return jsOK(map[string]any{"status": "dropped", "property": prop})
}

func jsReIndex(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	if len(args) < 1 {
		return jsErr(fmt.Errorf("missing property name"))
	}
	prop := args[0].String()
	if err := db.ReIndex(prop); err != nil {
		return jsErr(err)
	}
	return jsOK(map[string]any{"status": "reindexed", "property": prop})
}

// ---------------------------------------------------------------------------
// Metrics — mirrors GET /api/metrics
// ---------------------------------------------------------------------------

func jsGetMetrics(_ js.Value, _ []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	m := db.Metrics()
	if m == nil {
		return jsOK(map[string]any{})
	}
	return jsOK(m.Snapshot())
}

// ---------------------------------------------------------------------------
// Slow Queries — mirrors GET /api/slow-queries
// ---------------------------------------------------------------------------

func jsGetSlowQueries(_ js.Value, args []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}
	limit := 50
	if len(args) > 0 && args[0].Type() == js.TypeNumber {
		limit = args[0].Int()
	}
	entries := db.SlowQueries(limit)
	if entries == nil {
		return jsOK(map[string]any{"queries": []any{}, "count": 0})
	}
	qs := make([]any, len(entries))
	for i, e := range entries {
		qs[i] = map[string]any{
			"query":       e.Query,
			"duration_ms": e.DurationMs,
			"rows":        e.Rows,
			"timestamp":   e.Timestamp,
		}
	}
	return jsOK(map[string]any{"queries": qs, "count": len(entries)})
}

// ---------------------------------------------------------------------------
// Cluster stubs — WASM is always standalone
// ---------------------------------------------------------------------------

func jsGetClusterStatus(_ js.Value, _ []js.Value) any {
	return jsOK(map[string]any{
		"mode": "standalone",
		"role": "standalone",
	})
}

func jsGetClusterNodes(_ js.Value, _ []js.Value) any {
	var statsData map[string]any
	if db != nil {
		if s, err := db.Stats(); err == nil {
			statsData = map[string]any{
				"node_count":      s.NodeCount,
				"edge_count":      s.EdgeCount,
				"shard_count":     s.ShardCount,
				"disk_size_bytes": s.DiskSizeBytes,
			}
		}
	}
	node := map[string]any{
		"node_id":   "wasm-browser",
		"role":      "standalone",
		"status":    "ok",
		"readable":  true,
		"writable":  true,
		"stats":     statsData,
		"reachable": true,
	}
	return jsOK(map[string]any{
		"mode":      "standalone",
		"self":      "wasm-browser",
		"leader_id": "",
		"nodes":     []any{node},
	})
}

// ---------------------------------------------------------------------------
// Demo Data Seeding
// ---------------------------------------------------------------------------

func jsSeedDemo(_ js.Value, _ []js.Value) any {
	if db == nil {
		return jsErr(fmt.Errorf("database not open"))
	}

	// Create Person nodes.
	people := []struct {
		name string
		age  int
		city string
	}{
		{"Alice", 30, "San Francisco"},
		{"Bob", 28, "New York"},
		{"Charlie", 35, "London"},
		{"Diana", 27, "Berlin"},
		{"Eve", 32, "Tokyo"},
		{"Frank", 40, "Paris"},
		{"Grace", 25, "Sydney"},
		{"Hank", 33, "Toronto"},
	}

	nodeIDs := make([]graphdb.NodeID, len(people))
	for i, p := range people {
		id, err := db.AddNode(graphdb.Props{
			"name": p.name,
			"age":  p.age,
			"city": p.city,
		})
		if err != nil {
			return jsErr(fmt.Errorf("seed: add node %s: %w", p.name, err))
		}
		nodeIDs[i] = id
		_ = db.AddLabel(id, "Person")
	}

	// Create Company nodes.
	companies := []struct {
		name     string
		industry string
	}{
		{"Acme Corp", "Technology"},
		{"Globex", "Finance"},
		{"Initech", "Software"},
	}

	companyIDs := make([]graphdb.NodeID, len(companies))
	for i, c := range companies {
		id, err := db.AddNode(graphdb.Props{
			"name":     c.name,
			"industry": c.industry,
		})
		if err != nil {
			return jsErr(fmt.Errorf("seed: add company %s: %w", c.name, err))
		}
		companyIDs[i] = id
		_ = db.AddLabel(id, "Company")
	}

	// KNOWS edges (social network).
	knows := [][2]int{
		{0, 1}, {0, 2}, {1, 3}, {2, 4}, {3, 5}, {4, 6}, {5, 7}, {6, 0}, {1, 4}, {3, 7},
	}
	for _, k := range knows {
		_, err := db.AddEdge(nodeIDs[k[0]], nodeIDs[k[1]], "KNOWS", graphdb.Props{
			"since": 2020 + k[0]%4,
		})
		if err != nil {
			return jsErr(fmt.Errorf("seed: add KNOWS edge: %w", err))
		}
	}

	// WORKS_AT edges.
	worksAt := [][2]int{
		{0, 0}, {1, 0}, {2, 1}, {3, 1}, {4, 2}, {5, 2}, {6, 0}, {7, 1},
	}
	for _, w := range worksAt {
		_, err := db.AddEdge(nodeIDs[w[0]], companyIDs[w[1]], "WORKS_AT", graphdb.Props{
			"role": "Engineer",
		})
		if err != nil {
			return jsErr(fmt.Errorf("seed: add WORKS_AT edge: %w", err))
		}
	}

	// Create indexes on common properties.
	_ = db.CreateIndex("name")
	_ = db.CreateIndex("city")

	return jsOK(map[string]any{
		"status": "seeded",
		"nodes":  len(people) + len(companies),
		"edges":  len(knows) + len(worksAt),
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// safeCall wraps a WASM callback with panic recovery so a single failure
// cannot crash the entire Go runtime (which would make every subsequent
// JS call fail with "Go program has already exited").
func safeCall(fn func(js.Value, []js.Value) any) js.Func {
	return js.FuncOf(func(this js.Value, args []js.Value) (result any) {
		defer func() {
			if r := recover(); r != nil {
				result = map[string]any{"error": fmt.Sprintf("panic: %v", r)}
			}
		}()
		return fn(this, args)
	})
}

func parseJSJSON(args []js.Value, idx int) (map[string]any, error) {
	if idx >= len(args) {
		return nil, nil
	}
	jsonStr := args[idx].String()
	if jsonStr == "" || jsonStr == "undefined" || jsonStr == "null" {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &m); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	return m, nil
}

func nodeLabel(props map[string]any) string {
	for _, key := range []string{"name", "title", "label", "username"} {
		if v, ok := props[key]; ok {
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}

func safeProps(p map[string]any) map[string]any {
	if p == nil {
		return map[string]any{}
	}
	return p
}

func safeStringSlice(ss []string) []any {
	if ss == nil {
		return []any{}
	}
	result := make([]any, len(ss))
	for i, s := range ss {
		result[i] = s
	}
	return result
}

// jsOK converts a result map so every value is safe for js.ValueOf().
// js.ValueOf panics on uint64, int64, time.Time, etc. — this converts
// all values recursively to float64/string/[]any/map[string]any.
func jsOK(data map[string]any) any {
	if data == nil {
		return map[string]any{}
	}
	return sanitizeForJS(data)
}

func jsErr(err error) any {
	return map[string]any{"error": err.Error()}
}

// sanitizeForJS recursively converts Go values into types that js.ValueOf
// accepts: bool, int, float64, string, []any, map[string]any, nil.
func sanitizeForJS(v any) any {
	switch val := v.(type) {
	case nil:
		return nil
	case bool:
		return val
	case int:
		return val
	case int8:
		return int(val)
	case int16:
		return int(val)
	case int32:
		return int(val)
	case int64:
		return float64(val)
	case uint:
		return float64(val)
	case uint8:
		return int(val)
	case uint16:
		return int(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	case string:
		return val
	case time.Time:
		return val.Format(time.RFC3339)
	case time.Duration:
		return float64(val.Milliseconds())
	case []string:
		return safeStringSlice(val)
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = sanitizeForJS(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(val))
		for k, item := range val {
			out[k] = sanitizeForJS(item)
		}
		return out
	case *graphdb.Node:
		return map[string]any{
			"id":     float64(val.ID),
			"labels": safeStringSlice(val.Labels),
			"props":  sanitizeForJS(safeProps(val.Props)),
		}
	case *graphdb.Edge:
		return map[string]any{
			"id":    float64(val.ID),
			"from":  float64(val.From),
			"to":    float64(val.To),
			"label": val.Label,
			"props": sanitizeForJS(safeProps(val.Props)),
		}
	case graphdb.NodeID:
		return float64(val)
	case graphdb.EdgeID:
		return float64(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func toJSSafe(v any) any {
	return sanitizeForJS(v)
}
