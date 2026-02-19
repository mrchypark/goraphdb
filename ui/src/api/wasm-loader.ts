// ---------------------------------------------------------------------------
// WASM Loader — loads and initializes the Go WASM graphdb module.
//
// After loading, the global `_graphdb` object is available with all
// database operations exposed by cmd/graphdb-wasm/main.go.
// ---------------------------------------------------------------------------

declare global {
  // eslint-disable-next-line no-var
  var Go: any
  // eslint-disable-next-line no-var
  var _graphdb: GraphDBWasm | undefined
  // eslint-disable-next-line no-var
  var _graphdbReady: (() => void) | undefined
}

export interface GraphDBWasm {
  open(shardCount?: number): WasmResult
  close(): WasmResult
  stats(): WasmResult
  cypher(query: string): WasmResult

  addNode(propsJSON: string): WasmResult
  getNode(id: number): WasmResult
  updateNode(id: number, propsJSON: string): WasmResult
  deleteNode(id: number): WasmResult
  listNodes(limit?: number, offset?: number): WasmResult
  listNodesCursor(cursor?: number, limit?: number): WasmResult
  getNeighborhood(id: number): WasmResult

  addEdge(from: number, to: number, label: string, propsJSON?: string): WasmResult
  getEdge(id: number): WasmResult
  deleteEdge(id: number): WasmResult
  listEdgesCursor(cursor?: number, limit?: number): WasmResult

  addLabel(nodeID: number, ...labels: string[]): WasmResult
  getLabels(nodeID: number): WasmResult

  listIndexes(): WasmResult
  createIndex(property: string): WasmResult
  dropIndex(property: string): WasmResult
  reIndex(property: string): WasmResult

  getMetrics(): WasmResult
  getSlowQueries(limit?: number): WasmResult

  getClusterStatus(): WasmResult
  getClusterNodes(): WasmResult

  seedDemo(): WasmResult
}

export type WasmResult = Record<string, any>

let _loaded = false
let _loading: Promise<void> | null = null

/**
 * Load and initialize the WASM graphdb module.
 * Safe to call multiple times — subsequent calls are no-ops.
 */
export async function loadGraphDBWasm(): Promise<GraphDBWasm> {
  if (_loaded && globalThis._graphdb) {
    return globalThis._graphdb
  }
  if (_loading) {
    await _loading
    return globalThis._graphdb!
  }

  _loading = _doLoad()
  await _loading
  _loaded = true
  return globalThis._graphdb!
}

async function _doLoad(): Promise<void> {
  // 1. Load wasm_exec.js (Go's WASM support runtime).
  if (typeof globalThis.Go === 'undefined') {
    await new Promise<void>((resolve, reject) => {
      const script = document.createElement('script')
      script.src = '/wasm_exec.js'
      script.onload = () => resolve()
      script.onerror = () => reject(new Error('Failed to load wasm_exec.js'))
      document.head.appendChild(script)
    })
  }

  // 2. Set up readiness callback.
  const ready = new Promise<void>((resolve) => {
    globalThis._graphdbReady = resolve
  })

  // 3. Instantiate the Go WASM module.
  const go = new globalThis.Go()
  const result = await WebAssembly.instantiateStreaming(
    fetch('/graphdb.wasm'),
    go.importObject,
  )
  // go.run() is async and never resolves (the Go main blocks with select{}).
  go.run(result.instance)

  // 4. Wait for the Go code to signal readiness.
  await ready

  if (!globalThis._graphdb) {
    throw new Error('WASM module loaded but _graphdb not found on globalThis')
  }
}

/**
 * Check if WASM mode is available (module loaded and DB open).
 */
export function isWasmReady(): boolean {
  return _loaded && !!globalThis._graphdb
}
