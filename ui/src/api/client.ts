import type {
  GraphStats,
  CypherResponse,
  IndexListResponse,
  NodeListResponse,
  GNode,
  NeighborhoodResponse,
  MetricsSnapshot,
  SlowQueryResponse,
  NodeCursorPage,
  EdgeCursorPage,
  ClusterNodesResponse,
  ClusterStatusResponse,
} from '../types'
import { loadGraphDBWasm } from './wasm-loader'
import { createWasmApi } from './wasm-client'

// ---------------------------------------------------------------------------
// API type — shared interface between HTTP and WASM backends.
// ---------------------------------------------------------------------------

export interface Api {
  getStats(): Promise<GraphStats>
  executeCypher(query: string): Promise<CypherResponse>
  listIndexes(): Promise<IndexListResponse>
  createIndex(property: string): Promise<{ status: string }>
  dropIndex(name: string): Promise<{ status: string }>
  reIndex(name: string): Promise<{ status: string }>
  listNodes(limit?: number, offset?: number): Promise<NodeListResponse>
  getNode(id: number): Promise<GNode>
  getNeighborhood(id: number): Promise<NeighborhoodResponse>
  createNode(props: Record<string, any>): Promise<{ id: number }>
  deleteNode(id: number): Promise<{ status: string }>
  createEdge(from: number, to: number, label: string, props?: Record<string, any>): Promise<{ id: number }>
  deleteEdge(id: number): Promise<{ status: string }>
  getMetrics(): Promise<MetricsSnapshot>
  getSlowQueries(limit?: number): Promise<SlowQueryResponse>
  listNodesCursor(cursor?: number, limit?: number): Promise<NodeCursorPage>
  listEdgesCursor(cursor?: number, limit?: number): Promise<EdgeCursorPage>
  getClusterStatus(): Promise<ClusterStatusResponse>
  getClusterNodes(): Promise<ClusterNodesResponse>
}

// ---------------------------------------------------------------------------
// HTTP backend (original implementation)
// ---------------------------------------------------------------------------

const BASE = '/api'

async function fetchJSON<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(BASE + url, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.error || `HTTP ${res.status}`)
  }
  return res.json()
}

const httpApi: Api = {
  getStats: () => fetchJSON<GraphStats>('/stats'),
  executeCypher: (query: string) =>
    fetchJSON<CypherResponse>('/cypher', {
      method: 'POST',
      body: JSON.stringify({ query }),
    }),
  listIndexes: () => fetchJSON<IndexListResponse>('/indexes'),
  createIndex: (property: string) =>
    fetchJSON<{ status: string }>('/indexes', {
      method: 'POST',
      body: JSON.stringify({ property }),
    }),
  dropIndex: (name: string) =>
    fetchJSON<{ status: string }>(`/indexes/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    }),
  reIndex: (name: string) =>
    fetchJSON<{ status: string }>(`/indexes/${encodeURIComponent(name)}/reindex`, {
      method: 'POST',
    }),
  listNodes: (limit = 50, offset = 0) =>
    fetchJSON<NodeListResponse>(`/nodes?limit=${limit}&offset=${offset}`),
  getNode: (id: number) => fetchJSON<GNode>(`/nodes/${id}`),
  getNeighborhood: (id: number) =>
    fetchJSON<NeighborhoodResponse>(`/nodes/${id}/neighborhood`),
  createNode: (props: Record<string, any>) =>
    fetchJSON<{ id: number }>('/nodes', {
      method: 'POST',
      body: JSON.stringify({ props }),
    }),
  deleteNode: (id: number) =>
    fetchJSON<{ status: string }>(`/nodes/${id}`, { method: 'DELETE' }),
  createEdge: (from: number, to: number, label: string, props?: Record<string, any>) =>
    fetchJSON<{ id: number }>('/edges', {
      method: 'POST',
      body: JSON.stringify({ from, to, label, props }),
    }),
  deleteEdge: (id: number) =>
    fetchJSON<{ status: string }>(`/edges/${id}`, { method: 'DELETE' }),
  getMetrics: () => fetchJSON<MetricsSnapshot>('/metrics'),
  getSlowQueries: (limit = 50) =>
    fetchJSON<SlowQueryResponse>(`/slow-queries?limit=${limit}`),
  listNodesCursor: (cursor = 0, limit = 30) =>
    fetchJSON<NodeCursorPage>(`/nodes/cursor?cursor=${cursor}&limit=${limit}`),
  listEdgesCursor: (cursor = 0, limit = 30) =>
    fetchJSON<EdgeCursorPage>(`/edges/cursor?cursor=${cursor}&limit=${limit}`),
  getClusterStatus: () => fetchJSON<ClusterStatusResponse>('/cluster'),
  getClusterNodes: () => fetchJSON<ClusterNodesResponse>('/cluster/nodes'),
}

// ---------------------------------------------------------------------------
// Mode detection & unified export
// ---------------------------------------------------------------------------

/** WASM mode when URL path starts with /demo or VITE_WASM env var is set. */
function shouldUseWasm(): boolean {
  if (typeof window !== 'undefined' && window.location.pathname.startsWith('/demo')) {
    return true
  }
  return import.meta.env.VITE_WASM === 'true'
}

let _wasmApi: Api | null = null
let _wasmInit: Promise<Api> | null = null

/**
 * Initialize the WASM backend: load module, open DB, seed demo data.
 * Returns the WASM api instance.
 */
export async function initWasmDemo(): Promise<Api> {
  if (_wasmApi) return _wasmApi
  if (_wasmInit) return _wasmInit

  _wasmInit = (async () => {
    const wasm = await loadGraphDBWasm()
    wasm.open(2) // 2 shards
    wasm.seedDemo()
    _wasmApi = createWasmApi(wasm)
    return _wasmApi
  })()

  return _wasmInit
}

/** True if the current session is running in WASM demo mode. */
export function isWasmMode(): boolean {
  return shouldUseWasm()
}

/**
 * The main API export. In WASM mode, all calls go through the in-browser
 * database. In normal mode, calls go to the HTTP server.
 *
 * Components import `{ api }` from this file — no changes needed.
 */
export const api: Api = new Proxy(httpApi, {
  get(target, prop, receiver) {
    // In WASM mode, delegate to the WASM api if initialized.
    if (shouldUseWasm() && _wasmApi) {
      return Reflect.get(_wasmApi, prop, receiver)
    }
    return Reflect.get(target, prop, receiver)
  },
})
