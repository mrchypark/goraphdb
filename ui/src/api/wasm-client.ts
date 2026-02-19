// ---------------------------------------------------------------------------
// WASM API Client — drop-in replacement for the HTTP api client.
//
// Instead of making fetch() calls to /api/*, this client calls the Go WASM
// functions exposed on globalThis._graphdb. The response shapes are identical
// to the HTTP server so the UI components work unchanged.
// ---------------------------------------------------------------------------

import type { GraphDBWasm, WasmResult } from './wasm-loader'
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

function unwrap<T>(result: WasmResult): T {
  if (result.error) {
    throw new Error(result.error as string)
  }
  return result as unknown as T
}

export function createWasmApi(wasm: GraphDBWasm) {
  return {
    // ── Stats ──────────────────────────────────────────────────────────
    getStats: async (): Promise<GraphStats> => {
      return unwrap<GraphStats>(wasm.stats())
    },

    // ── Cypher ─────────────────────────────────────────────────────────
    executeCypher: async (query: string): Promise<CypherResponse> => {
      return unwrap<CypherResponse>(wasm.cypher(query))
    },

    // ── Indexes ────────────────────────────────────────────────────────
    listIndexes: async (): Promise<IndexListResponse> => {
      return unwrap<IndexListResponse>(wasm.listIndexes())
    },
    createIndex: async (property: string): Promise<{ status: string }> => {
      return unwrap<{ status: string }>(wasm.createIndex(property))
    },
    dropIndex: async (name: string): Promise<{ status: string }> => {
      return unwrap<{ status: string }>(wasm.dropIndex(name))
    },
    reIndex: async (name: string): Promise<{ status: string }> => {
      return unwrap<{ status: string }>(wasm.reIndex(name))
    },

    // ── Nodes ──────────────────────────────────────────────────────────
    listNodes: async (limit = 50, offset = 0): Promise<NodeListResponse> => {
      return unwrap<NodeListResponse>(wasm.listNodes(limit, offset))
    },
    getNode: async (id: number): Promise<GNode> => {
      return unwrap<GNode>(wasm.getNode(id))
    },
    getNeighborhood: async (id: number): Promise<NeighborhoodResponse> => {
      return unwrap<NeighborhoodResponse>(wasm.getNeighborhood(id))
    },
    createNode: async (props: Record<string, any>): Promise<{ id: number }> => {
      return unwrap<{ id: number }>(wasm.addNode(JSON.stringify(props)))
    },
    deleteNode: async (id: number): Promise<{ status: string }> => {
      return unwrap<{ status: string }>(wasm.deleteNode(id))
    },

    // ── Edges ──────────────────────────────────────────────────────────
    createEdge: async (
      from: number,
      to: number,
      label: string,
      props?: Record<string, any>,
    ): Promise<{ id: number }> => {
      const propsJSON = props ? JSON.stringify(props) : undefined
      return unwrap<{ id: number }>(wasm.addEdge(from, to, label, propsJSON))
    },
    deleteEdge: async (id: number): Promise<{ status: string }> => {
      return unwrap<{ status: string }>(wasm.deleteEdge(id))
    },

    // ── Metrics ─────────────────────────────────────────────────────────
    getMetrics: async (): Promise<MetricsSnapshot> => {
      return unwrap<MetricsSnapshot>(wasm.getMetrics())
    },

    // ── Slow Queries ────────────────────────────────────────────────────
    getSlowQueries: async (limit = 50): Promise<SlowQueryResponse> => {
      return unwrap<SlowQueryResponse>(wasm.getSlowQueries(limit))
    },

    // ── Cursor Pagination ───────────────────────────────────────────────
    listNodesCursor: async (cursor = 0, limit = 30): Promise<NodeCursorPage> => {
      return unwrap<NodeCursorPage>(wasm.listNodesCursor(cursor, limit))
    },
    listEdgesCursor: async (cursor = 0, limit = 30): Promise<EdgeCursorPage> => {
      return unwrap<EdgeCursorPage>(wasm.listEdgesCursor(cursor, limit))
    },

    // ── Cluster ──────────────────────────────────────────────────────────
    getClusterStatus: async (): Promise<ClusterStatusResponse> => {
      return unwrap<ClusterStatusResponse>(wasm.getClusterStatus())
    },
    getClusterNodes: async (): Promise<ClusterNodesResponse> => {
      return unwrap<ClusterNodesResponse>(wasm.getClusterNodes())
    },
  }
}
