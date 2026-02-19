//go:build js

package graphdb

import (
	"context"
	"fmt"
	"sync"

	"github.com/mstrYoda/goraphdb/wasm"
)

type Snapshot struct {
	db       *DB
	shardTxs []*wasm.MemTx
	released bool
	mu       sync.Mutex
}

func (db *DB) Snapshot() (*Snapshot, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	snap := &Snapshot{
		db:       db,
		shardTxs: make([]*wasm.MemTx, len(db.shards)),
	}
	// In WASM in-memory mode, snapshots are lightweight.
	// We just hold references; the MemDB View provides consistent reads.
	db.log.Debug("snapshot created", "shards", len(db.shards))
	return snap, nil
}

func (s *Snapshot) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return
	}
	s.released = true
	s.db.log.Debug("snapshot released")
}

func (s *Snapshot) isReleased() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.released
}

func (s *Snapshot) Cypher(ctx context.Context, query string) (*CypherResult, error) {
	if s.isReleased() {
		return nil, fmt.Errorf("graphdb: snapshot has been released")
	}
	if s.db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	return safeExecuteResult(func() (*CypherResult, error) {
		ctx, cancel := s.db.governor.wrapContext(ctx)
		defer cancel()
		parsed, err := parseCypher(query)
		if err != nil {
			return nil, err
		}
		if parsed.write != nil {
			return nil, fmt.Errorf("graphdb: snapshot does not support write queries")
		}
		return s.db.executeCypher(ctx, parsed.read)
	})
}

func (s *Snapshot) CypherWithParams(ctx context.Context, query string, params map[string]any) (*CypherResult, error) {
	if s.isReleased() {
		return nil, fmt.Errorf("graphdb: snapshot has been released")
	}
	if s.db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	return safeExecuteResult(func() (*CypherResult, error) {
		ctx, cancel := s.db.governor.wrapContext(ctx)
		defer cancel()
		parsed, err := parseCypher(query)
		if err != nil {
			return nil, err
		}
		if parsed.write != nil {
			return nil, fmt.Errorf("graphdb: snapshot does not support write queries")
		}
		ast := parsed.read
		if len(params) > 0 {
			resolved := *ast
			if err := resolveParams(&resolved, params); err != nil {
				return nil, err
			}
			ast = &resolved
		}
		return s.db.executeCypher(ctx, ast)
	})
}
