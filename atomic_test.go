package graphdb

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func openAtomicTestDB(t *testing.T) *DB {
	t.Helper()
	opts := DefaultOptions()
	opts.NoSync = false
	opts.ShardCount = 1
	db, err := Open(filepath.Join(t.TempDir(), "graph"), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestUpdateAtomicCommitsCypherAndMetadata(t *testing.T) {
	db := openAtomicTestDB(t)
	var created NodeID
	err := db.UpdateAtomic(context.Background(), func(tx *AtomicTx) error {
		result, err := tx.CypherWithParams(context.Background(),
			`CREATE (n:Person {name: $name}) RETURN n`, map[string]any{"name": "Alice"})
		if err != nil {
			return err
		}
		created = result.Rows[0]["n"].(*Node).ID
		return tx.PutMetadata([]byte("rhiza/applied_slot"), []byte("1"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if created != 1 {
		t.Fatalf("created ID=%d, want 1", created)
	}
	result, err := db.CypherWithParams(context.Background(),
		`MATCH (n:Person {name: $name}) RETURN n`, map[string]any{"name": "Alice"})
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("query rows=%d err=%v", len(result.Rows), err)
	}
	if err := db.UpdateAtomic(context.Background(), func(tx *AtomicTx) error {
		if got := string(tx.GetMetadata([]byte("rhiza/applied_slot"))); got != "1" {
			t.Fatalf("metadata=%q, want 1", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateAtomicRollbackDoesNotConsumeID(t *testing.T) {
	db := openAtomicTestDB(t)
	wantErr := errors.New("rollback")
	err := db.UpdateAtomic(context.Background(), func(tx *AtomicTx) error {
		if _, err := tx.CypherWithParams(context.Background(),
			`CREATE (n:Person {name: $name})`, map[string]any{"name": "discarded"}); err != nil {
			return err
		}
		if err := tx.PutMetadata([]byte("rhiza/applied_slot"), []byte("1")); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("rollback error=%v", err)
	}
	if db.NodeCount() != 0 {
		t.Fatalf("node count=%d after rollback", db.NodeCount())
	}
	var created NodeID
	if err := db.UpdateAtomic(context.Background(), func(tx *AtomicTx) error {
		if tx.GetMetadata([]byte("rhiza/applied_slot")) != nil {
			t.Fatal("rolled-back metadata is visible")
		}
		result, err := tx.CypherWithParams(context.Background(),
			`CREATE (n:Person {name: $name}) RETURN n`, map[string]any{"name": "kept"})
		if err == nil {
			created = result.Rows[0]["n"].(*Node).ID
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if created != 1 {
		t.Fatalf("created ID=%d after rollback, want 1", created)
	}
}

func TestUpdateAtomicParameterizedMergeAndSet(t *testing.T) {
	db := openAtomicTestDB(t)
	for _, age := range []int64{30, 31} {
		if err := db.UpdateAtomic(context.Background(), func(tx *AtomicTx) error {
			_, err := tx.CypherWithParams(context.Background(),
				`MERGE (n:Person {name: $name}) ON CREATE SET n.age = $age ON MATCH SET n.age = $age RETURN n`,
				map[string]any{"name": "Alice", "age": age})
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	if db.NodeCount() != 1 {
		t.Fatalf("node count=%d, want 1", db.NodeCount())
	}
	result, err := db.CypherWithParams(context.Background(),
		`MATCH (n:Person {name: $name}) RETURN n.age`, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0]["n.age"]; got != int64(31) {
		t.Fatalf("age=%v (%T), want 31", got, got)
	}
}

func TestUpdateAtomicRejectsMultipleShards(t *testing.T) {
	opts := DefaultOptions()
	opts.ShardCount = 2
	db, err := Open(filepath.Join(t.TempDir(), "graph"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.UpdateAtomic(context.Background(), func(*AtomicTx) error { return nil })
	if err == nil {
		t.Fatal("expected multi-shard atomic update rejection")
	}
}

func TestCypherReadWithParamsRejectsMutation(t *testing.T) {
	db := openAtomicTestDB(t)
	if _, err := db.CypherReadWithParams(context.Background(),
		`CREATE (n:Person {name: $name})`, map[string]any{"name": "Alice"}); err == nil {
		t.Fatal("read API accepted CREATE")
	}
	if db.NodeCount() != 0 {
		t.Fatalf("node count=%d after rejected mutation", db.NodeCount())
	}
}
