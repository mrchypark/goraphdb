package graphdb

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestFileSnapshotPinsMetadata(t *testing.T) {
	db, err := Open(t.TempDir(), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	key := []byte("checkpoint-tip")
	if err := db.UpdateAtomic(context.Background(), func(tx *AtomicTx) error { return tx.PutMetadata(key, []byte("before")) }); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.BeginFileSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if err := db.UpdateAtomic(context.Background(), func(tx *AtomicTx) error { return tx.PutMetadata(key, []byte("after")) }); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.db")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.WriteTo(context.Background(), file); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	copyDB, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer copyDB.Close()
	if err := copyDB.View(func(tx *bolt.Tx) error {
		if got := string(tx.Bucket(bucketAppMeta).Get(key)); got != "before" {
			t.Fatalf("snapshot metadata=%q, want before", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
