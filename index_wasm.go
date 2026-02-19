//go:build js

package graphdb

import (
	"bytes"
	"context"
	"fmt"

	"github.com/mstrYoda/goraphdb/wasm"
)

func (db *DB) CreateIndex(propName string) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}

	for _, s := range db.shards {
		err := s.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
			idxBucket := tx.Bucket(bucketIdxProp)
			nodesBucket := tx.Bucket(bucketNodes)
			return nodesBucket.ForEach(func(k, v []byte) error {
				props, err := decodeProps(v)
				if err != nil {
					return nil
				}
				val, ok := props[propName]
				if !ok {
					return nil
				}
				idxKeyStr := fmt.Sprintf("%s:%v", propName, val)
				nodeID := decodeNodeID(k)
				idxKey := encodeIndexKey(idxKeyStr, uint64(nodeID))
				return idxBucket.Put(idxKey, nil)
			})
		})
		if err != nil {
			return fmt.Errorf("graphdb: failed to create index on %s: %w", propName, err)
		}
	}

	db.indexedProps.Store(propName, true)
	db.walAppend(OpCreateIndex, WALCreateIndex{PropName: propName})
	return nil
}

func (db *DB) FindByProperty(propName string, value interface{}) ([]*Node, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	_, hasIndex := db.indexedProps.Load(propName)
	if hasIndex && db.metrics != nil {
		db.metrics.IndexLookups.Add(1)
	}

	idxKeyStr := fmt.Sprintf("%s:%v", propName, value)
	prefix := encodeIndexPrefix(idxKeyStr)

	var nodes []*Node

	for _, s := range db.shards {
		err := s.db.View(func(tx *wasm.MemTx) error {
			idxBucket := tx.Bucket(bucketIdxProp)
			nodesBucket := tx.Bucket(bucketNodes)

			if hasIndex {
				c := idxBucket.Cursor()
				for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
					nodeIDBytes := k[len(prefix):]
					if len(nodeIDBytes) < 8 {
						continue
					}
					nodeID := decodeNodeID(nodeIDBytes)
					data := nodesBucket.Get(encodeNodeID(nodeID))
					if data == nil {
						continue
					}
					props, err := decodeProps(data)
					if err != nil {
						continue
					}
					nodes = append(nodes, &Node{ID: nodeID, Props: props})
				}
				return nil
			}

			return nodesBucket.ForEach(func(k, v []byte) error {
				props, err := decodeProps(v)
				if err != nil {
					return nil
				}
				if val, ok := props[propName]; ok && fmt.Sprintf("%v", val) == fmt.Sprintf("%v", value) {
					nodeID := decodeNodeID(k)
					nodes = append(nodes, &Node{ID: nodeID, Props: props})
				}
				return nil
			})
		})
		if err != nil {
			return nil, err
		}
	}
	return nodes, nil
}

func (db *DB) DropIndex(propName string) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}

	prefix := []byte(propName + ":")

	for _, s := range db.shards {
		err := s.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
			idxBucket := tx.Bucket(bucketIdxProp)
			c := idxBucket.Cursor()
			var toDelete [][]byte
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				keyCopy := make([]byte, len(k))
				copy(keyCopy, k)
				toDelete = append(toDelete, keyCopy)
			}
			for _, k := range toDelete {
				if err := idxBucket.Delete(k); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("graphdb: failed to drop index on %s: %w", propName, err)
		}
	}

	db.indexedProps.Delete(propName)
	db.walAppend(OpDropIndex, WALDropIndex{PropName: propName})
	return nil
}

func (db *DB) ListIndexes() []string {
	var names []string
	db.indexedProps.Range(func(key, _ any) bool {
		names = append(names, key.(string))
		return true
	})
	if names == nil {
		names = []string{}
	}
	return names
}

func (db *DB) ReIndex(propName string) error {
	if err := db.DropIndex(propName); err != nil {
		return err
	}
	return db.CreateIndex(propName)
}

func (db *DB) indexNodeProps(tx *wasm.MemTx, nodeID NodeID, props Props) error {
	if len(props) == 0 {
		return nil
	}
	idxBucket := tx.Bucket(bucketIdxProp)
	var firstErr error
	db.indexedProps.Range(func(key, _ any) bool {
		propName := key.(string)
		val, ok := props[propName]
		if !ok {
			return true
		}
		idxKeyStr := fmt.Sprintf("%s:%v", propName, val)
		idxKey := encodeIndexKey(idxKeyStr, uint64(nodeID))
		if err := idxBucket.Put(idxKey, nil); err != nil {
			firstErr = err
			return false
		}
		return true
	})
	if firstErr != nil {
		return firstErr
	}
	return db.indexNodeComposite(tx, nodeID, props)
}

func (db *DB) unindexNodeProps(tx *wasm.MemTx, nodeID NodeID, props Props) error {
	if len(props) == 0 {
		return nil
	}
	idxBucket := tx.Bucket(bucketIdxProp)
	var firstErr error
	db.indexedProps.Range(func(key, _ any) bool {
		propName := key.(string)
		val, ok := props[propName]
		if !ok {
			return true
		}
		idxKeyStr := fmt.Sprintf("%s:%v", propName, val)
		idxKey := encodeIndexKey(idxKeyStr, uint64(nodeID))
		if err := idxBucket.Delete(idxKey); err != nil {
			firstErr = err
			return false
		}
		return true
	})
	if firstErr != nil {
		return firstErr
	}
	return db.unindexNodeComposite(tx, nodeID, props)
}
