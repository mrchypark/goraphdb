//go:build js

package graphdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/mstrYoda/goraphdb/wasm"
)

type compositeIndexDef struct {
	Properties []string
	Key        string
}

func newCompositeIndexDef(props []string) compositeIndexDef {
	sorted := make([]string, len(props))
	copy(sorted, props)
	sort.Strings(sorted)
	return compositeIndexDef{
		Properties: sorted,
		Key:        strings.Join(sorted, "\x00"),
	}
}

func (db *DB) CreateCompositeIndex(propNames ...string) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}
	if len(propNames) < 2 {
		return fmt.Errorf("graphdb: composite index requires at least 2 properties")
	}

	def := newCompositeIndexDef(propNames)

	for _, s := range db.shards {
		err := s.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
			idxBucket := tx.Bucket(bucketIdxComposite)
			nodesBucket := tx.Bucket(bucketNodes)
			return nodesBucket.ForEach(func(k, v []byte) error {
				props, err := decodeProps(v)
				if err != nil {
					return nil
				}
				idxKey := buildCompositeKey(def, props, decodeNodeID(k))
				if idxKey == nil {
					return nil
				}
				return idxBucket.Put(idxKey, nil)
			})
		})
		if err != nil {
			return fmt.Errorf("graphdb: failed to create composite index on %v: %w", propNames, err)
		}
	}

	db.compositeIndexes.Store(def.Key, def)
	db.walAppend(OpCreateCompositeIndex, WALCreateCompositeIndex{PropNames: propNames})
	db.log.Info("composite index created", "properties", def.Properties)
	return nil
}

func (db *DB) FindByCompositeIndex(filters map[string]any) ([]*Node, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	if len(filters) < 2 {
		return nil, fmt.Errorf("graphdb: composite lookup requires at least 2 properties")
	}

	propNames := make([]string, 0, len(filters))
	for k := range filters {
		propNames = append(propNames, k)
	}
	def := newCompositeIndexDef(propNames)

	_, hasIndex := db.compositeIndexes.Load(def.Key)
	if hasIndex {
		return db.findByCompositeIndexFast(def, filters)
	}
	return db.findByCompositeIndexScan(filters)
}

func (db *DB) findByCompositeIndexFast(def compositeIndexDef, filters map[string]any) ([]*Node, error) {
	prefix := buildCompositePrefix(def, filters)
	if prefix == nil {
		return nil, fmt.Errorf("graphdb: could not build composite prefix for %v", def.Properties)
	}

	var nodes []*Node
	for _, s := range db.shards {
		err := s.db.View(func(tx *wasm.MemTx) error {
			idxBucket := tx.Bucket(bucketIdxComposite)
			nodesBucket := tx.Bucket(bucketNodes)
			c := idxBucket.Cursor()
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				if len(k) < 8 {
					continue
				}
				nodeID := NodeID(binary.BigEndian.Uint64(k[len(k)-8:]))
				data := nodesBucket.Get(encodeNodeID(nodeID))
				if data == nil {
					continue
				}
				props, err := decodeProps(data)
				if err != nil {
					continue
				}
				labels := loadLabels(tx, nodeID)
				nodes = append(nodes, &Node{ID: nodeID, Labels: labels, Props: props})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return nodes, nil
}

func (db *DB) findByCompositeIndexScan(filters map[string]any) ([]*Node, error) {
	var nodes []*Node
	for _, s := range db.shards {
		err := s.db.View(func(tx *wasm.MemTx) error {
			nodesBucket := tx.Bucket(bucketNodes)
			return nodesBucket.ForEach(func(k, v []byte) error {
				props, err := decodeProps(v)
				if err != nil {
					return nil
				}
				for fk, fv := range filters {
					pv, ok := props[fk]
					if !ok || fmt.Sprintf("%v", pv) != fmt.Sprintf("%v", fv) {
						return nil
					}
				}
				nodeID := decodeNodeID(k)
				labels := loadLabels(tx, nodeID)
				nodes = append(nodes, &Node{ID: nodeID, Labels: labels, Props: props})
				return nil
			})
		})
		if err != nil {
			return nil, err
		}
	}
	return nodes, nil
}

func (db *DB) DropCompositeIndex(propNames ...string) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}

	def := newCompositeIndexDef(propNames)
	prefix := compositeDefPrefix(def)

	for _, s := range db.shards {
		err := s.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
			idxBucket := tx.Bucket(bucketIdxComposite)
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
			return fmt.Errorf("graphdb: failed to drop composite index on %v: %w", propNames, err)
		}
	}

	db.compositeIndexes.Delete(def.Key)
	db.walAppend(OpDropCompositeIndex, WALDropCompositeIndex{PropNames: propNames})
	db.log.Info("composite index dropped", "properties", def.Properties)
	return nil
}

func (db *DB) ListCompositeIndexes() [][]string {
	var result [][]string
	db.compositeIndexes.Range(func(_, value any) bool {
		def := value.(compositeIndexDef)
		cp := make([]string, len(def.Properties))
		copy(cp, def.Properties)
		result = append(result, cp)
		return true
	})
	return result
}

func (db *DB) HasCompositeIndex(propNames ...string) bool {
	def := newCompositeIndexDef(propNames)
	_, ok := db.compositeIndexes.Load(def.Key)
	return ok
}

func compositeDefPrefix(def compositeIndexDef) []byte {
	var buf bytes.Buffer
	for _, p := range def.Properties {
		buf.WriteString(p)
		buf.WriteByte(0x00)
	}
	return buf.Bytes()
}

func buildCompositeKey(def compositeIndexDef, props Props, nodeID NodeID) []byte {
	var buf bytes.Buffer
	for _, p := range def.Properties {
		buf.WriteString(p)
		buf.WriteByte(0x00)
	}
	for _, p := range def.Properties {
		val, ok := props[p]
		if !ok {
			return nil
		}
		buf.WriteString(fmt.Sprintf("%v", val))
		buf.WriteByte(0x00)
	}
	var idBuf [8]byte
	binary.BigEndian.PutUint64(idBuf[:], uint64(nodeID))
	buf.Write(idBuf[:])
	return buf.Bytes()
}

func buildCompositePrefix(def compositeIndexDef, filters map[string]any) []byte {
	var buf bytes.Buffer
	for _, p := range def.Properties {
		buf.WriteString(p)
		buf.WriteByte(0x00)
	}
	for _, p := range def.Properties {
		val, ok := filters[p]
		if !ok {
			return nil
		}
		buf.WriteString(fmt.Sprintf("%v", val))
		buf.WriteByte(0x00)
	}
	return buf.Bytes()
}

func (db *DB) indexNodeComposite(tx *wasm.MemTx, nodeID NodeID, props Props) error {
	idxBucket := tx.Bucket(bucketIdxComposite)
	var firstErr error
	db.compositeIndexes.Range(func(_, value any) bool {
		def := value.(compositeIndexDef)
		key := buildCompositeKey(def, props, nodeID)
		if key == nil {
			return true
		}
		if err := idxBucket.Put(key, nil); err != nil {
			firstErr = err
			return false
		}
		return true
	})
	return firstErr
}

func (db *DB) unindexNodeComposite(tx *wasm.MemTx, nodeID NodeID, props Props) error {
	idxBucket := tx.Bucket(bucketIdxComposite)
	var firstErr error
	db.compositeIndexes.Range(func(_, value any) bool {
		def := value.(compositeIndexDef)
		key := buildCompositeKey(def, props, nodeID)
		if key == nil {
			return true
		}
		if err := idxBucket.Delete(key); err != nil {
			firstErr = err
			return false
		}
		return true
	})
	return firstErr
}

func (db *DB) discoverCompositeIndexes() {
	seen := make(map[string]bool)
	for _, s := range db.shards {
		_ = s.db.View(func(tx *wasm.MemTx) error {
			b := tx.Bucket(bucketIdxComposite)
			if b == nil {
				return nil
			}
			c := b.Cursor()
			for k, _ := c.First(); k != nil; {
				defKey := extractCompositeDefKey(k)
				if defKey != "" && !seen[defKey] {
					seen[defKey] = true
					props := strings.Split(defKey, "\x00")
					var clean []string
					for _, p := range props {
						if p != "" {
							clean = append(clean, p)
						}
					}
					if len(clean) >= 2 {
						def := newCompositeIndexDef(clean)
						db.compositeIndexes.Store(def.Key, def)
					}
				}
				prefix := compositeDefPrefix(newCompositeIndexDef(strings.Split(defKey, "\x00")))
				next := make([]byte, len(prefix))
				copy(next, prefix)
				for i := len(next) - 1; i >= 0; i-- {
					next[i]++
					if next[i] != 0 {
						break
					}
				}
				k, _ = c.Seek(next)
			}
			return nil
		})
	}
}

func extractCompositeDefKey(k []byte) string {
	var props []string
	remaining := k
	for len(remaining) > 0 {
		idx := bytes.IndexByte(remaining, 0x00)
		if idx <= 0 {
			break
		}
		segment := string(remaining[:idx])
		if !looksLikePropName(segment) {
			break
		}
		props = append(props, segment)
		remaining = remaining[idx+1:]
	}
	if len(props) < 2 {
		return ""
	}
	return strings.Join(props, "\x00")
}

func looksLikePropName(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i, c := range s {
		if i == 0 {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
				return false
			}
		} else {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
				return false
			}
		}
	}
	return true
}
