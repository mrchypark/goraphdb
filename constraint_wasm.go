//go:build js

package graphdb

import (
	"bytes"
	"context"
	"fmt"

	"github.com/mstrYoda/goraphdb/wasm"
)

var ErrUniqueConstraintViolation = fmt.Errorf("graphdb: unique constraint violation")

type UniqueConstraint struct {
	Label    string `json:"label"`
	Property string `json:"property"`
}

func (uc UniqueConstraint) constraintKey() string {
	return uc.Label + "\x00" + uc.Property
}

func (db *DB) CreateUniqueConstraint(label, property string) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}

	uc := UniqueConstraint{Label: label, Property: property}
	if _, loaded := db.uniqueConstraints.Load(uc.constraintKey()); loaded {
		return nil
	}

	seen := make(map[string]NodeID)
	for _, s := range db.shards {
		err := s.db.View(func(tx *wasm.MemTx) error {
			lblBucket := tx.Bucket(bucketIdxNodeLabel)
			nodesBucket := tx.Bucket(bucketNodes)
			prefix := encodeLabelIndexPrefix(label)
			c := lblBucket.Cursor()
			for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
				if len(k) < len(prefix)+8 {
					continue
				}
				nodeID := decodeNodeID(k[len(prefix):])
				data := nodesBucket.Get(encodeNodeID(nodeID))
				if data == nil {
					continue
				}
				props, err := decodeProps(data)
				if err != nil {
					continue
				}
				val, ok := props[property]
				if !ok {
					continue
				}
				valStr := fmt.Sprintf("%v", val)
				if existingID, dup := seen[valStr]; dup {
					return fmt.Errorf("%w: label=%s property=%s value=%q (nodes %d and %d)",
						ErrUniqueConstraintViolation, label, property, valStr, existingID, nodeID)
				}
				seen[valStr] = nodeID
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	for valStr, nodeID := range seen {
		target := db.shardFor(nodeID)
		err := target.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
			idxKey := encodeUniqueKey(label, property, valStr)
			return tx.Bucket(bucketIdxUnique).Put(idxKey, encodeNodeID(nodeID))
		})
		if err != nil {
			return fmt.Errorf("graphdb: failed to build unique index: %w", err)
		}
	}

	err := db.primaryShard().writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
		metaKey := encodeConstraintMetaKey(label, property)
		return tx.Bucket(bucketUniqueMeta).Put(metaKey, nil)
	})
	if err != nil {
		return fmt.Errorf("graphdb: failed to persist constraint metadata: %w", err)
	}

	db.uniqueConstraints.Store(uc.constraintKey(), uc)
	db.walAppend(OpCreateUniqueConstraint, WALCreateUniqueConstraint{Label: label, Property: property})
	db.log.Info("unique constraint created", "label", label, "property", property)
	return nil
}

func (db *DB) DropUniqueConstraint(label, property string) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}

	uc := UniqueConstraint{Label: label, Property: property}
	if _, loaded := db.uniqueConstraints.Load(uc.constraintKey()); !loaded {
		return nil
	}

	prefix := encodeUniquePrefix(label, property)
	for _, s := range db.shards {
		err := s.writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
			idxBucket := tx.Bucket(bucketIdxUnique)
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
			return err
		}
	}

	err := db.primaryShard().writeUpdate(context.Background(), func(tx *wasm.MemTx) error {
		return tx.Bucket(bucketUniqueMeta).Delete(encodeConstraintMetaKey(label, property))
	})
	if err != nil {
		return err
	}

	db.uniqueConstraints.Delete(uc.constraintKey())
	db.walAppend(OpDropUniqueConstraint, WALDropUniqueConstraint{Label: label, Property: property})
	db.log.Info("unique constraint dropped", "label", label, "property", property)
	return nil
}

func (db *DB) HasUniqueConstraint(label, property string) bool {
	uc := UniqueConstraint{Label: label, Property: property}
	_, ok := db.uniqueConstraints.Load(uc.constraintKey())
	return ok
}

func (db *DB) ListUniqueConstraints() []UniqueConstraint {
	var result []UniqueConstraint
	db.uniqueConstraints.Range(func(_, value any) bool {
		result = append(result, value.(UniqueConstraint))
		return true
	})
	return result
}

func (db *DB) checkUniqueConstraints(labels []string, props Props, excludeID NodeID) error {
	if len(labels) == 0 || len(props) == 0 {
		return nil
	}
	for _, label := range labels {
		var checkErr error
		db.uniqueConstraints.Range(func(_, value any) bool {
			uc := value.(UniqueConstraint)
			if uc.Label != label {
				return true
			}
			val, ok := props[uc.Property]
			if !ok {
				return true
			}
			valStr := fmt.Sprintf("%v", val)
			existingID, found := db.lookupUniqueIndex(label, uc.Property, valStr)
			if found && existingID != excludeID {
				checkErr = fmt.Errorf("%w: label=%s property=%s value=%q already exists (node %d)",
					ErrUniqueConstraintViolation, label, uc.Property, valStr, existingID)
				return false
			}
			return true
		})
		if checkErr != nil {
			return checkErr
		}
	}
	return nil
}

func (db *DB) checkUniqueConstraintsInTx(tx *wasm.MemTx, labels []string, props Props, excludeID NodeID) error {
	if len(labels) == 0 || len(props) == 0 {
		return nil
	}
	idxBucket := tx.Bucket(bucketIdxUnique)
	if idxBucket == nil {
		return nil
	}
	for _, label := range labels {
		var checkErr error
		db.uniqueConstraints.Range(func(_, value any) bool {
			uc := value.(UniqueConstraint)
			if uc.Label != label {
				return true
			}
			val, ok := props[uc.Property]
			if !ok {
				return true
			}
			valStr := fmt.Sprintf("%v", val)
			idxKey := encodeUniqueKey(label, uc.Property, valStr)
			existing := idxBucket.Get(idxKey)
			if existing != nil {
				existingID := decodeNodeID(existing)
				if existingID != excludeID {
					checkErr = fmt.Errorf("%w: label=%s property=%s value=%q already exists (node %d)",
						ErrUniqueConstraintViolation, label, uc.Property, valStr, existingID)
					return false
				}
			}
			return true
		})
		if checkErr != nil {
			return checkErr
		}
	}
	return nil
}

func (db *DB) indexUniqueConstraints(tx *wasm.MemTx, nodeID NodeID, labels []string, props Props) error {
	if len(labels) == 0 || len(props) == 0 {
		return nil
	}
	idxBucket := tx.Bucket(bucketIdxUnique)
	if idxBucket == nil {
		return nil
	}
	for _, label := range labels {
		db.uniqueConstraints.Range(func(_, value any) bool {
			uc := value.(UniqueConstraint)
			if uc.Label != label {
				return true
			}
			val, ok := props[uc.Property]
			if !ok {
				return true
			}
			valStr := fmt.Sprintf("%v", val)
			idxKey := encodeUniqueKey(label, uc.Property, valStr)
			_ = idxBucket.Put(idxKey, encodeNodeID(nodeID))
			return true
		})
	}
	return nil
}

func (db *DB) unindexUniqueConstraints(tx *wasm.MemTx, nodeID NodeID, labels []string, props Props) error {
	if len(labels) == 0 || len(props) == 0 {
		return nil
	}
	idxBucket := tx.Bucket(bucketIdxUnique)
	if idxBucket == nil {
		return nil
	}
	for _, label := range labels {
		db.uniqueConstraints.Range(func(_, value any) bool {
			uc := value.(UniqueConstraint)
			if uc.Label != label {
				return true
			}
			val, ok := props[uc.Property]
			if !ok {
				return true
			}
			valStr := fmt.Sprintf("%v", val)
			idxKey := encodeUniqueKey(label, uc.Property, valStr)
			existing := idxBucket.Get(idxKey)
			if existing != nil && decodeNodeID(existing) == nodeID {
				_ = idxBucket.Delete(idxKey)
			}
			return true
		})
	}
	return nil
}

func (db *DB) lookupUniqueIndex(label, property, valueStr string) (NodeID, bool) {
	idxKey := encodeUniqueKey(label, property, valueStr)
	for _, s := range db.shards {
		var found bool
		var id NodeID
		_ = s.db.View(func(tx *wasm.MemTx) error {
			b := tx.Bucket(bucketIdxUnique)
			if b == nil {
				return nil
			}
			v := b.Get(idxKey)
			if v != nil {
				id = decodeNodeID(v)
				found = true
			}
			return nil
		})
		if found {
			return id, true
		}
	}
	return 0, false
}

func (db *DB) FindByUniqueConstraint(label, property string, value any) (*Node, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	valStr := fmt.Sprintf("%v", value)
	nodeID, found := db.lookupUniqueIndex(label, property, valStr)
	if !found {
		return nil, nil
	}
	return db.getNode(nodeID)
}

func (db *DB) discoverUniqueConstraints() {
	s := db.primaryShard()
	_ = s.db.View(func(tx *wasm.MemTx) error {
		meta := tx.Bucket(bucketUniqueMeta)
		if meta == nil {
			return nil
		}
		return meta.ForEach(func(k, _ []byte) error {
			label, property := decodeConstraintMetaKey(k)
			if label != "" && property != "" {
				uc := UniqueConstraint{Label: label, Property: property}
				db.uniqueConstraints.Store(uc.constraintKey(), uc)
			}
			return nil
		})
	})
}

func encodeUniqueKey(label, property, valueStr string) []byte {
	key := make([]byte, len(label)+1+len(property)+1+len(valueStr))
	n := copy(key, label)
	key[n] = 0x00
	n++
	n += copy(key[n:], property)
	key[n] = 0x00
	n++
	copy(key[n:], valueStr)
	return key
}

func encodeUniquePrefix(label, property string) []byte {
	prefix := make([]byte, len(label)+1+len(property)+1)
	n := copy(prefix, label)
	prefix[n] = 0x00
	n++
	n += copy(prefix[n:], property)
	prefix[n] = 0x00
	return prefix
}

func encodeConstraintMetaKey(label, property string) []byte {
	key := make([]byte, len(label)+1+len(property))
	n := copy(key, label)
	key[n] = 0x00
	n++
	copy(key[n:], property)
	return key
}

func decodeConstraintMetaKey(key []byte) (label, property string) {
	idx := bytes.IndexByte(key, 0x00)
	if idx < 0 {
		return "", ""
	}
	return string(key[:idx]), string(key[idx+1:])
}
