//go:build js

package graphdb

import (
	"fmt"

	"github.com/mstrYoda/goraphdb/wasm"
)

type IntegrityError struct {
	Shard   int
	Bucket  string
	Key     string
	Message string
}

func (e IntegrityError) Error() string {
	return fmt.Sprintf("shard %d, %s[%s]: %s", e.Shard, e.Bucket, e.Key, e.Message)
}

type IntegrityReport struct {
	NodesChecked int
	EdgesChecked int
	Errors       []IntegrityError
}

func (r *IntegrityReport) OK() bool {
	return len(r.Errors) == 0
}

func (db *DB) VerifyIntegrity() (*IntegrityReport, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	report := &IntegrityReport{}
	for idx, s := range db.shards {
		err := s.db.View(func(tx *wasm.MemTx) error {
			nodesBucket := tx.Bucket(bucketNodes)
			if nodesBucket != nil {
				err := nodesBucket.ForEach(func(k, v []byte) error {
					report.NodesChecked++
					_, err := decodeProps(v)
					if err != nil {
						report.Errors = append(report.Errors, IntegrityError{
							Shard:   idx,
							Bucket:  "nodes",
							Key:     fmt.Sprintf("%x", k),
							Message: err.Error(),
						})
					}
					return nil
				})
				if err != nil {
					return err
				}
			}
			edgesBucket := tx.Bucket(bucketEdges)
			if edgesBucket != nil {
				err := edgesBucket.ForEach(func(k, v []byte) error {
					report.EdgesChecked++
					_, err := decodeEdge(v)
					if err != nil {
						report.Errors = append(report.Errors, IntegrityError{
							Shard:   idx,
							Bucket:  "edges",
							Key:     fmt.Sprintf("%x", k),
							Message: err.Error(),
						})
					}
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return report, fmt.Errorf("graphdb: integrity check failed on shard %d: %w", idx, err)
		}
	}
	if report.OK() {
		db.log.Info("integrity check passed", "nodes_checked", report.NodesChecked, "edges_checked", report.EdgesChecked)
	} else {
		db.log.Error("integrity check found errors", "nodes_checked", report.NodesChecked, "edges_checked", report.EdgesChecked, "errors", len(report.Errors))
	}
	return report, nil
}
