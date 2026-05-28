// Package main provides the BoltDB (bbolt) integration subsystem.
//
// Objectives:
//  1. Safe MVCC Backups: Perform transactionally consistent block-level backups of live BoltDB databases
//     using shared read-only transactions (`ReadOnly: true`) without blocking concurrent writers or risking corruption.
//  2. Recursive Hierarchical JSON Dumps: Support dumping full, nested bucket trees recursively into a standard JSON representation.
//  3. Robust Type-Agnostic Format Handling: Safely identify and render non-UTF8 binary keys and values using Base64 encoding.
//
// Core Components:
//   - BackupBoltDB: Establishes a read-only shared database handle and calls the bbolt transaction `WriteTo`
//     writer to coordinate raw database page copy streams.
//   - BackupBoltJSON: Acts as the entrypoint for recursive schema dumping, opening a view transaction
//     and iterating through root-level buckets.
//   - streamBucketContents: A recursive helper that uses a Bolt cursor to sequentially fetch all key-value
//     records in a bucket, then recursively traverses its child buckets.
//   - formatBytes: Converts raw byte slices into readable UTF-8 strings, or base64-encoded strings with a
//     "base64:" prefix if invalid UTF-8 bytes are detected.
//
// Functionality and Data Flows:
//   - Physical Block Replication:
//     Source Path (Disk) -> bolt.Open(ReadOnly) -> Read-Only Tx (tx.WriteTo) -> Stream to io.Writer
//   - Logical Recursive JSON Streaming:
//     Source Path -> bolt.Open(ReadOnly) -> Root Buckets Loop -> Cursor Iteration -> streamBucketContents (Recurse) -> formatBytes -> JSON Marshal -> Stream to io.Writer
package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"time"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"
)

// BackupBoltDB copies the active BoltDB database to w using a consistent read-only transaction
func BackupBoltDB(srcPath string, w io.Writer) error {
	// Open database in shared read-only mode
	db, err := bolt.Open(srcPath, 0o666, &bolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("failed to open BoltDB in read-only mode: %w", err)
	}
	defer func() {
		_ = db.Close()
	}()

	// Run a read-only transaction and copy the database pages safely
	err = db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(w)
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to perform safe BoltDB block copy: %w", err)
	}

	return nil
}

// BackupBoltJSON dumps the BoltDB as a recursive JSON structure, streaming directly to w
func BackupBoltJSON(srcPath string, w io.Writer) error {
	// Open database in shared read-only mode
	db, err := bolt.Open(srcPath, 0o666, &bolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("failed to open BoltDB in read-only mode: %w", err)
	}
	defer func() {
		_ = db.Close()
	}()

	// Write JSON header
	_, err = io.WriteString(w, `{"type":"boldb","buckets":{`)
	if err != nil {
		return err
	}

	// Run read transaction to scan buckets
	err = db.View(func(tx *bolt.Tx) error {
		firstBucket := true

		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if !firstBucket {
				if _, err := io.WriteString(w, ","); err != nil {
					return err
				}
			}
			firstBucket = false

			// Write bucket name
			bucketNameStr := formatBytes(name)
			if _, err := io.WriteString(w, fmt.Sprintf("%q:", bucketNameStr)); err != nil {
				return err
			}

			// Stream the contents of this bucket
			return streamBucketContents(b, w)
		})
	})
	if err != nil {
		return fmt.Errorf("failed to stream BoltDB buckets: %w", err)
	}

	// Write JSON footer
	_, err = io.WriteString(w, "}}")
	return err
}

// streamBucketContents recursively streams keys and sub-buckets of a BoltDB bucket
func streamBucketContents(b *bolt.Bucket, w io.Writer) error {
	if _, err := io.WriteString(w, `{"keys":[`); err != nil {
		return err
	}

	c := b.Cursor()
	firstKey := true

	// 1. Iterate keys first
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if v != nil { // This is a standard key-value pair
			if !firstKey {
				if _, err := io.WriteString(w, ","); err != nil {
					return err
				}
			}
			firstKey = false

			keyStr := formatBytes(k)
			valStr := formatBytes(v)

			entry := struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}{
				Key:   keyStr,
				Value: valStr,
			}

			data, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			if _, err := w.Write(data); err != nil {
				return err
			}
		}
	}

	// 2. Iterate sub-buckets
	if _, err := io.WriteString(w, `],"sub_buckets":{`); err != nil {
		return err
	}

	firstSubBucket := true
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if v == nil { // This is a sub-bucket
			if !firstSubBucket {
				if _, err := io.WriteString(w, ","); err != nil {
					return err
				}
			}
			firstSubBucket = false

			subNameStr := formatBytes(k)
			if _, err := io.WriteString(w, fmt.Sprintf("%q:", subNameStr)); err != nil {
				return err
			}

			subBucket := b.Bucket(k)
			if err := streamBucketContents(subBucket, w); err != nil {
				return err
			}
		}
	}

	if _, err := io.WriteString(w, `}}`); err != nil {
		return err
	}

	return nil
}

// formatBytes checks if a slice of bytes is valid UTF-8, else formats it as base64
func formatBytes(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	return "base64:" + base64.StdEncoding.EncodeToString(b)
}
