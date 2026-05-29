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
	"os"
	"time"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"
)

// openBoltDBWithFallback attempts to open BoltDB directly in read-only mode.
// If it fails (due to locking/timeout), it copies the database to a temporary file in targetDir,
// opens it there, and returns both the DB handle and the temporary path so it can be cleaned up later.
func openBoltDBWithFallback(srcPath, targetDir string) (*bolt.DB, string, error) {
	// First, try opening the database directly in read-only mode with a short timeout.
	// We use a 1-second timeout here to fail quickly and activate the robust fallback.
	db, err := bolt.Open(srcPath, 0o666, &bolt.Options{ReadOnly: true, Timeout: 1 * time.Second})
	if err == nil {
		return db, "", nil
	}

	// Fallback path with verification and retry loop (up to 3 attempts)
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		tempFile, err := os.CreateTemp(targetDir, "emdbmgr_bolt_fallback_*.db")
		if err != nil {
			return nil, "", fmt.Errorf("failed to create temporary BoltDB copy: %w", err)
		}
		tempPath := tempFile.Name()
		_ = tempFile.Close()

		if err := copyFile(srcPath, tempPath); err != nil {
			_ = os.Remove(tempPath)
			lastErr = fmt.Errorf("failed to copy locked BoltDB file: %w", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Verify the copied database integrity using Tx.Check() consistency check
		if err := verifyBoltDB(tempPath); err != nil {
			_ = os.Remove(tempPath)
			lastErr = fmt.Errorf("integrity check failed for copied BoltDB (attempt %d): %w", attempt, err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Open the verified database copy. Since it is a temporary copy and no other process has it open,
		// standard bbolt lock acquisition will succeed immediately without any timeouts.
		db, err = bolt.Open(tempPath, 0o666, &bolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
		if err != nil {
			_ = os.Remove(tempPath)
			lastErr = fmt.Errorf("failed to open verified BoltDB copy: %w", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		return db, tempPath, nil
	}

	return nil, "", fmt.Errorf("all BoltDB copy attempts failed. Last error: %w", lastErr)
}

// verifyBoltDB performs consistency checks on a BoltDB database file to detect torn writes/corruption
func verifyBoltDB(path string) error {
	db, err := bolt.Open(path, 0o666, &bolt.Options{ReadOnly: true, Timeout: 2 * time.Second})
	if err != nil {
		return fmt.Errorf("failed to open for integrity check: %w", err)
	}
	defer func() {
		_ = db.Close()
	}()

	var checkErrs []error
	err = db.View(func(tx *bolt.Tx) error {
		ch := tx.Check()
		for err := range ch {
			if err != nil {
				checkErrs = append(checkErrs, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(checkErrs) > 0 {
		return fmt.Errorf("consistency check failed with %d errors: %v", len(checkErrs), checkErrs[0])
	}
	return nil
}

// BackupBoltDB copies the active BoltDB database to w using a consistent read-only transaction
func BackupBoltDB(srcPath, targetDir string, w io.Writer) error {
	db, tempPath, err := openBoltDBWithFallback(srcPath, targetDir)
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
		if tempPath != "" {
			_ = os.Remove(tempPath)
		}
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
func BackupBoltJSON(srcPath, targetDir string, w io.Writer) error {
	db, tempPath, err := openBoltDBWithFallback(srcPath, targetDir)
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
		if tempPath != "" {
			_ = os.Remove(tempPath)
		}
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
