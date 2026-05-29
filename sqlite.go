// Package main provides the SQLite integration subsystem.
//
// Objectives:
//  1. Safe Concurrency: Enable transactionally consistent, zero-blocking backups of active
//     SQLite databases (including databases operating in WAL or rollback modes) without producing corrupt hot journals.
//  2. High-Fidelity Serialization: Support streaming JSON dumps of all user-defined database tables
//     with automatic UTF-8 validation and Base64 encoding for raw binary BLOB fields.
//  3. Memory Efficiency: Avoid loading massive datasets into memory by streaming records directly to the destination writer.
//
// Core Components:
//   - BackupSQLiteDB: Coordinates vacuum-based block level replication. It establishes a read-only
//     connection to the active DB and performs a SQL-level safe clone to a temporary shadow database before streaming.
//   - BackupSQLiteJSON: Coordinates JSON table dumping. It queries database metadata, loops through
//     individual user-defined tables, inspects their column schemas dynamically, scans column values, and writes them streamingly.
//
// Functionality and Data Flows:
//   - Physical Block Replication:
//     Source Path (Disk) -> Read-Only Connection -> sql.DB (Exec "VACUUM INTO <temp>") -> Temp Shadow DB File -> Stream to io.Writer
//   - Logical JSON Streaming:
//     Source Path -> Read-Only Connection -> Query sqlite_master -> Iterate Tables -> Query Schema & Rows -> Row Scan -> Map Values -> JSON Marshal -> Stream to io.Writer
package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	_ "modernc.org/sqlite" // Pure-Go SQLite driver
)

// BackupSQLiteDB copies the active SQLite database to a shadow DB using VACUUM INTO
func BackupSQLiteDB(srcPath, targetDir string, w io.Writer) error {
	// First attempt: Standard VACUUM INTO from active DB
	err := func() error {
		db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=ro", srcPath))
		if err != nil {
			return err
		}
		defer func() {
			_ = db.Close()
		}()

		// Set busy timeout so we wait for temporary write locks to release
		_, _ = db.Exec("PRAGMA busy_timeout = 5000")

		// Create a temporary file path for the vacuumed database copy inside targetDir
		tempDest, err := os.CreateTemp(targetDir, "emdbmgr_sqlite_vacuum_*.db")
		if err != nil {
			return fmt.Errorf("failed to create temporary vacuum destination: %w", err)
		}
		tempDestPath := tempDest.Name()
		_ = tempDest.Close()
		defer func() {
			_ = os.Remove(tempDestPath)
		}()

		// Execute VACUUM INTO which is transactionally consistent and works while the DB is open
		// Use double-quotes or single-quotes around the destination path to escape Windows paths safely
		// #nosec G201
		// nosemgrep
		query := fmt.Sprintf("VACUUM INTO %q", tempDestPath)
		// #nosec G201
		// nosemgrep
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("failed to perform safe SQLite backup (VACUUM INTO): %w", err)
		}

		// Open the vacuumed file and stream it to our writer
		shadowFile, err := os.Open(tempDestPath) // #nosec G304 -- Path is system-generated securely by os.CreateTemp
		if err != nil {
			return fmt.Errorf("failed to open vacuumed shadow database: %w", err)
		}
		defer func() {
			_ = shadowFile.Close()
		}()

		if _, err := io.Copy(w, shadowFile); err != nil {
			return fmt.Errorf("failed to stream shadow database: %w", err)
		}

		return nil
	}()

	if err == nil {
		return nil
	}

	// Fallback path: copy locked database to a temp file in targetDir first, verify it, then stream it
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		tempFile, err := os.CreateTemp(targetDir, "emdbmgr_sqlite_fallback_*.db")
		if err != nil {
			return fmt.Errorf("failed to create temporary SQLite copy: %w", err)
		}
		tempPath := tempFile.Name()
		_ = tempFile.Close()

		if err := copyFile(srcPath, tempPath); err != nil {
			_ = os.Remove(tempPath)
			lastErr = fmt.Errorf("failed to copy locked SQLite file: %w", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Verify the copied SQLite database integrity
		if err := verifySQLiteDB(tempPath); err != nil {
			_ = os.Remove(tempPath)
			lastErr = fmt.Errorf("integrity check failed for copied SQLite (attempt %d): %w", attempt, err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Stream the copied DB file directly to w since it is already a transactionally self-contained copy
		copiedFile, err := os.Open(tempPath) // #nosec G304
		if err != nil {
			_ = os.Remove(tempPath)
			lastErr = fmt.Errorf("failed to open staged SQLite copy: %w", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		_, err = io.Copy(w, copiedFile)
		_ = copiedFile.Close()
		_ = os.Remove(tempPath)
		if err != nil {
			return fmt.Errorf("failed to stream staged SQLite copy: %w", err)
		}
		return nil
	}

	return fmt.Errorf("all SQLite copy attempts failed. Last error: %w", lastErr)
}

// BackupSQLiteJSON dumps the active SQLite database as a JSON structure, streaming directly to w
func BackupSQLiteJSON(srcPath, targetDir string, w io.Writer) error {
	// First, attempt to run the JSON backup on the active file
	err := runSQLiteJSONBackup(srcPath, w)
	if err == nil {
		return nil
	}

	// Fallback path: Copy the locked SQLite DB to a temp file in targetDir, verify it, then perform JSON backup from that copy
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		tempFile, err := os.CreateTemp(targetDir, "emdbmgr_sqlite_json_fallback_*.db")
		if err != nil {
			return fmt.Errorf("failed to create temporary SQLite copy for JSON backup: %w", err)
		}
		tempPath := tempFile.Name()
		_ = tempFile.Close()

		if err := copyFile(srcPath, tempPath); err != nil {
			_ = os.Remove(tempPath)
			lastErr = fmt.Errorf("failed to copy locked SQLite file for JSON backup: %w", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Verify the copied SQLite database integrity
		if err := verifySQLiteDB(tempPath); err != nil {
			_ = os.Remove(tempPath)
			lastErr = fmt.Errorf("integrity check failed for copied SQLite for JSON backup (attempt %d): %w", attempt, err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Perform JSON backup from the verified copy
		err = runSQLiteJSONBackup(tempPath, w)
		_ = os.Remove(tempPath)
		if err != nil {
			return fmt.Errorf("failed to perform JSON backup from staged SQLite copy: %w", err)
		}
		return nil
	}

	return fmt.Errorf("all SQLite copy attempts for JSON backup failed. Last error: %w", lastErr)
}

// runSQLiteJSONBackup is the core JSON backup generator logic
func runSQLiteJSONBackup(srcPath string, w io.Writer) error {
	// Open in read-only mode
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=ro", srcPath))
	if err != nil {
		return fmt.Errorf("failed to open source sqlite db: %w", err)
	}
	defer func() {
		_ = db.Close()
	}()

	// 1. Get all user-defined tables
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		return fmt.Errorf("failed to query table names: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, name)
	}
	_ = rows.Close()

	// Write JSON header
	_, err = io.WriteString(w, `{"type":"sqlite3","tables":{`)
	if err != nil {
		return err
	}

	// 2. Stream each table's rows as JSON
	for i, tableName := range tables {
		if strings.ContainsAny(tableName, "\"\n\r\t;") {
			return fmt.Errorf("dangerous table name detected in schema: %q", tableName)
		}
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}

		// Write table name header
		tableHeader := fmt.Sprintf("%q:[", tableName)
		if _, err := io.WriteString(w, tableHeader); err != nil {
			return err
		}

		// Fetch rows
		// #nosec G201
		// nosemgrep
		tableRows, err := db.Query(fmt.Sprintf("SELECT * FROM %q", tableName))
		if err != nil {
			return fmt.Errorf("failed to query table %s: %w", tableName, err)
		}

		cols, err := tableRows.Columns()
		if err != nil {
			_ = tableRows.Close()
			return fmt.Errorf("failed to get columns for table %s: %w", tableName, err)
		}

		rowIdx := 0
		for tableRows.Next() {
			if rowIdx > 0 {
				if _, err := io.WriteString(w, ","); err != nil {
					_ = tableRows.Close()
					return err
				}
			}

			// Prepare interface scanning slice
			scanArgs := make([]any, len(cols))
			vals := make([]any, len(cols))
			for j := range vals {
				scanArgs[j] = &vals[j]
			}

			if err := tableRows.Scan(scanArgs...); err != nil {
				_ = tableRows.Close()
				return fmt.Errorf("failed to scan row in table %s: %w", tableName, err)
			}

			// Map column to values
			rowMap := make(map[string]any)
			for j, colName := range cols {
				val := vals[j]

				// Inspect byte slices for non-UTF8 binary data
				if bytesVal, ok := val.([]byte); ok {
					if utf8.Valid(bytesVal) {
						rowMap[colName] = string(bytesVal)
					} else {
						rowMap[colName] = "base64:" + base64.StdEncoding.EncodeToString(bytesVal)
					}
				} else {
					rowMap[colName] = val
				}
			}

			// Encode this row directly into our streaming writer
			rowData, err := json.Marshal(rowMap)
			if err != nil {
				_ = tableRows.Close()
				return fmt.Errorf("failed to marshal row in table %s: %w", tableName, err)
			}

			if _, err := w.Write(rowData); err != nil {
				_ = tableRows.Close()
				return err
			}
			rowIdx++
		}
		_ = tableRows.Close()

		// Close table array
		if _, err := io.WriteString(w, "]"); err != nil {
			return err
		}
	}

	// Close JSON structure
	if _, err := io.WriteString(w, "}}"); err != nil {
		return err
	}

	return nil
}

// verifySQLiteDB performs an integrity check on a SQLite database file using PRAGMA integrity_check
func verifySQLiteDB(path string) error {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=ro", path))
	if err != nil {
		return fmt.Errorf("failed to open for integrity check: %w", err)
	}
	defer func() {
		_ = db.Close()
	}()

	var result string
	err = db.QueryRow("PRAGMA integrity_check").Scan(&result)
	if err != nil {
		return fmt.Errorf("integrity check query failed: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("SQLite database is corrupted: %s", result)
	}
	return nil
}
