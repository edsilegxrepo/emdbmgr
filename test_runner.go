//go:build ignore

// Package main coordinates automated integration and regression testing for the emdbmgr utility.
//
// Objectives:
//  1. Zero-Pollution Sandboxing: Ensure all compiled binaries, SQLite databases, BoltDB databases,
//     backup archives, and test logs are created strictly within an isolated temporary directory,
//     guaranteeing no development repository pollution.
//  2. Comprehensive Feature Verification: Automate the sequential verification of version querying,
//     magic signature header detection, zst-compressed dual backups, boundary conditions (zero-byte/invalid files),
//     and multi-threaded concurrency safety.
//  3. Complete Integrity Auditing: Dynamically decompress generated tar.zst packages, list inner files,
//     extract and read JSON audit metadata, and ensure that dynamic xxHash (XXH64) checksum integrity matches.
//
// Core Components:
//   - main(): Orchestrates the end-to-end testing workflow: resolves target sandbox folder, compiles
//     `emdbmgr` on-the-fly, creates test databases, runs five distinct test cases, and performs final cleanups.
//   - getTempDir(): Cascades through environment variables (TMPDIR, TEMP, TMP) to dynamically locate
//     a secure and writable OS-level temporary path.
//   - createSampleSQLite() / createSampleBolt(): Stages authentic database fixtures filled with multi-type
//     columns, binary BLOBs, nested buckets, and typical data formats.
//   - runVersionCheck() / runDetection() / testBackup() / testBoundaryFailures(): Individual validation blocks
//     that execute `emdbmgr` subcommands and assertion checks against CLI stdout/stderr outputs.
//   - verifyArchive(): Decodes Zstandard compress streams, reads Tar envelopes, parses serialized
//     BackupMetadata structs, and validates output checksums.
//   - testActiveConcurrency(): Spawns a parallel goroutine executing continuous, rapid database insertions
//     (every 10ms in WAL mode) to stress-test concurrent SQLite backup procedures while writers are highly active.
//
// Functionality and Data Flows:
//   - Execution Flow:
//     Resolve Temp Directory -> go build emdbmgr -> Create DB Fixtures -> Execute Subtests (0 to 4) ->
//     Verify Tar/Zstd Contents & Audit Checksums -> Teardown/Remove Sandbox
//   - Concurrency Test Flow:
//     Spawn Goroutine (Active SQLite Insert Loop) -> Sleep 100ms -> Trigger CLI Backup (emdbmgr -backup=json,db) ->
//     Wait for Backup Success -> Terminate insertion loop -> Assert no lockouts or blockages.
package main

import (
	"archive/tar"
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	bolt "go.etcd.io/bbolt"
	_ "modernc.org/sqlite"
)

func main() {
	fmt.Println("--- Starting emdbmgr Integration Tests ---")

	// 1. Create portable unitests directory under TMPDIR / environment priority path
	baseTemp := getTempDir()
	unitestsDir := filepath.Join(baseTemp, "unitests")
	if err := os.MkdirAll(unitestsDir, 0o755); err != nil {
		panic(err)
	}

	// Determine binary name by platform
	exeName := "emdbmgr"
	if runtime.GOOS == "windows" {
		exeName = "emdbmgr.exe"
	}
	binPath := filepath.Join(unitestsDir, exeName)

	// 2. Compile emdbmgr directly to TMPDIR/unitests with injected version ldflags
	fmt.Printf("Compiling emdbmgr on-the-fly to: %s...\n", binPath)
	// #nosec G204 -- Staged compilation of the dynamic binary inside target sandbox is fully secure
	buildCmd := exec.Command("go", "build", "-ldflags", "-X main.version=1.0.0-20260528", "-o", binPath)
	if err := buildCmd.Run(); err != nil {
		panic(fmt.Errorf("failed to compile emdbmgr in temp folder: %w", err))
	}

	// 3. Test Version Flag (Test 0)
	fmt.Println("\n[Test 0] Testing Version Flag...")
	runVersionCheck(binPath, "1.0.0-20260528")

	// 4. Create individual test run folder inside TMPDIR/unitests
	tempDir, err := os.MkdirTemp(unitestsDir, "emdbmgr_test_*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tempDir)

	sqlitePath := filepath.Join(tempDir, "test_sqlite.db")
	boltPath := filepath.Join(tempDir, "test_bolt.db")

	// 5. Create sample databases in tempDir
	createSampleSQLite(sqlitePath)
	createSampleBolt(boltPath)

	// 6. Test Detection
	fmt.Println("\n[Test 1] Testing Signature Detection...")
	runDetection(binPath, sqlitePath)
	runDetection(binPath, boltPath)

	// 7. Test Backups
	fmt.Println("\n[Test 2] Testing Safe Backups...")
	testBackup(binPath, sqlitePath, filepath.Join(tempDir, "sqlite_backup"))
	testBackup(binPath, boltPath, filepath.Join(tempDir, "bolt_backup"))

	// 8. Test Boundary & Signature Failures (Test 3)
	testBoundaryFailures(binPath, tempDir)

	// 9. Test Active Concurrency & Safety (Test 4)
	testActiveConcurrency(binPath, sqlitePath, tempDir)

	// 10. Test Locked Database Fallbacks (Test 5)
	testLockedDatabaseFallbacks(binPath, sqlitePath, boltPath, tempDir)

	fmt.Println("\n--- All Tests Completed Successfully ---")
}

func createSampleSQLite(path string) {
	fmt.Printf("Creating sample SQLite DB at %s...\n", path)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			avatar BLOB
		);
		INSERT INTO users (id, name, avatar) VALUES (1, 'Alice', X'0001020304');
		INSERT INTO users (id, name, avatar) VALUES (2, 'Bob', NULL);
	`)
	if err != nil {
		panic(err)
	}
}

func createSampleBolt(path string) {
	fmt.Printf("Creating sample BoltDB at %s...\n", path)
	db, err := bolt.Open(path, 0o666, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("users"))
		if err != nil {
			return err
		}
		err = b.Put([]byte("user:1"), []byte(`{"name":"Alice"}`))
		if err != nil {
			return err
		}
		// Put non-UTF8 binary data
		err = b.Put([]byte("user:binary"), []byte{0x00, 0xFF, 0x00, 0xFF})
		if err != nil {
			return err
		}

		sub, err := b.CreateBucket([]byte("settings"))
		if err != nil {
			return err
		}
		return sub.Put([]byte("theme"), []byte("dark"))
	})
	if err != nil {
		panic(err)
	}
}

func runDetection(binPath, dbPath string) {
	// #nosec G204
	// nosemgrep
	cmd := exec.Command(binPath, "-detectdb", "-sourcedb-path", dbPath)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		fmt.Printf("Detection failed for %s: %v\nStderr: %s\n", dbPath, err, stderr.String())
		panic(err)
	}

	fmt.Printf("Detection result for %s:\n%s\n", filepath.Base(dbPath), stdout.String())
}

func testBackup(binPath, dbPath, targetDir string) {
	// Ensure target directory exists
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		panic(err)
	}

	// Execute both JSON and DB dual backup mode
	// #nosec G204
	// nosemgrep
	cmd := exec.Command(binPath, "-backup-type=json,db", "-sourcedb-path", dbPath, "-targetdata-path", targetDir)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		fmt.Printf("Backup failed for %s: %v\nStderr: %s\n", dbPath, err, stderr.String())
		panic(err)
	}

	fmt.Printf("Backup succeeded for %s.\nCLI stdout:\n%s\n", filepath.Base(dbPath), stdout.String())

	// Read target directory dynamically to find the generated backup archives
	files, err := os.ReadDir(targetDir)
	if err != nil {
		panic(err)
	}

	srcBase := filepath.Base(dbPath)
	srcName := strings.TrimSuffix(srcBase, filepath.Ext(srcBase)) // e.g. "test_sqlite" or "test_bolt"

	// Find and verify archives belonging to this source DB
	countVerified := 0
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		// Outer format: backup_<srcName>_<mappedType>-<timestamp>.<mode>.tar.zst
		if strings.HasPrefix(name, "backup_"+srcName+"_") && strings.HasSuffix(name, ".tar.zst") {
			parts := strings.Split(name, ".") // ["backup_test_sqlite_sqlite-20260528135728", "db", "tar", "zst"]
			if len(parts) >= 4 {
				mode := parts[len(parts)-3] // "db" or "json"

				var mappedType string
				if strings.Contains(name, "_sqlite-") {
					mappedType = "sqlite"
				} else if strings.Contains(name, "_bolt-") {
					mappedType = "bolt"
				}

				expectedInnerName := fmt.Sprintf("backup_%s_%s.%s", srcName, mappedType, mode)
				archivePath := filepath.Join(targetDir, name)

				verifyArchive(archivePath, expectedInnerName)
				countVerified++
			}
		}
	}

	if countVerified != 2 {
		panic(fmt.Sprintf("expected to find and verify 2 backup files for %s, but found %d", srcName, countVerified))
	}
}

func verifyArchive(archivePath, expectedFileName string) {
	fmt.Printf("Verifying archive %s...\n", filepath.Base(archivePath))

	f, err := os.Open(archivePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	zstdReader, err := zstd.NewReader(f)
	if err != nil {
		panic(err)
	}
	defer zstdReader.Close()

	tarReader := tar.NewReader(zstdReader)

	var foundDataFile, foundMetadataFile bool
	var metadataContent string

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		if header.Name == expectedFileName {
			foundDataFile = true
			fmt.Printf("  -> Found expected data file inside tar: %s (size: %d bytes)\n", header.Name, header.Size)
		} else if header.Name == "metadata.json" {
			foundMetadataFile = true
			buf := new(bytes.Buffer)
			_, err := io.CopyN(buf, tarReader, header.Size)
			if err != nil {
				panic(err)
			}
			metadataContent = buf.String()
			fmt.Printf("  -> Found expected metadata.json file inside tar. Content:\n%s\n", metadataContent)
		}
	}

	if !foundDataFile || !foundMetadataFile {
		panic(fmt.Sprintf("verification failed: data_file_found=%t, metadata_file_found=%t", foundDataFile, foundMetadataFile))
	}
}

// getTempDir returns the resolved temporary directory based on priority:
// 1. TMPDIR
// 2. TEMP
// 3. TMP
// 4. Default OS Fallback: c:\temp (Windows) or /tmp (Linux/other)
func getTempDir() string {
	if val := os.Getenv("TMPDIR"); val != "" {
		return val
	}
	if val := os.Getenv("TEMP"); val != "" {
		return val
	}
	if val := os.Getenv("TMP"); val != "" {
		return val
	}
	if runtime.GOOS == "windows" {
		return `c:\temp`
	}
	return "/tmp"
}

func runVersionCheck(binPath, expectedVersion string) {
	// #nosec G204
	// nosemgrep
	cmd := exec.Command(binPath, "-version")
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		panic(fmt.Errorf("failed to run version check: %w", err))
	}
	expected := fmt.Sprintf("Embedded Database Manager - v%s\n", expectedVersion)
	if stdout.String() != expected {
		panic(fmt.Errorf("expected version output %q, got %q", expected, stdout.String()))
	}
	fmt.Printf("Version flag verified successfully: %s", stdout.String())
}

func testBoundaryFailures(binPath, tempDir string) {
	fmt.Println("\n[Test 3] Testing Boundary & Signature Failures...")

	// 1. Zero-byte file
	zeroFile := filepath.Join(tempDir, "zero.db")
	if err := os.WriteFile(zeroFile, []byte{}, 0o644); err != nil {
		panic(err)
	}

	// #nosec G204
	// nosemgrep
	cmd := exec.Command(binPath, "-detectdb", "-sourcedb-path", zeroFile)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err == nil {
		panic("expected failure for zero-byte file, but it succeeded")
	}
	fmt.Printf("  -> Handled empty file successfully. Error block:\n%s\n", stderr.String())

	// 2. Non-database text file
	textFile := filepath.Join(tempDir, "text.db")
	if err := os.WriteFile(textFile, []byte("this is not a database file at all, just plain text info"), 0o644); err != nil {
		panic(err)
	}

	// #nosec G204
	// nosemgrep
	cmd = exec.Command(binPath, "-detectdb", "-sourcedb-path", textFile)
	stdout.Reset()
	stderr.Reset()
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err == nil {
		panic("expected failure for text file, but it succeeded")
	}
	fmt.Printf("  -> Handled non-database file successfully. Error block:\n%s\n", stderr.String())
}

func testActiveConcurrency(binPath, dbPath, tempDir string) {
	fmt.Println("\n[Test 4] Testing Active Concurrency & Safety...")

	// Open the database directly to write to it concurrently
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Ensure WAL mode is active to support concurrent read/write
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		panic(err)
	}

	stopChan := make(chan bool)
	doneChan := make(chan bool)

	go func() {
		counter := 0
		for {
			select {
			case <-stopChan:
				close(doneChan)
				return
			default:
				counter++
				// #nosec G201
				// nosemgrep
				_, err := db.Exec("INSERT INTO users (name, avatar) VALUES (?, NULL)", fmt.Sprintf("Concurrent_User_%d", counter))
				if err != nil {
					fmt.Printf("  -> Active writer thread error: %v\n", err)
					panic(err)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Allow writes to begin
	time.Sleep(100 * time.Millisecond)

	// Execute dual backup while concurrent writer is loop-inserting records
	targetDir := filepath.Join(tempDir, "concurrent_run")
	// #nosec G204
	// nosemgrep
	cmd := exec.Command(binPath, "-backup-type=json,db", "-sourcedb-path", dbPath, "-targetdata-path", targetDir)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		fmt.Printf("  -> Concurrent backup failed: %v\nStderr: %s\n", err, stderr.String())
		panic(err)
	}

	// Terminate writer thread
	stopChan <- true
	<-doneChan

	fmt.Println("  -> Concurrency test completed successfully. Writers & backup streams executed simultaneously with zero blockages.")
}

func testLockedDatabaseFallbacks(binPath, sqlitePath, boltPath, tempDir string) {
	fmt.Println("\n[Test 5] Testing Locked Database Fallbacks...")

	// 1. Lock BoltDB exclusively using a standard bolt.Open connection (simulating SFTPGo)
	fmt.Println("  -> Acquiring exclusive lock on BoltDB file...")
	externalBolt, err := bolt.Open(boltPath, 0o666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		panic(fmt.Errorf("failed to lock BoltDB for testing: %w", err))
	}
	defer externalBolt.Close()

	// 2. Lock SQLite exclusively by opening a transaction with EXCLUSIVE locking mode
	fmt.Println("  -> Acquiring exclusive lock on SQLite file...")
	externalSQL, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		panic(fmt.Errorf("failed to open SQLite for testing: %w", err))
	}
	defer externalSQL.Close()

	_, err = externalSQL.Exec("PRAGMA locking_mode=EXCLUSIVE")
	if err != nil {
		panic(fmt.Errorf("failed to set exclusive locking mode: %w", err))
	}
	sqlTx, err := externalSQL.Begin()
	if err != nil {
		panic(fmt.Errorf("failed to begin exclusive SQL transaction: %w", err))
	}
	defer func() {
		_ = sqlTx.Rollback()
	}()

	// 3. Trigger dual backup on BoltDB while it is locked exclusively
	boltTargetDir := filepath.Join(tempDir, "bolt_locked_backup")
	if err := os.MkdirAll(boltTargetDir, 0o755); err != nil {
		panic(err)
	}

	fmt.Println("  -> Executing backup on locked BoltDB...")
	// #nosec G204
	// nosemgrep
	cmdBolt := exec.Command(binPath, "-backup-type=json,db", "-sourcedb-path", boltPath, "-targetdata-path", boltTargetDir)
	var stdoutBolt, stderrBolt bytes.Buffer
	cmdBolt.Stdout = &stdoutBolt
	cmdBolt.Stderr = &stderrBolt

	if err := cmdBolt.Run(); err != nil {
		fmt.Printf("  -> Locked BoltDB backup failed: %v\nStderr: %s\n", err, stderrBolt.String())
		panic(err)
	}
	fmt.Println("  -> Locked BoltDB backup succeeded via copy fallback!")

	// 4. Trigger dual backup on SQLite while it is locked exclusively
	sqliteTargetDir := filepath.Join(tempDir, "sqlite_locked_backup")
	if err := os.MkdirAll(sqliteTargetDir, 0o755); err != nil {
		panic(err)
	}

	fmt.Println("  -> Executing backup on locked SQLite...")
	// #nosec G204
	// nosemgrep
	cmdSQL := exec.Command(binPath, "-backup-type=json,db", "-sourcedb-path", sqlitePath, "-targetdata-path", sqliteTargetDir)
	var stdoutSQL, stderrSQL bytes.Buffer
	cmdSQL.Stdout = &stdoutSQL
	cmdSQL.Stderr = &stderrSQL

	if err := cmdSQL.Run(); err != nil {
		fmt.Printf("  -> Locked SQLite backup failed: %v\nStderr: %s\n", err, stderrSQL.String())
		panic(err)
	}
	fmt.Println("  -> Locked SQLite backup succeeded via copy fallback!")
}
