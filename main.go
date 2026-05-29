// Package main is the entrypoint for emdbmgr, the Embedded Database Manager.
//
// Objectives:
//  1. CLI Routing and Verification: Parse and validate CLI flags, ensuring appropriate flags
//     are provided based on the operational mode (version query, structure detection, or backup execution).
//  2. Automagic Format Auto-Detection: Eliminate manual user inputs for database type identification by
//     routing through the binary signature detector during backup staging.
//  3. Structured Standard Outputs: Guarantee that all outputs (errors, metadata info, backup summaries)
//     are serialized and output to stderr/stdout in standardized, machine-readable JSON format.
//
// Core Components:
//   - main(): Enforces argument syntax, performs route switching, coordinates sequential executions of
//     individual backup modes (db, json, or both), and formats the final run report.
//   - printErrorAndExit(): Formats program abort conditions as structured JSON errors written directly to os.Stderr.
//   - printJSONResults(): Encodes single or multi-action backup results arrays directly to os.Stdout.
//
// Functionality and Data Flows:
//   - Detection Path:
//     CLI Flag (-detectdb) -> DetectDB() -> Parse Binary Magic -> Print DBInfo JSON -> os.Stdout
//   - Backup Orchestration Path:
//     CLI Flag (-backup) -> DetectDB() -> Validate DBType -> Parse Paths & Base Names -> Suffix
//     Generation -> Loop Modes -> Build backupFunc closure -> CreateTarZstdArchive() -> printJSONResults() -> os.Stdout
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var version = "development"

func main() {
	// Define CLI Flags
	showVersion := flag.Bool("version", false, "Print version information and exit")
	detectdb := flag.Bool("detectdb", false, "Detect type and metadata of the database")
	backupOpt := flag.String("backup", "", "Backup option: db, json, or json,db")
	sourcedbPath := flag.String("sourcedb-path", "", "Path to the active source database")
	targetdataPath := flag.String("targetdata-path", "", "Path to the backup target folder")

	flag.Parse()

	// 0. Version Flag Check
	if *showVersion {
		fmt.Printf("Embedded Database Manager - v%s\n", version)
		return
	}

	// 1. Validation & Detect Database Mode
	if *detectdb {
		if *sourcedbPath == "" {
			printErrorAndExit("Missing required flag: -sourcedb-path", 1)
		}
		if err := validateSourceFile(*sourcedbPath); err != nil {
			printErrorAndExit(err.Error(), 1)
		}
		info, err := DetectDB(*sourcedbPath)
		if err != nil {
			printErrorAndExit(err.Error(), 1)
		}
		if err := PrintDBInfoJSON(info); err != nil {
			printErrorAndExit(fmt.Sprintf("Failed to encode JSON: %v", err), 1)
		}
		return
	}

	// 2. Backup Mode Execution
	if *backupOpt != "" {
		if *sourcedbPath == "" {
			printErrorAndExit("Missing required flag: -sourcedb-path", 1)
		}
		if *targetdataPath == "" {
			printErrorAndExit("Missing required flag: -targetdata-path", 1)
		}
		if err := validateSourceFile(*sourcedbPath); err != nil {
			printErrorAndExit(err.Error(), 1)
		}

		// Dynamically auto-detect DB type to save user from specifying it!
		info, err := DetectDB(*sourcedbPath)
		if err != nil {
			printErrorAndExit(fmt.Sprintf("Database detection failed: %v", err), 1)
		}

		// Extract original DB name (e.g. sftpgo.db -> sftpgo)
		srcBase := filepath.Base(*sourcedbPath)
		srcName := strings.TrimSuffix(srcBase, filepath.Ext(srcBase))

		// Map internal type to friendly suffix
		mappedType := "unknown"
		switch info.DBType {
		case "sqlite3":
			mappedType = "sqlite"
		case "boldb":
			mappedType = "bolt"
		}

		// Generate YYYYMMDDHHMMSS timestamp
		timestamp := time.Now().Format("20060102150405")

		modes := strings.Split(*backupOpt, ",")
		var results []*BackupResult

		for _, mode := range modes {
			mode = strings.TrimSpace(strings.ToLower(mode))
			var destPath string
			var innerFileName string
			var backupFunc func(w io.Writer) error

			// Inner filename schema: backup_<srcName>_<mappedType>.<ext>
			ext := "db"
			if mode == "json" {
				ext = "json"
			}
			innerFileName = fmt.Sprintf("backup_%s_%s.%s", srcName, mappedType, ext)

			// Outer filename schema: backup_<srcName>_<mappedType>-<timestamp>.<mode>.tar.zst
			destPath = filepath.Join(*targetdataPath, fmt.Sprintf("backup_%s_%s-%s.%s.tar.zst", srcName, mappedType, timestamp, mode))

			switch mode {
			case "db":
				switch info.DBType {
				case "sqlite3":
					backupFunc = func(w io.Writer) error {
						return BackupSQLiteDB(*sourcedbPath, w)
					}
				case "boldb":
					backupFunc = func(w io.Writer) error {
						return BackupBoltDB(*sourcedbPath, w)
					}
				default:
					printErrorAndExit(fmt.Sprintf("Unsupported DB type for backup: %s", info.DBType), 1)
				}
			case "json":
				switch info.DBType {
				case "sqlite3":
					backupFunc = func(w io.Writer) error {
						return BackupSQLiteJSON(*sourcedbPath, w)
					}
				case "boldb":
					backupFunc = func(w io.Writer) error {
						return BackupBoltJSON(*sourcedbPath, w)
					}
				default:
					printErrorAndExit(fmt.Sprintf("Unsupported DB type for backup: %s", info.DBType), 1)
				}
			default:
				printErrorAndExit(fmt.Sprintf("Invalid backup mode specified: %q. Must be 'db', 'json', or 'json,db'", mode), 1)
			}

			// Run backup with streaming Tar + Zstd + on-the-fly xxHash
			result, err := CreateTarZstdArchive(destPath, innerFileName, info, backupFunc)
			if err != nil {
				printErrorAndExit(fmt.Sprintf("Backup failed for mode %s: %v", mode, err), 1)
			}
			result.BackupType = mode
			results = append(results, result)
		}

		// Print JSON report to stdout
		printJSONResults(results)
		return
	}

	// Default: Show CLI Help if no valid flags given
	fmt.Fprintf(os.Stderr, "emdbmgr - High Performance SQLite/BoltDB Op Manager\n\n")
	flag.Usage()
	os.Exit(1)
}

func printErrorAndExit(msg string, code int) {
	errResult := map[string]string{
		"status": "error",
		"error":  msg,
	}
	enc := json.NewEncoder(os.Stderr)
	enc.SetIndent("", "  ")
	_ = enc.Encode(errResult)
	os.Exit(code)
}

func printJSONResults(results []*BackupResult) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	if len(results) == 1 {
		_ = enc.Encode(results[0])
	} else {
		_ = enc.Encode(results)
	}
}

func validateSourceFile(path string) error {
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("source database path does not exist: %s", path)
		}
		return fmt.Errorf("failed to access source database path: %w", err)
	}
	if stat.IsDir() {
		return fmt.Errorf("source database path is a directory, not a regular file: %s", path)
	}
	// Try to open it read-only to verify read permission
	// #nosec G304 -- Path is a user-controlled parameter, cleaned using filepath.Clean.
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("source database is not readable: %w", err)
	}
	_ = f.Close()
	return nil
}
