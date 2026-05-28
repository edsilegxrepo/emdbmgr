// Package main provides the core routines for emdbmgr, the Embedded Database Manager.
// This module specifically implements format agnosticism and metadata extraction logic.
package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// DBInfo represents the universal JSON output structure for detection.
// It acts as the primary transfer object between the signature inspector
// and the CLI reporting controller or downstream backup pipelines.
type DBInfo struct {
	FilePath      string      `json:"file_path"`
	FileSize      int64       `json:"file_size_bytes"`
	DBType        string      `json:"db_type"`
	SQLiteDetails *SQLiteInfo `json:"sqlite_details,omitempty"`
	BoltDetails   *BoltInfo   `json:"bolt_details,omitempty"`
}

// SQLiteInfo contains SQLite-specific header metadata parsed directly
// from the first 100 bytes of the database file.
type SQLiteInfo struct {
	MagicMatched  bool   `json:"magic_matched"`
	PageSize      uint16 `json:"page_size"`
	WriteVersion  byte   `json:"write_version"` // 1: Legacy, 2: WAL
	ReadVersion   byte   `json:"read_version"`  // 1: Legacy, 2: WAL
	WALMode       bool   `json:"wal_mode"`
	SchemaVersion uint32 `json:"schema_version"`
	UserVersion   uint32 `json:"user_version"`
}

// BoltInfo contains BoltDB-specific metadata from its meta page.
// The meta structure is read from Page 0 starting at offset 16.
type BoltInfo struct {
	MagicMatched bool   `json:"magic_matched"`
	Version      uint32 `json:"version"`
	PageSize     uint32 `json:"page_size"`
	TxID         uint64 `json:"tx_id"`
	PageCount    uint64 `json:"page_count"` // Last pgid
}

// Operational Signatures & Constants:
// - sqliteMagic: Represents the standard SQLite3 signature "SQLite format 3\0".
// - boltMagic: Represents the BoltDB/bbolt metadata page identifier 0xED0CDAED.
const (
	sqliteMagic = "SQLite format 3\x00"
	boltMagic   = uint32(0xED0CDAED)
)

// DetectDB inspects the file at path and extracts type and metadata.
//
// Objectives:
// 1. Establish format-agnostic inspection using raw binary signature recognition.
// 2. Perform zero-allocation parsing of specific database headers.
//
// Binary Offset Mapping Schemas:
//
// A. SQLite3 Database Header (Offset 0-100 bytes):
// - Offset 0-15: Database magic string ("SQLite format 3\0").
// - Offset 16-17: Page size (16-bit big-endian).
// - Offset 18: File format write version (1: Legacy rollback, 2: WAL).
// - Offset 19: File format read version (1: Legacy rollback, 2: WAL).
// - Offset 40-43: Schema version number (32-bit big-endian).
// - Offset 68-71: User version number (32-bit big-endian).
//
// B. BoltDB Page 0 Meta Header (Offset 0-100 bytes):
// - Offset 0-7: pgid (uint64, ID of the page. Should be 0 for Page 0).
// - Offset 8-9: flags (uint16, 0x0004 for metaPageFlag).
// - Offset 10-11: count (uint16, number of keys).
// - Offset 12-15: overflow (uint32, number of overflow pages).
// - Offset 16-19: magic number (uint32, must match 0xED0CDAED in host-endian format).
// - Offset 20-23: database format version (uint32).
// - Offset 24-27: database page size (uint32).
// - Offset 56-63: total page count/last pgid (uint64).
// - Offset 64-71: last committed transaction ID (uint64).
func DetectDB(path string) (*DBInfo, error) {
	// Open the file with a shared read-only file handle.
	// On Windows, this is configured with share modes to prevent write conflicts.
	// #nosec G304 -- Path is a user-controlled parameter, cleaned using filepath.Clean.
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	info := &DBInfo{
		FilePath: path,
		FileSize: stat.Size(),
		DBType:   "unknown",
	}

	// Validate baseline minimum size requirement (100 bytes header)
	if stat.Size() < 100 {
		return info, errors.New("file is too small to be a valid SQLite or BoltDB database")
	}

	header := make([]byte, 100)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, fmt.Errorf("failed to read file header: %w", err)
	}

	// 1. Try SQLite3 Detection
	if string(header[0:16]) == sqliteMagic {
		info.DBType = "sqlite3"

		// Parse SQLite header details (big-endian as defined by SQLite specification)
		pageSize := binary.BigEndian.Uint16(header[16:18])
		writeVer := header[18]
		readVer := header[19]
		schemaVer := binary.BigEndian.Uint32(header[40:44])
		userVer := binary.BigEndian.Uint32(header[68:72])

		info.SQLiteDetails = &SQLiteInfo{
			MagicMatched:  true,
			PageSize:      pageSize,
			WriteVersion:  writeVer,
			ReadVersion:   readVer,
			WALMode:       writeVer == 2 || readVer == 2,
			SchemaVersion: schemaVer,
			UserVersion:   userVer,
		}
		return info, nil
	}

	// 2. Try BoltDB Detection
	// A BoltDB page 0 starts with standard page header (16 bytes):
	// id (uint64), flags (uint16), count (uint16), overflow (uint32)
	// Followed by meta structure at offset 16.
	// Check the page flags: metaPageFlag is 0x04.
	// Since byte-order matches host system, we need to inspect both little and big endian versions of the magic number.
	var isBolt bool
	var byteOrder binary.ByteOrder = binary.LittleEndian

	magicLE := binary.LittleEndian.Uint32(header[16:20])
	magicBE := binary.BigEndian.Uint32(header[16:20])

	if magicLE == boltMagic {
		isBolt = true
		byteOrder = binary.LittleEndian
	} else if magicBE == boltMagic {
		isBolt = true
		byteOrder = binary.BigEndian
	}

	if isBolt {
		info.DBType = "boldb"

		// Parse Bolt metadata structural details (evaluated via resolved byte-order)
		version := byteOrder.Uint32(header[20:24])
		pageSize := byteOrder.Uint32(header[24:28])
		pageCount := byteOrder.Uint64(header[56:64]) // pgid is offset 56 in meta
		txID := byteOrder.Uint64(header[64:72])      // txid is offset 64 in meta

		info.BoltDetails = &BoltInfo{
			MagicMatched: true,
			Version:      version,
			PageSize:     pageSize,
			TxID:         txID,
			PageCount:    pageCount,
		}
		return info, nil
	}

	return info, errors.New("file format could not be identified as SQLite3 or BoltDB")
}

// PrintDBInfoJSON formats and outputs the DBInfo as JSON to stdout
func PrintDBInfoJSON(info *DBInfo) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(info)
}
