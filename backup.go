// Package main provides the core backup orchestration structures.
// This module specifically coordinates the streaming archive pipeline (tar.zst)
// and handles on-the-fly checksum hashing and metadata compilation.
package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"
)

// HashingWriter intercepts writes to calculate the xxHash on the fly.
//
// Objectives:
// 1. Act as a pass-through structural writer (io.Writer).
// 2. Feed written byte chunks into an active xxhash digest engine.
// 3. Compute checksums with zero latency or uncompressed memory buffers.
type HashingWriter struct {
	w io.Writer
	h *xxhash.Digest
}

func NewHashingWriter(w io.Writer) *HashingWriter {
	return &HashingWriter{
		w: w,
		h: xxhash.New(),
	}
}

func (hw *HashingWriter) Write(p []byte) (int, error) {
	n, err := hw.w.Write(p)
	if n > 0 {
		_, _ = hw.h.Write(p[:n]) // xxhash Write never returns an error
	}
	return n, err
}

func (hw *HashingWriter) Sum64() uint64 {
	return hw.h.Sum64()
}

// BackupResult represents the JSON structure returned after a successful backup.
// It is written to stdout as a formal report upon command completion.
type BackupResult struct {
	BackupType string `json:"backup_type"` // "db", "json", or "json,db"
	TargetPath string `json:"target_path"`
	DataHash   string `json:"data_xxh64"`
	DurationMS int64  `json:"duration_ms"`
	Status     string `json:"status"`
}

// BackupMetadata details the backup execution for auditing and restoration.
// It is serialized as "metadata.json" inside the tar archive and records
// environmental context, host specifications, original DB specs, and hashes.
type BackupMetadata struct {
	ToolName          string `json:"tool_name"`
	BackupTimestamp   string `json:"backup_timestamp"`
	DurationMS        int64  `json:"duration_ms"`
	DataHash          string `json:"data_xxh64"`
	SourceDBPath      string `json:"source_db_path"`
	SourceDBSizeBytes int64  `json:"source_db_size_bytes"`
	DetectedDBType    string `json:"detected_db_type"`
	DBSpecs           any    `json:"db_specs"`
	BackupFileName    string `json:"backup_file_name"`
	BackupFileSize    int64  `json:"backup_file_size_bytes"`
	Hostname          string `json:"hostname"`
	Platform          string `json:"platform"`
}

// CreateTarZstdArchive sets up the streaming pipeline for tar and zstd compression.
//
// Objectives:
// 1. Establish a single, compact, transportable, and self-verifying backup package (.tar.zst).
// 2. Stream database records (database pages or JSON segments) directly to disk.
// 3. Resolve the size requirements of the Tar Header standard portably using temp files.
//
// Data Flow and Sequences:
// 1. Initialize target archive file and wrap it sequentially: os.Create -> zstd.Writer -> tar.Writer.
// 2. Create a local temporary file to stream database backup without holding gigabytes in RAM.
// 3. Wrap the temporary file in HashingWriter and run the database backup function.
// 4. Retrieve uncompressed size, flush temp file, and seek back to starting offset.
// 5. Append backup data file entry (`innerFileName`) inside the tar archive.
// 6. Assemble operational metrics (hostname, sizes, timestamps) into BackupMetadata.
// 7. Write "metadata.json" directly inside the tar archive.
// 8. Close and flush all writers to finalize the .tar.zst archive.
func CreateTarZstdArchive(destPath, innerFileName string, dbInfo *DBInfo, backupFunc func(w io.Writer) error) (*BackupResult, error) {
	startTime := time.Now()

	// Ensure the parent directory exists
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create target directories: %w", err)
	}

	// 1. Create target archive file atomically by writing to a temporary file in the same directory first
	tmpDestPath := destPath + ".tmp"
	// #nosec G304 -- Path is a user-controlled backup target parameter, cleaned using filepath.Clean.
	archiveFile, err := os.Create(filepath.Clean(tmpDestPath))
	if err != nil {
		return nil, fmt.Errorf("failed to create backup file: %w", err)
	}

	var success bool
	defer func() {
		_ = archiveFile.Close()
		if !success {
			_ = os.Remove(tmpDestPath)
		}
	}()

	// Wrap in a high-speed buffered writer (128 KB buffer) to minimize system write calls
	bufWriter := bufio.NewWriterSize(archiveFile, 128*1024)

	// 2. Set up Zstandard encoder with multi-core parallelism
	zstdWriter, err := zstd.NewWriter(
		bufWriter,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(runtime.NumCPU()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize zstd writer: %w", err)
	}
	defer func() {
		_ = zstdWriter.Close()
	}()

	// 3. Set up Tar writer
	tarWriter := tar.NewWriter(zstdWriter)
	defer func() {
		_ = tarWriter.Close()
	}()

	// 4. Set up the hybrid spill-to-disk staging buffer
	hwData, err := NewHybridWriter(32 * 1024 * 1024) // Stage in RAM up to 32 MB
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = hwData.Close()
	}()

	// Stream database to the staging buffer while calculating xxHash
	hw := NewHashingWriter(hwData)
	if err := backupFunc(hw); err != nil {
		return nil, err
	}

	tempFileSize := hwData.Size()
	tempReader, err := hwData.Reader()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve backup reader: %w", err)
	}

	// Get computed hash
	checksumValue := hw.Sum64()
	checksumStr := fmt.Sprintf("%016x\n", checksumValue)
	durationMS := time.Since(startTime).Milliseconds()

	// 5. Write data file to Tar
	dataHeader := &tar.Header{
		Name:    innerFileName,
		Size:    tempFileSize,
		Mode:    0o600,
		ModTime: time.Now(),
	}
	if err := tarWriter.WriteHeader(dataHeader); err != nil {
		return nil, fmt.Errorf("failed to write tar header for data: %w", err)
	}

	if _, err := io.Copy(tarWriter, tempReader); err != nil {
		return nil, fmt.Errorf("failed to copy data to tar archive: %w", err)
	}

	// 6. Write metadata.json file to Tar
	hostname, _ := os.Hostname()
	var dbSpecs any
	switch dbInfo.DBType {
	case "sqlite3":
		dbSpecs = dbInfo.SQLiteDetails
	case "boldb":
		dbSpecs = dbInfo.BoltDetails
	}

	meta := BackupMetadata{
		ToolName:          "emdbmgr",
		BackupTimestamp:   time.Now().Format(time.RFC3339),
		DurationMS:        durationMS,
		DataHash:          strings.TrimSpace(checksumStr),
		SourceDBPath:      dbInfo.FilePath,
		SourceDBSizeBytes: dbInfo.FileSize,
		DetectedDBType:    dbInfo.DBType,
		DBSpecs:           dbSpecs,
		BackupFileName:    innerFileName,
		BackupFileSize:    tempFileSize,
		Hostname:          hostname,
		Platform:          fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}

	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal backup metadata: %w", err)
	}

	metaHeader := &tar.Header{
		Name:    "metadata.json",
		Size:    int64(len(metaBytes)),
		Mode:    0o600,
		ModTime: time.Now(),
	}
	if err := tarWriter.WriteHeader(metaHeader); err != nil {
		return nil, fmt.Errorf("failed to write tar header for metadata: %w", err)
	}

	if _, err := tarWriter.Write(metaBytes); err != nil {
		return nil, fmt.Errorf("failed to write metadata file to tar: %w", err)
	}

	// Explicitly close the writers to flush them to disk before return
	if err := tarWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close tar writer: %w", err)
	}
	if err := zstdWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close zstd writer: %w", err)
	}
	if err := bufWriter.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush buffer writer: %w", err)
	}
	if err := archiveFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close archive file: %w", err)
	}

	// Atomically commit the backup target rename (guaranteed partition safe!)
	if err := os.Rename(tmpDestPath, destPath); err != nil {
		return nil, fmt.Errorf("failed to atomically commit backup file: %w", err)
	}
	success = true

	return &BackupResult{
		TargetPath: destPath,
		DataHash:   strings.TrimSpace(checksumStr),
		DurationMS: durationMS,
		Status:     "success",
	}, nil
}

// HybridWriter stages writes in memory up to maxMemory, then automatically spills to a local temp file.
type HybridWriter struct {
	maxMemory int
	buf       *bytes.Buffer
	file      *os.File
	isDisk    bool
}

func NewHybridWriter(maxMemory int) (*HybridWriter, error) {
	return &HybridWriter{
		maxMemory: maxMemory,
		buf:       new(bytes.Buffer),
	}, nil
}

func (hw *HybridWriter) Write(p []byte) (int, error) {
	if hw.isDisk {
		return hw.file.Write(p)
	}
	if hw.buf.Len()+len(p) > hw.maxMemory {
		// Spill over to temporary disk file
		var err error
		// #nosec G304 -- Staging scratch file generated securely inside OS temp directory
		hw.file, err = os.CreateTemp("", "emdbmgr_spill_*.tmp")
		if err != nil {
			return 0, fmt.Errorf("failed to create spill-over temp file: %w", err)
		}
		hw.isDisk = true

		// Write existing buffer contents to temp file
		if _, err := hw.buf.WriteTo(hw.file); err != nil {
			_ = hw.file.Close()
			_ = os.Remove(hw.file.Name())
			return 0, fmt.Errorf("failed to spill existing buffer to disk: %w", err)
		}
		hw.buf = nil // Free memory buffer

		return hw.file.Write(p)
	}
	return hw.buf.Write(p)
}

func (hw *HybridWriter) Size() int64 {
	if hw.isDisk {
		stat, err := hw.file.Stat()
		if err != nil {
			return 0
		}
		return stat.Size()
	}
	return int64(hw.buf.Len())
}

func (hw *HybridWriter) Reader() (io.Reader, error) {
	if hw.isDisk {
		if _, err := hw.file.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		return hw.file, nil
	}
	return bytes.NewReader(hw.buf.Bytes()), nil
}

func (hw *HybridWriter) Close() error {
	if hw.isDisk && hw.file != nil {
		name := hw.file.Name()
		_ = hw.file.Close()
		_ = os.Remove(name)
	}
	return nil
}
