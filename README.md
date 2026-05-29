# Embedded Database Manager (`emdbmgr`) - Technical Specification & User Manual

`emdbmgr` is a cross-platform, high-performance command-line utility written in Go. It is designed to inspect, identify, and perform safe, non-blocking, transactionally consistent backups of SQLite3 and BoltDB databases.

---

## 1. Application Overview and Objectives

The primary objective of `emdbmgr` is to manage embedded database files (SQLite3 and BoltDB) in active production systems without causing locks, writes blockade, or data corruption. 

### Core Operational Objectives:
* **Format Agnosticism via Hex Signature Detection:** Identify the database engine format directly from the file header rather than trusting filename extensions.
* **ACID-Compliant Backups on Open Databases:** Replicate active, open databases without interrupting writers or reading partially written or corrupted transactions.
* **Low System Resource Footprint:** Maintain a $O(1)$ memory consumption profile by streaming backups and compression pipelines, eliminating large intermediate uncompressed RAM buffers.
* **Agnostic and Portable Tooling:** Compile cleanly across multiple target OS/architectures (Windows, Linux, macOS) by enforcing a CGO-free driver design.
* **Self-Verifying Deliverables:** Package the backup and its dynamic operational telemetry (execution duration, platform, sizes, specs, and xxHash integrity checksum) into a single, compact archive.

---

## 2. Architecture and Design Choices

The architecture of `emdbmgr` is built on several key technical decisions:

```mermaid
graph TD
    CLI["emdbmgr CLI Controller"]
    SQLite["SQLite3 Engine Driver (modernc.org/sqlite)"]
    Bolt["BoltDB Engine Driver (go.etcd.io/bbolt)"]
    HW["HashingWriter (io.Writer Wrapper) <br> [xxHash / XXH64]"]
    Tar["tar.Writer <br> (archive/tar)"]
    Zstd["zstd.Writer <br> (zstd stream compressor)"]
    File["[.tar.zst Output Package]"]

    CLI --> SQLite
    CLI --> Bolt
    SQLite --> HW
    Bolt --> HW
    HW --> Tar
    Tar --> Zstd
    Zstd --> File
```

### 2.1 CGO-Free SQLite Driver (`modernc.org/sqlite`)
To ensure complete portability and eliminate external C-runtime dependencies during compilation (which typically require `gcc` on Windows or specialized toolchains on Linux), the application leverages `modernc.org/sqlite`. This is a pure-Go translation of the SQLite C engine. It implements the standard Go `database/sql` interface and matches the performance and stability profile of C-based drivers without CGO compiler overhead.

### 2.2 Concurrency, Security, & Transaction Safety Models
Database file copies executed at the operating system level while a database is actively being written to can lead to "hot journals" or corrupted WAL (Write-Ahead Log) states. `emdbmgr` addresses this through database-specific concurrency and security safety models:
* **SQLite3 Safe Copy (`-backup-type=db`):** Invokes the SQLite `VACUUM INTO` command over a read-only (`mode=ro`) connection. The SQLite engine locks the database in a shared read state, processes page fragmentation, and copies pages directly to a temporary replica file. Active write transactions are not blocked, and the output is transactionally consistent.
* **SQLite3 Data Extraction & Sanitization (`-backup-type=json`):** Queries table structures dynamically and loops through rows sequentially. Table names are dynamically sanitized using strict character validation (`strings.ContainsAny`) to eliminate any risk of SQL injection prior to query formatting, and scanned interfaces are streamed directly as JSON.
* **BoltDB Shared Read Access & Lock Fallback (`-backup-type=db` and `-backup-type=json`):** BoltDB operates a single-writer, multi-reader MVCC (Multi-Version Concurrency Control) model. `emdbmgr` attempts to open BoltDB files in shared read-only mode with a **`1 * time.Second` timeout**. If the lock acquisition fails (e.g. because another process like SFTPGo holds an exclusive write lock on Windows), the tool automatically falls back to **copy-on-read target folder staging** with programmatic consistency verification (`tx.Check()`) and retry loops, ensuring zero write blockage and 100% uncorrupted backups. On success, it launches a read-only transaction (`db.View`):
  * For raw block-level replication (`db`), it executes Bolt's internal `tx.Copy(w)` block copy.
  * For JSON extraction (`json`), it traverses the B+ tree recursively via cursors, streaming key-values directly.
* **Pre-Flight Input Sanitization**: Before passing any file paths to database engines, the CLI explicitly validates that the file exists, is a regular file (not a directory), is readable, and cleans the path using `filepath.Clean`.

#### Summary of Concurrency Safety:

| Operation | DB Format | Concurrency Mechanism | Blocking Behavior on Writers | Data Corruption Risk |
|---|---|---|---|---|
| `-detectdb` | Both | OS-level `os.Open` (with Read-Share) | Zero Blocking | Zero |
| `-backup-type=db` | SQLite3 | Read-only connection + SQL `VACUUM INTO` | Zero Blocking (in WAL mode) | Zero (ACID consistent) |
| `-backup-type=db` | BoltDB | Shared Read-only open + `db.View` + `tx.Copy` | Zero Blocking (MVCC isolated) | Zero (ACID consistent) |
| `-backup-type=json` | SQLite3 | Read-only connection + streaming `SELECT` row scan | Zero Blocking (in WAL mode) | Zero (ACID consistent) |
| `-backup-type=json` | BoltDB | Shared Read-only open + `db.View` + cursor tree scan | Zero Blocking (MVCC isolated) | Zero (ACID consistent) |


### 2.3 Compressed Tar Stream Packaging with On-the-Fly xxHash & Hybrid Staging
The backup engine combines tape archiving (`archive/tar`) and Zstandard compression (`github.com/klauspost/compress/zstd`) in a single streaming pipeline:
* **The HashingWriter Wrapper:** A custom structural writer wraps the file stream, intercepting written bytes to compute a 64-bit **xxHash (XXH64)** on-the-fly (`github.com/cespare/xxhash/v2`) at near-RAM speeds.
* **Redundancy Elimination:** Rather than storing loose, vulnerable `.xxh64` files on disk, the checksum and execution telemetry (file sizes, host platform, durations, specific DB details) are formatted as a JSON document (`metadata.json`) and appended directly inside the `.tar.zst` archive package alongside the backup data.
* **Dynamic Spill-to-Disk Hybrid Buffer (`HybridWriter`):** Staging uncompressed size formats in memory up to **32 MB** in a `bytes.Buffer` for zero-I/O rapid execution. Backups exceeding 32 MB automatically spill to a secure temporary file located **strictly inside the user-specified target backup directory (`-targetdata-path`)**, maintaining low RAM profiles while avoiding global temp folder pollution and cross-volume I/O latency.
* **Partition-Safe Atomic Rename Commit**: To ensure backup integrity, files are written as `.tar.zst.tmp` in the **exact same target directory** (guaranteeing same-partition O(1) atomic filesystem swaps) and renamed to `.tar.zst` only upon full stream flush and close.
* **High-Speed Buffered Writes & Parallelism**: Output streams are buffered using a `128 KB` `bufio.WriterSize` to minimize system write syscalls, and the Zstandard encoder is parallelized across all cores using Go's `runtime.NumCPU()`.

### 2.4 Portable Multi-Platform Testing Strategy
To maintain a strict zero-footprint repository policy during integration and CI/CD tests, `emdbmgr` enforces an isolated testing environment strategy:
* **Compile-on-the-Fly Isolation:** The test suite compiles the source code on-the-fly directly into the resolved temp folder, preventing untracked executables or database files from cluttering the version-controlled repository workspace.
* **Environment-Variable Priority Resolution:** The test runner resolves the baseline temporary directory by evaluating environment variables in a strict sequential cascade:
  1. **`TMPDIR`**
  2. **`TEMP`**
  3. **`TMP`**
  4. **System Fallbacks:** `c:\temp` on Windows systems; `/tmp` on Linux and Unix environments.
* **Dynamic Sandboxed Execution:** All target test databases, timestamped output archives, and extracted verification steps are containerized within a dynamically generated sandboxed subfolder (`unitests/emdbmgr_test_*`) inside the resolved temporary base path, which is cleaned up automatically upon execution exit.

---

## 3. Data Flow and Control Logic

### 3.1 Operational Control Flow
1. **CLI Execution:** Command line flags are validated. If `-version` is set, version info is immediately printed to stdout and the process terminates.
2. **Dynamic Format Detection (`DetectDB`):** The program opens the database in read-only mode and reads the first 100 bytes of the file.
   * If bytes 0–15 match `"SQLite format 3\0"`, the engine is classified as `sqlite3` and SQLite metrics (page size, WAL state, user version, schema version) are extracted.
   * If the first 16 bytes match Bolt's metadata page header flags (`metaPageFlag = 0x04`) and bytes 16–19 match Bolt's system-endian magic number `0xED0CDAED`, the engine is classified as `boldb` and Bolt metrics (version, page size, transaction ID, page count) are extracted.
3. **Backup Routing:** If `-backup-type` is requested, the program routes the detected DB structure into the appropriate driver methods based on the requested output formats (`json`, `db`, or both).
4. **Archive Stream Pipeline:**
   * Opens the target file on disk (`.tar.zst`).
   * Wraps it in a `zstd.Writer` and nests a `tar.Writer` inside it.
   * Runs the dynamic backup stream, intercepting written bytes with `HashingWriter` to compute the xxHash.
   * Generates a structural metadata document (`metadata.json`) containing the dynamic hash and system metrics, and appends it to the archive.
   * Flushes and closes all streams.
5. **Stdout CLI Report:** Emits a formal JSON result block to stdout detailing the backup operation.

### 3.2 System Sequence Diagram
```mermaid
sequenceDiagram
    autonumber
    actor CLI as User/CI Script
    participant Main as CLI Main Controller
    participant Det as detector.go (DetectDB)
    participant Engine as sqlite.go / bolt.go
    participant Back as backup.go (Staging & Archive)
    participant Disk as Target Disk (tar.zst)

    CLI->>Main: Execute emdbmgr with args
    Note over Main: Validate flags & paths
    Main->>Det: DetectDB(sourcedbPath)
    Note over Det: Read first 100 raw bytes
    Det-->>Main: Return DBInfo (sqlite3/boldb + specs)
    
    alt -detectdb flag set
        Main-->>CLI: Print DBInfo JSON to stdout & exit
    else -backup-type flag set
        Main->>Back: CreateTarZstdArchive(targetPath, innerFileName, dbInfo)
        Back->>Disk: Create target archive (.tar.zst)
        Back->>Back: Initialize zstd.Writer & tar.Writer
        
        Back->>Engine: Run BackupFunc(HybridWriter)
        loop Data Staging
            Engine->>Back: Stream bytes (pages / JSON) to Staging Buffer
        end
        Engine-->>Back: Complete staging stream
        
        Note over Back: Systematic Backup Integrity Check <br> (verifyBackupFile: tx.Check / PRAGMA / JSON validation)
        
        alt Integrity Check Fails
            Back-->>Main: Return Validation Error
            Main-->>CLI: Print Error JSON & Exit 1
        else Integrity Check Passes
            loop Archive Compression
                Back->>Disk: Stream verified bytes to Tar (Zstd Compressed)
                Note over Back: Compute xxHash (XXH64) on-the-fly
            end
            Note over Back: Construct metadata.json with XXH64 & specs
            Back->>Disk: Write metadata.json into tar
            
            Note over Back: Flush & Close all writers
            Back-->>Main: Return BackupResult
            Main-->>CLI: Print BackupResult JSON to stdout
        end
    end
```

---

## 4. Dependencies

`emdbmgr` uses minimal external dependencies to ensure reliability and maintain CGO-free compilation:

| Dependency Package | Purpose | CGO Requirement |
|---|---|---|
| `std/archive/tar` | Packaging multiple files (data file + metadata JSON). | None (Go standard library) |
| `std/database/sql` | Abstract SQL connection management. | None (Go standard library) |
| `modernc.org/sqlite` | CGO-free pure-Go SQLite3 database engine driver. | **None** |
| `go.etcd.io/bbolt` | Standard active BoltDB embedded engine. | **None** |
| `github.com/klauspost/compress/zstd` | High-performance, streaming Zstandard compression. | **None** |
| `github.com/cespare/xxhash/v2` | Highly optimized assembly/Go xxHash (XXH64) digest engine. | **None** |

---

## 5. Command Line Arguments

`emdbmgr` offers a clear CLI argument structure to configure runtime behavior:

| Argument | Type | Default Value | Description |
|---|---|---|---|
| `-version` | Boolean | `false` | Prints the application name and the compile-time injected version, then exits cleanly. |
| `-detectdb` | Boolean | `false` | Inspects the raw signature of `-sourcedb-path` and dumps the parsed metadata to stdout in JSON. |
| `-backup-type` | String | `""` | Specifies the backup execution type. Accepted values: `db`, `json`, or `json,db` (comma-separated for simultaneous dual backup). |
| `-sourcedb-path` | String | `""` | Absolute or relative filesystem path to the active source SQLite or BoltDB file. |
| `-targetdata-path`| String | `""` | Destination path. For dual-backups or directories, this acts as the target backup folder. |

---

## 6. Detailed Examples on How to Use

### 6.1 Compiling the Binary
To compile a lightweight, optimized CGO-free binary:
```bash
go build -o emdbmgr.exe
```

To inject a custom production version string at compile-time (e.g. into your CD build pipeline):
```bash
go build -ldflags "-X main.version=1.4.2-stable" -o emdbmgr.exe
```

---

### 6.2 Checking Tool Version
```bash
emdbmgr -version
```
**Stdout Output:**
```text
Embedded Database Manager - v1.4.2-stable
```

---

### 6.3 Detecting SQLite3 Database Specs
```bash
emdbmgr -detectdb -sourcedb-path=/data/sftpgo.db
```
**Stdout Output:**
```json
{
  "file_path": "/data/sftpgo.db",
  "file_size_bytes": 102400,
  "db_type": "sqlite3",
  "sqlite_details": {
    "magic_matched": true,
    "page_size": 4096,
    "write_version": 2,
    "read_version": 2,
    "wal_mode": true,
    "schema_version": 4,
    "user_version": 0
  }
}
```

---

### 6.4 Detecting BoltDB Database Specs
```bash
emdbmgr -detectdb -sourcedb-path=/data/bolt_cache.db
```
**Stdout Output:**
```json
{
  "file_path": "/data/bolt_cache.db",
  "file_size_bytes": 65536,
  "db_type": "boldb",
  "bolt_details": {
    "magic_matched": true,
    "version": 2,
    "page_size": 4096,
    "tx_id": 142,
    "page_count": 16
  }
}
```

---

### 6.5 Safe Backup: Database Replication (Shadow DB)
To create a safe shadow copy of an active database:
```bash
emdbmgr -backup-type=db -sourcedb-path=/data/sftpgo.db -targetdata-path=/backups
```
**Stdout Output:**
```json
{
  "backup_type": "db",
  "target_path": "/backups/backup_sftpgo_sqlite-20260528135822.db.tar.zst",
  "data_xxh64": "2b97537832b3d9b8",
  "duration_ms": 12,
  "status": "success"
}
```

---

### 6.6 Safe Backup: Dynamic JSON Extraction
To dump an active database as raw JSON:
```bash
emdbmgr -backup-type=json -sourcedb-path=/data/bolt_cache.db -targetdata-path=/backups
```
**Stdout Output:**
```json
{
  "backup_type": "json",
  "target_path": "/backups/backup_bolt_cache_bolt-20260528135822.json.tar.zst",
  "data_xxh64": "83b5f78fbb3fb191",
  "duration_ms": 4,
  "status": "success"
}
```

---

### 6.7 Safe Backup: Dual Mode (Shadow DB + JSON Extraction)
To execute both backup methods simultaneously:
```bash
emdbmgr -backup-type=json,db -sourcedb-path=/data/sftpgo.db -targetdata-path=/backups
```
**Stdout Output:**
```json
[
  {
    "backup_type": "json",
    "target_path": "/backups/backup_sftpgo_sqlite-20260528135822.json.tar.zst",
    "data_xxh64": "be6b2b95315ad19f",
    "duration_ms": 5,
    "status": "success"
  },
  {
    "backup_type": "db",
    "target_path": "/backups/backup_sftpgo_sqlite-20260528135822.db.tar.zst",
    "data_xxh64": "2b97537832b3d9b8",
    "duration_ms": 13,
    "status": "success"
  }
]
```

### Contents of the Resulting `.tar.zst` Package:
Inside the generated `backup_sftpgo_sqlite-20260528135822.db.tar.zst` package, the following files exist:
* `backup_sftpgo_sqlite.db` — The safe transaction-consistent replica.
* `metadata.json` — Operational metadata (durations, platforms, exact sizes, specs, and xxHash integrity checksum).

---

## 7. Automated Integration and Unit Testing

`emdbmgr` includes an automated testing framework (`test_runner.go`) designed to run high-coverage integration and validation checks. It establishes sandboxed environments on-the-fly, generating live databases and verifying the full operation of signature parsing, safe backup, and archive creation routines.

### 7.1 Executing the Test Suite

To run the test suite under the default operating system temp directory:
```bash
go run test_runner.go
```

To run the test suite using a prioritized temporary directory path (e.g. for CI/CD pipeline isolation):
```bash
# Windows PowerShell
$env:TMPDIR="C:\custom_ci_tmp"; go run test_runner.go

# Linux / macOS
TMPDIR=/custom_ci_tmp go run test_runner.go
```

---

### 7.2 Testing Matrix and Acceptance Criteria

The following verification matrix defines the test cases performed by the test suite, their scope, and their respective architectural acceptance criteria:

| Test Case | Purpose / Scope | Verification Methodology | Acceptance Criteria |
|---|---|---|---|
| **Version Verification (Test 0)** | Validate compile-time version injection via Go linker flags. | Compiles the target binary on-the-fly with `-ldflags "-X main.version=1.0.0-20260528"` and executes `-version`. | Output stdout matches exactly `Embedded Database Manager - v1.0.0-20260528\n` and exits with code 0. |
| **SQLite3 Signature Analysis (Test 1)** | Validate raw format identification and metadata extraction from open SQLite databases. | Creates a mock SQLite database, populates standard tables (containing integers, texts, and raw non-UTF-8 binary BLOB blocks), and runs `-detectdb`. | Output matches `sqlite3` and successfully extracts correct `page_size` (4096), `wal_mode` status, schema, and user versions. |
| **BoltDB Signature Analysis (Test 1)** | Validate page structural offset analysis and host-endian magic word matching. | Creates a mock BoltDB database, populates standard buckets (containing UTF-8 keys and raw non-UTF-8 binary values), and runs `-detectdb`. | Output matches `boldb` and successfully extracts correct `version` (2), `page_size` (4096), `tx_id`, and `page_count` metrics. |
| **Safe SQLite3 Backup Verification (Test 2)** | Validate transaction-consistent shadow replication and row-by-row streaming JSON serialization for active SQLite. | Triggers a dual backup (`-backup-type=json,db`) on an open SQLite database, extracts the resulting `.tar.zst` packages, and scans contents. | The database shadow replica and JSON extraction data exist inside the archives, the `metadata.json` is present, and its `data_xxh64` checksum matches the on-the-fly calculated hash. |
| **Safe BoltDB Backup Verification (Test 2)** | Validate read-only MVCC block replication and recursive tree cursor serialization for active Bolt. | Triggers a dual backup (`-backup-type=json,db`) on an open Bolt database, extracts the resulting `.tar.zst` packages, and scans contents. | The database shadow replica and JSON extraction data exist inside the archives, the `metadata.json` is present, and its `data_xxh64` checksum matches the on-the-fly calculated hash. |
| **Boundary & Signature Failure (Test 3)** | Validate rejection of empty, text, or corrupted database files. | Feeds empty files, non-database raw binary files, and text files to `-detectdb` and `-backup-type`. | The tool gracefully handles failures, exits with code 1, and outputs a formatted JSON error mapping the exact failure reason. |
| **Active Concurrency & Safety (Test 4)** | Prove safe, non-blocking concurrent writer behavior under database backups. | Spawns a background writer thread executing continuous database updates while executing `-backup-type=json,db`. | The background writer experiences **zero block timeouts** (100% write transaction success) and the generated backup remains completely intact and uncorrupted. |
| **Locked Database Fallbacks (Test 5)** | Prove safe, verified fallback backups under exclusive writer locks on both database formats. | Spawns exclusive locks on SQLite and BoltDB files, then executes `-backup=json,db`. | Backups succeed cleanly via target-staged copy-on-read, and all copies pass programmatic integrity verification. |
| **Sandboxed Portability & Zero-Clutter** | Guarantee zero file-system pollution inside the git repository. | Inspects the repository directory structure dynamically during and after test suite execution. | No binary executables, mock database files, target backup archives, or temporal logs are written to the repository directory. The dynamic sandbox folder is cleaned up automatically upon exit. |

---

## 8. Concurrency & Exclusive Locks: Deep Dive

In production environments, embedded databases (like SQLite3 and BoltDB) are frequently embedded within long-running stateful services (e.g., SFTPGo). These services typically open the database files in **Read-Write (exclusive write) mode**. Managing backups in these scenarios introduces complex operating system and transactional challenges.

### 8.1 The Operating System Locking Conundrum: Windows vs. Linux

Embedded databases utilize OS-level file locking to enforce concurrency safety across processes. However, Windows and Linux implement these locks differently, creating unique constraints for a standalone backup tool:

```mermaid
sequenceDiagram
    participant SFTPGo as Service Process (SFTPGo)
    participant DB as production.db
    participant emdbmgr as emdbmgr CLI Backup Process

    SFTPGo->>DB: Holds Exclusive Write Lock (LockFileEx / flock)
    
    rect rgb(255, 235, 235)
        Note over emdbmgr: Phase 1: Direct Open Attempt
        emdbmgr-xDB: 1. Direct Open (Shared Read Lock Request)
        Note right of emdbmgr: Blocked / Denied on Windows & Linux
    end

    rect rgb(235, 255, 235)
        Note over emdbmgr: Phase 2: OS Read-Share Open (Staging)
        emdbmgr->>DB: 2. OS Read-Share Open (os.Open with FILE_SHARE_READ)
        DB-->>emdbmgr: SUCCESS (Reads raw bytes to temp file)
        
        Note over emdbmgr: Phase 3: Integrity Verification
        emdbmgr->>emdbmgr: 3. Verify Copy (tx.Check / PRAGMA integrity_check)
        Note right of emdbmgr: Double-verifies page structures
    end
```

*   **Linux (Unix `flock` / `fcntl`):**
    *   Unix file locks are advisory by default.
    *   If a service holds an exclusive lock (writer), any attempt by an external process to open the file and acquire a **shared read lock** via `bbolt.Open` will block or timeout.
*   **Windows (`LockFileEx`):**
    *   Windows file locking is mandatory and strictly enforced.
    *   When a service opens BoltDB or SQLite in read-write mode, the OS locks the file region.
    *   Any subsequent attempt by a separate process to call `bbolt.Open` (even with `ReadOnly: true`) attempts to acquire a shared lock on the database header. Under Windows, **a shared lock request is immediately denied** if an exclusive lock is active. This causes `bbolt.Open` to hang and eventually fail with a `timeout` error.

---

### 8.2 The "Torn Write" Backup Corruption Risk

To bypass the OS lock blockages, a naive approach might perform a raw file copy (e.g. using `io.Copy`) of the active database file. **This is highly dangerous and causes database backup corruption.**

1.  **The Concurrency Window:** A B-Tree/B+ Tree database consists of complex, interconnected pages (indexes, schemas, data pages, and freelists).
2.  **The Torn Write Scenario:** If `io.Copy` is reading the file sequentially while the active writer commits a transaction:
    *   The copy might capture some pages **before** they are written by the new transaction.
    *   It might capture other pages **after** they are written by the new transaction.
3.  **The Resulting State:** The copied file will contain a mix of mismatched page references, invalid headers, or a corrupted freelist. If this candidate file is archived, **the backup is permanently corrupted** and will fail to restore.

---

### 8.3 The `emdbmgr` Safe Backup Blueprint

To achieve 100% safety, zero-downtime operations, and guarantee **0% risk of backup corruption**, `emdbmgr` implements a multi-phase, self-healing backup pipeline:

```mermaid
graph TD
    Start[Trigger Backup CLI] --> DirectOpen[Attempt Direct Non-Blocking Open]
    
    subgraph Direct Path
        DirectOpen -->|Unlocked SQLite / Bolt| DirectBackup[Stream Backup to Staging Buffer]
    end

    subgraph Locked Staging Path
        DirectOpen -->|Timeout / Lock Error| StageDir[Target Folder Staging]
        StageDir --> CopyRaw[Copy Locked DB to Temp File]
        CopyRaw --> VerifyCopy[Integrity Check: tx.Check / integrity_check]
        VerifyCopy -->|Corrupt / Torn Write| RetryLimit{Attempts < 3?}
        RetryLimit -->|Yes| Sleep[Sleep 50ms & Retry]
        Sleep --> CopyRaw
        RetryLimit -->|No| Fail[Abort & Return Error]
        VerifyCopy -->|100% Consistent & OK| StagedBackup[Stream Staged Copy to Staging Buffer]
    end

    DirectBackup --> UniversalCheck[Systematic Backup Integrity Check: PRAGMA / tx.Check / json.Decoder]
    StagedBackup --> UniversalCheck

    UniversalCheck -->|Validation Passed| Compress[Stream to Zstandard & Tar Archive]
    UniversalCheck -->|Validation Failed| FailAbort[Abort, Discard & Return Error]
    Compress --> CleanUp[Clean Up Staging/Temporary Files]
```

#### Phase 1: Direct Non-Blocking Connection (Best-Path)
*   **SQLite:** We open a read-only (`mode=ro`) connection and set a `PRAGMA busy_timeout = 5000`. This instructs SQLite to automatically wait up to 5 seconds for any brief transaction locks to release, then executes **`VACUUM INTO`**, which is transactionally consistent and guaranteed safe by the SQLite engine itself.
*   **BoltDB:** We attempt a direct `bolt.Open` with `ReadOnly: true` and a short `Timeout: 1 * time.Second`. If the file is not exclusively locked, we back it up directly using standard MVCC transaction streaming (`tx.WriteTo`).

#### Phase 2: OS-Level Read-Share Copying (Contingency Staging)
If the direct open fails with a timeout (proving an exclusive lock is active):
*   We leverage Go's `os.Open` which opens the file with read-only sharing flags (`FILE_SHARE_READ`). The OS permits reading the raw bytes without acquiring the database lock.
*   We copy the active database file to a temporary file located **strictly inside the user-specified target directory (`-targetdata-path`)**.

#### Phase 3: Programmatic Verification Loop
Rather than trust the copied file, we treat it as a **candidate copy** and run programmatic validation:
*   **BoltDB Integrity Check:** We open the candidate file and run **`tx.Check()`**. This method traverses the entire B+ tree structure, checks all page header flags, audits keys, and verifies the freelist consistency. If a single page reference or CRC is mismatched, it fails.
*   **SQLite Integrity Check:** We open the candidate file and run **`PRAGMA integrity_check`**, which verifies the database schema, cell sizes, and index ordering.

#### Phase 4: Linear Back-off Retry
*   If the integrity check fails (detecting a torn write), the tool **instantly deletes the candidate copy**. No corrupted archive is ever created.
*   The tool sleeps for `50 milliseconds`—giving the live database writer plenty of time to fully commit its transaction and return to idle.
*   It retries the process up to 3 times. If all attempts fail, it aborts cleanly, returning a structured JSON error.

### 8.4 Systematic Backup Integrity Verification

In addition to lock-bypassing checks, `emdbmgr` enforces a **Universal, Systematic Integrity Check** on **every single backup** (whether executed in direct mode or fallback mode, as a SQL/binary database copy or a JSON data extraction) before committing it to the final compressed tar archive.

#### Core Verification Mechanics:

1.  **Staging Decoupling:**
    Before Zstandard compression and archive packing occur, the compiled backup data stream is generated and held inside a high-speed hybrid staging buffer (`HybridWriter`).
2.  **Dual-Format Validation:**
    The tool instantiates an independent validation reader on the staged buffer and executes the following programmatic checks depending on the target format:
    *   **SQLite `.db` Backups:** Runs a full **`PRAGMA integrity_check`** which scans database cell mappings, page orders, relational schema tables, and index structures.
    *   **BoltDB `.db` Backups:** Executes **`tx.Check()`** inside an active read transaction to crawl the entire B+ Tree structural hierarchy, check node page boundaries, and audit the freelist table.
    *   **JSON `.json` Backups:** Runs a zero-allocation, streaming JSON token decoder (**`json.NewDecoder`**). If the JSON file contains any truncated lines, missing braces, or syntactical errors, it is instantly flagged.
3.  **Strict Commit Guarantee:**
    If any check fails, the tool **immediately aborts the backup**, deletes the temporary staged file from the disk, and returns a formatted JSON error. A corrupted or partially written backup package is **never** archived or saved to disk.

### 8.5 Summary
By decoupling raw file operations from the archive compilation and wrapping them in **mandatory programmatic B+ Tree/SQL page verification**, `emdbmgr` guarantees that **the live production database is never written to (0% risk of live database corruption)**, and **the final backup package is guaranteed to be 100% healthy, verified, and transactionally consistent (0% risk of archiving corrupted backups)**.

