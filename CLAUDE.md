# Project Instructions

## Important Security Notice

Do not commit IP-addresses, usernames or passwords to git

## Development Environment

This project is using devenv for the development environment, dependencies and for git hooks. See more in ./devenv.nix

## Documentation

Do not create separate markdown files. Only use ./README.md and check that it's always consistent with the project state.

## Testing with curl

Here are some examples of how webdav works with curl

### Create folder

```sh
curl -X MKCOL --user '$STORAGEBOX_USER:$STORAGEBOX_PASSWORD' https://$STORAGEBOX_USER.your-storagebox.de/folder/
```

### List files

```sh
curl -X PROPFIND -H "Depth: 1" --user '$STORAGEBOX_USER:$STORAGEBOX_PASSWORD' https://$STORAGEBOX_USER.your-storagebox.de/folder/
```

---

## DuckDB WebDAVFS Extension - Technical Deep Dive

### What Is This Extension?

This is a DuckDB filesystem extension that enables reading and writing files (CSV, Parquet, JSON, etc.) directly from WebDAV servers. While originally based on DuckDB's httpfs extension code, webdavfs is a standalone extension that adds full read/write WebDAV support.

### Key Features

- **Read/Write Support**: This extension supports full read/write operations, unlike the read-only httpfs extension
- **WebDAV Protocol**: Uses WebDAV (RFC 4918) methods: PROPFIND, MKCOL, MOVE, PUT, DELETE
- **Authentication**: Integrates with DuckDB's secrets system for credentials
- **Hetzner Integration**: Built-in `storagebox://` protocol for Hetzner Storage Box
- **DuckLake Compatible**: Works with DuckDB's DuckLake extension for ACID transactions
- **Glob Patterns**: Supports `*.csv`, `**/*.parquet` and Hive partitioning
- **Streaming Uploads**: Memory-efficient uploads for large files (>50MB)

### WebDAV Protocol Basics

WebDAV (Web Distributed Authoring and Versioning) is an extension to HTTP/1.1 that allows clients to perform remote web content authoring operations.

### HTTP Methods Used

1. **PROPFIND** - List directory contents and get file metadata
   - `Depth: 0` - Properties of the resource itself
   - `Depth: 1` - Resource + immediate children
   - Returns XML with file properties (size, modified time, etc.)

2. **MKCOL** - Create directory (Make Collection)
   - Creates a new directory/collection
   - Returns 201 Created on success

3. **MOVE** - Rename/move files (server-side operation)
   - Uses `Destination:` header for target URL
   - Uses `Overwrite: T/F` header
   - Returns 201 Created or 204 No Content
   - **Critical for performance**: Avoids download+upload+delete pattern

4. **PUT** - Upload file
   - Standard HTTP PUT with file contents
   - Returns 201 Created or 204 No Content

5. **DELETE** - Delete file
   - Standard HTTP DELETE
   - Returns 204 No Content

6. **GET** - Download file (with range support)
   - Standard HTTP GET
   - Supports `Range:` header for partial reads
   - **Critical for performance**: Parquet can read only needed row groups

7. **HEAD** - Get file metadata without downloading
   - Returns headers with Content-Length, Last-Modified, etc.

### Hetzner Storage Box

#### What It Is

Hetzner Storage Box is a WebDAV-compatible network storage service offered by Hetzner Cloud.

**Pricing**: ~€3.20/TB/month (BX11 - 1TB storage box)

- Significantly cheaper than S3 (~€20/TB/month)
- No egress fees (unlike S3)
- Reachable over public internet

**Performance Characteristics**:

- Bandwidth: 1-2.5 Gbps (100-300 MB/s) - Limited compared to S3
- Latency: Higher than S3 (Europe-based servers)
- No multipart upload support
- Single request uploads only

**Best Use Cases**:

- Infrequent access patterns
- Cost-sensitive workloads
- Moderate file sizes (<1GB)
- European data residency requirements
- Backup/archival storage

**Not Great For**:

- High-throughput streaming analytics
- Very large files (>1GB) with frequent writes
- Low-latency requirements
- Concurrent heavy workloads

#### Why It Works Well with DuckDB

1. **Read Performance is Surprisingly Good**
   - WebDAV supports HTTP 1.1 range requests
   - Parquet format only reads needed columns/row groups
   - DuckDB can read small portions efficiently
   - File cache (`enable_external_file_cache=true`) helps a lot

2. **Cost-Effective for Analytics**
   - Most analytics = read-heavy workloads
   - Compress data with ZSTD (reduces storage/bandwidth)
   - €3.20/TB vs €20/TB = 6x cost savings

3. **DuckLake Integration**
   - DuckLake provides ACID transactions on object storage
   - Works perfectly with WebDAV backend
   - Small metadata files + larger data files pattern fits WebDAV well

### Architecture Overview

#### Class Hierarchy

```text
FileSystem (DuckDB core)
  └── HTTPFileSystem (httpfs extension)
        └── WebDAVFileSystem (this extension)

FileHandle (DuckDB core)
  └── HTTPFileHandle (httpfs extension)
        └── WebDAVFileHandle (this extension)

HTTPClient (DuckDB core)
  └── HTTPFSCurlClient (httpfs curl implementation)
```

#### URL Parsing

The extension handles two URL schemes:

1. **Generic WebDAV**: `webdav://server.com/path/file.parquet`
   - Converted to: `https://server.com/path/file.parquet`

2. **Hetzner Storage Box**: `storagebox://u123456/path/file.parquet`
   - Converted to: `https://u123456.your-storagebox.de/path/file.parquet`

#### Authentication

Uses DuckDB's secret system:

```sql
CREATE SECRET my_storage (
    TYPE WEBDAV,
    USERNAME 'u123456',
    PASSWORD 'password',
    SCOPE 'storagebox://u123456'
);
```

Authentication is Basic Auth over HTTPS:

- Header: `Authorization: Basic base64(username:password)`
- Added to every WebDAV request

#### Write Operation Flow

1. **Open file for writing**:
   - Creates `WebDAVFileHandle` with write mode
   - Initializes write buffer

2. **Write() calls**:
   - Data accumulates in memory buffer
   - If buffer exceeds 50MB threshold → spill to temp file
   - Continues writing to temp file

3. **Close() / FileSync()**:
   - Calls `FlushBuffer()`
   - If using temp file: streams upload via curl
   - If in memory: uploads buffer directly
   - Uses WebDAV MOVE to rename `tmp_file` to final name

4. **Streaming Upload** (for files >50MB):
   - Opens temp file with `fopen()`
   - Sets up curl with `CURLOPT_UPLOAD` and `CURLOPT_READFUNCTION`
   - Curl reads file in 64KB chunks via `ReadCallbackFile()`
   - Never loads entire file into memory

### Performance Optimizations

#### 1. WebDAV MOVE Method (Implemented)

**Problem**: Original implementation downloaded file, re-uploaded it, then deleted original

- 113MB file required 339MB of bandwidth (113MB down + 113MB up + 113MB up)
- Took 85 seconds

**Solution**: Use WebDAV MOVE method (RFC 4918 Section 9.9)

- Server-side atomic operation
- Only 113MB bandwidth
- Reduced to 23.8 seconds (72% faster)

Implementation:

```cpp
HTTPHeaders headers;
headers["Destination"] = target_url;
headers["Overwrite"] = "T";
CustomRequest(handle, source_url, headers, "MOVE", nullptr, 0);
```

#### 2. Streaming Uploads (Implemented)

**Problem**: Entire file buffered in memory before upload

- 113MB file = 113MB RAM usage
- Could cause OOM on larger files

**Solution**: Spill to temp file at 50MB threshold, stream upload via curl

- Max 50MB in memory during writes
- Curl streams from temp file in chunks
- Temp file cleaned up after upload

Implementation details:

- Threshold: `static constexpr idx_t STREAMING_BUFFER_THRESHOLD = 50 * 1024 * 1024;`
- Temp file: `/tmp/webdav_upload_XXXXXX` (mkstemp)
- Curl callback: `ReadCallbackFile()` reads 64KB chunks
- Cleanup: Destructor and `Close()` remove temp file

#### 3. HTTP Optimizations

**Expect: 100-continue Header**:

- Disabled for large uploads (>10MB)
- Some WebDAV servers (Hetzner) don't handle it well
- Prevents timeout issues

**Timeout Extension**:

- Default curl timeout: 30 seconds
- Large uploads (>10MB): 600 seconds (10 minutes)
- Prevents timeout on slow connections

**Range Requests**:

- Inherited from httpfs
- Allows reading portions of files
- Critical for Parquet performance

### Common Issues & Solutions

#### Issue 1: HTTP 507 Insufficient Storage

When storage box is full, WebDAV returns HTTP 507.

**Detection**: Check response status in PutRequest
**Handling**: Custom error message in `GetHTTPError()`

#### Issue 2: Non-Sequential Writes

WebDAV doesn't support random-access writes like local filesystems.

**Solution**: Validate write location matches expected offset

```cpp
if (location != expected_location) {
    throw IOException("WebDAV does not support non-sequential writes");
}
```

#### Issue 3: GPG Signing Failures in Git Hooks

Pre-commit hooks sometimes fail with GPG agent errors.

**Solution**: Retry the commit (GPG agent usually recovers)

#### Issue 4: Clang-format Pre-commit Hook

Code must be formatted before commit.

**Solution**: Hook automatically reformats, just re-stage and commit

### File Operations Mapping

| DuckDB Operation | WebDAV Method | Notes |
|-----------------|---------------|-------|
| `FileExists()` | HEAD | Check 404 vs 2xx |
| `DirectoryExists()` | PROPFIND Depth:0 | Check if it's a collection |
| `ListFiles()` | PROPFIND Depth:1 | Parse XML response |
| `CreateDirectory()` | MKCOL | Returns 201 Created |
| `RemoveFile()` | DELETE | Returns 204 No Content |
| `MoveFile()` | MOVE | Server-side operation |
| `Read()` | GET with Range | Supports partial reads |
| `Write()` | PUT (buffered) | Flushes on close |
| `FileSync()` | PUT (flush) | Forces upload |

### Temporary File Pattern

When writing files, the extension uses a temporary file pattern:

1. Open `file.parquet` for writing
2. Actually write to `tmp_file.parquet`
3. On close, MOVE `tmp_file.parquet` → `file.parquet`

This provides:

- Atomic writes (file doesn't exist until complete)
- Crash safety (incomplete writes don't corrupt existing file)
- Consistency with local filesystem behavior

### Testing

#### Running Full Test Suite

After making any changes to the extension, always run the full test suite with the Docker WebDAV test server:

```bash
./scripts/run_webdav_test_server.sh && source ./scripts/set_webdav_test_server_variables.sh && ./build/release/test/unittest "test/*"
```

This command:

1. Starts a Docker container with WebDAV server and test data
2. Sets environment variables for test authentication
3. Runs all SQL logic tests including integration tests against the Docker server

**Important**: Tests should only test against files present in the Docker test server (defined in `scripts/webdav.yml`). Do not create tests against non-existent servers like `webdav://server.com/` as they will fail unpredictably.

#### Manual Testing

```bash
# Build
make release GEN=ninja VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake

# Run test script
./build/release/duckdb -unsigned -f test_create_remote_parquet.sql
```

#### What to Look For

1. **Memory Usage**: Should not exceed ~50MB even for 100MB+ uploads
2. **Bandwidth**: MOVE should not download+re-upload
3. **Streaming Logs**: Should see `ReadCallbackFile` messages for large files
4. **Temp File Cleanup**: No `/tmp/webdav_upload_*` files left after completion

### Future Improvements

#### Not Yet Implemented

1. **Chunked Uploads**: Break very large files into multiple PUTs
   - Would help with >1GB files
   - Needs custom server-side reassembly

2. **Connection Pooling**: Reuse HTTP connections better
   - Currently relies on curl's connection reuse
   - Could be more explicit

3. **Parallel Uploads**: Upload multiple files simultaneously
   - DuckDB may already do this at higher level
   - Need to verify thread safety

4. **Read Caching**: More sophisticated local cache
   - DuckDB has `enable_external_file_cache`
   - Could add WebDAV-specific optimizations

5. **Compression**: Transparent compression/decompression
   - Already supported via Parquet compression
   - Could add at transport level

6. **Retry Logic**: Automatic retries on transient failures
   - Network issues, 5xx errors
   - Exponential backoff

### Development Notes

#### Building

Requires:

- CMake
- Ninja
- vcpkg (for curl)
- DuckDB source (submodule in `./duckdb/`)

The build uses DuckDB's extension system:

- Extension config: `extension_config.cmake`
- Loaded into DuckDB core at build time
- Can be built as loadable extension or statically linked

#### Code Organization

```text
src/
  ├── webdavfs.cpp              # Main WebDAV filesystem implementation
  ├── webdavfs.hpp              # WebDAV classes and interfaces
  ├── httpfs_curl_client.cpp    # Curl HTTP client (from httpfs)
  ├── httpfs.cpp                # Base HTTP filesystem (from httpfs)
  └── webdav_secrets.cpp        # Secret manager integration

src/include/
  ├── webdavfs.hpp
  ├── httpfs.hpp
  └── httpfs_curl_client.hpp
```

#### Key Functions

**webdavfs.cpp**:

- `ParseUrl()` - Convert webdav:// and storagebox:// to https://
- `MoveRequest()` - WebDAV MOVE method implementation
- `PutRequest()` - Regular PUT for small files
- `PutRequestFromFile()` - Streaming PUT for large files
- `PropfindRequest()` - Directory listing
- `MkcolRequest()` - Create directory
- `Write()` - Buffered write with spill-to-disk

**httpfs_curl_client.cpp**:

- `HTTPFSCurlClient::Put()` - Curl PUT implementation
- `ReadCallbackFile()` - Curl callback for streaming uploads
- `SetHTTPClientUploadFile()` - Helper to configure streaming

### Debugging

#### Enable Debug Output

The code has extensive `fprintf(stderr, ...)` logging:

```bash
./build/release/duckdb -unsigned -f script.sql 2>&1 | grep WebDAV
```

Common prefixes:

- `[WebDAV]` - WebDAV filesystem operations
- `[CURL]` - Curl client operations
- `[CURL ReadCallbackFile]` - Streaming upload chunks

### Performance Comparison

#### Hetzner Storage Box vs S3

| Metric | Hetzner Storage Box | AWS S3 Standard |
|--------|---------------------|-----------------|
| Cost (storage) | €3.20/TB/month | ~€20/TB/month |
| Cost (egress) | Free | ~€80/TB |
| Bandwidth | 1-2.5 Gbps | 10-100 Gbps |
| Latency | ~20-50ms (Europe) | ~5-20ms |
| Multipart upload | No | Yes |
| Range requests | Yes | Yes |
| Protocol | WebDAV over HTTPS | S3 API |

#### Read Performance

Reading with DuckDB file cache enabled:

- First read: ~2-5s for 113MB file
- Subsequent reads: <100ms (cached)
- Selective column read (Parquet): ~500ms (only reads needed columns)

### Security Considerations

1. **HTTPS Only**: All traffic is encrypted
2. **Basic Auth**: Credentials sent with every request (over HTTPS)
3. **Secrets Storage**: Uses DuckDB's secret manager (not plaintext SQL)
4. **No Credential Logging**: Passwords never logged to stderr/stdout
5. **Temp File Security**: Uses `mkstemp()` for secure temp file creation

### Limitations

1. **No Random Access Writes**: Must write sequentially
2. **No Append Mode**: Can't append to existing files
3. **No Simultaneous Read/Write**: File opened for either read OR write
4. **No File Locking**: No cross-client locking mechanism
5. **Single Request Upload**: No chunked/multipart uploads
6. **No Streaming Downloads**: Full file download (though range requests help)

### References

- RFC 4918: HTTP Extensions for Web Distributed Authoring and Versioning (WebDAV)
- DuckDB Documentation: <https://duckdb.org/docs/>
- Hetzner Storage Box: <https://www.hetzner.com/storage/storage-box>
- Curl Documentation: <https://curl.se/libcurl/>

---

## Test Setup Instructions

### Creating Your Test File

1. **Copy the example file**:

   ```bash
   cp test_create_remote_parquet.sql.example test_create_remote_parquet.sql
   ```

2. **Set environment variables**:

   ```bash
   export STORAGEBOX_USER='u123456'
   export STORAGEBOX_PASSWORD='your_password_here'
   ```

3. **Update the test file**:
   - Replace `/path/to/your/data.csv` with path to your test CSV file
   - Or create your own test data inline

4. **Run the test**:

   ```bash
   ./build/release/duckdb -unsigned -f test_create_remote_parquet.sql
   ```

### Important Security Notes

- **Never commit** `test_create_remote_parquet.sql` with real credentials
- The file is in `.gitignore` to prevent accidental commits
- Always use environment variables or DuckDB secrets for credentials
- For CI/CD, use encrypted secrets management

### Alternative: Using DuckDB Secrets Only

Instead of environment variables, you can create secrets interactively:

```sql
.timer on
LOAD webdavfs;

-- Create secret interactively (DuckDB will prompt for password)
CREATE SECRET hetzner_test (
    TYPE WEBDAV,
    USERNAME 'u123456',
    PASSWORD read_text('/path/to/password_file'),  -- or use getenv()
    SCOPE 'storagebox://u123456'
);

-- Your queries here
```

### For Hetzner Storage Box Setup

#### Quick Setup

```bash
# Install CLI tools
brew install hcloud pwgen

# Login to Hetzner Cloud
hcloud context create my-account

# Generate password
export STORAGEBOX_PASSWORD=$(pwgen -y -1 -n 32)

# Create storage box (1TB for ~€3.20/month)
hcloud storage-box create \
    --enable-webdav \
    --reachable-externally \
    --name duckdb-test-box \
    --location hel1 \
    --type bx11 \
    --password $STORAGEBOX_PASSWORD

# Note the username (e.g., u123456) from output
export STORAGEBOX_USER='u123456'
```

#### Manual Setup via Web UI

1. Go to <https://www.hetzner.com/storage/storage-box>
2. Order a storage box (BX11 is cheapest at 1TB)
3. Enable WebDAV in storage box settings
4. Note username and set password
5. Use in your test file

### Example Test Data

If you don't have a CSV file, create one:

```bash
cat > test_data.csv <<EOF
id,name,value
1,test1,100
2,test2,200
3,test3,300
EOF
```

Then use it in your test:

```sql
COPY (FROM 'test_data.csv')
TO 'storagebox://' || getenv('STORAGEBOX_USER') || '/test.parquet';
```
