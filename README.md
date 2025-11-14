# DuckDB WebDAVFS Extension

A DuckDB filesystem extension for reading and writing files on WebDAV servers, including Nextcloud, ownCloud, and Hetzner Storage Box. This is a standalone extension that provides full read/write support for WebDAV, unlike the read-only HTTP support in DuckDB's httpfs extension.

## Features

- **Full read/write support** - Unlike httpfs, webdavfs supports writing files to WebDAV servers
- Read/write CSV, Parquet, JSON files directly from WebDAV
- Authentication via DuckDB secrets
- Glob patterns (`*.csv`, `**/*.parquet`) and Hive partitioning
- Built-in `storagebox://` protocol for Hetzner Storage Box
- DuckLake support for ACID transactions on WebDAV storage
- Memory-efficient streaming uploads for large files

## Quick Start

### Hetzner Storage Box

```sql
CREATE SECRET my_storage_box (
    TYPE WEBDAV,
    USERNAME 'u123456',
    PASSWORD 'your_password',
    SCOPE 'storagebox://u123456'
);

-- Read files
SELECT * FROM 'storagebox://u123456/data.parquet';
SELECT * FROM 'storagebox://u123456/*.csv';

-- Write files
COPY (SELECT * FROM my_table)
TO 'storagebox://u123456/output.parquet'
(FORMAT PARQUET);
```

To create a storage box quickly from command line:

```sh
# Install the dependencies
brew install hcloud pwgen

# Login to your account
hcloud context create my-account-for-ducklake-tests

export HETZNER_STORAGEBOX_PASSWORD=$(pwgen -y -1 -n 32)

# Creates the cheapest 1Tb per 3.20+VAT € storage box
hcloud storage-box create --enable-webdav --reachable-externally \
    --name duckdb-test-box --location hel1 --type bx11 \
    --password $HETZNER_STORAGEBOX_PASSWORD
```

### Generic WebDAV Server

```sql
CREATE SECRET my_webdav (
    TYPE WEBDAV,
    USERNAME 'user',
    PASSWORD 'password',
    SCOPE 'webdav://your-server.com'
);

SELECT * FROM 'webdav://your-server.com/data.parquet';
```

## DuckLake Integration

### Cost vs Performance Trade-offs

**Hetzner Storage Box** is significantly cheaper than object storage (S3, GCS) but has important limitations:

- **Cost**: ~€3.20/TB/month (Storage Box BX11) vs ~€20/TB/month (AWS S3)
- **Bandwidth**: Limited to 1-2.5Gbps (100-300MB/s) vs 10-100Gbps in S3 like object storages
- **Upload**: No multipart uploads - files uploaded as single requests
- **Best for**: Infrequent access, cost-sensitive workloads, moderate file sizes (<1GB)
- **Read performance**: Surprisingly good due because Webdav supports HTTP 1.1 range requests and we can read only small portions of parquet files

### Example

```sql
INSTALL ducklake;
LOAD webdavfs;

CREATE SECRET hetzner (
    TYPE WEBDAV,
    USERNAME 'u123456',
    PASSWORD 'your_password',
    SCOPE 'storagebox://u123456'
);

-- Attach DuckLake database
ATTACH 'ducklake:analytics.ducklake' (
    DATA_PATH 'storagebox://u123456/datalake/'
);
USE analytics;

-- Create table from data
CREATE TABLE events AS
SELECT * FROM read_csv('https://example.com/events.csv');
```

## Performance Tips

```sql
-- Enable DuckDB's file cache to speed up reads
SET enable_external_file_cache=true;

-- If you have CPU cycles to spare use zstd with high compression
COPY my_table TO 'storagebox://u123456/data.parquet'
(FORMAT PARQUET, COMPRESSION 'ZSTD', COMPRESSION_LEVEL 20);
```

## Configuration

The WebDAV extension can be configured using DuckDB settings:

### Available Settings

```sql
-- Enable debug logging for WebDAV operations (default: false)
SET webdav_debug_logging = true;

-- Maximum number of retries for failed operations (default: 3)
SET webdav_max_retries = 5;

-- File size threshold in MB for streaming uploads (default: 50)
-- Files larger than this are streamed from disk to avoid memory pressure
SET webdav_streaming_threshold_mb = 100;
```

### Example: Enable Debug Logging

```sql
-- Enable verbose logging to troubleshoot connection issues
SET webdav_debug_logging = true;

SELECT * FROM 'webdav://server/file.parquet';
-- Debug output will be printed to stderr
```

### Example: Adjust Retry Behavior

```sql
-- Increase retries for unreliable networks
SET webdav_max_retries = 10;

-- Writes and reads will retry up to 10 times on transient failures
COPY my_table TO 'storagebox://u123456/data.parquet';
```

### Example: Configure Memory Usage

```sql
-- Reduce memory usage by streaming files larger than 10MB
SET webdav_streaming_threshold_mb = 10;

-- OR increase threshold for faster writes if you have enough RAM
SET webdav_streaming_threshold_mb = 500;

COPY large_table TO 'storagebox://u123456/large.parquet';
```

## Local development

### Building

```sh
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
make
```

### Running tests

```sh
make test
```

### Testing locally

```sh
./build/release/duckdb -unsigned
```

## License

MIT
