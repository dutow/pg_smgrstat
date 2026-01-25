# pg_smgrstat

A PostgreSQL extension that collects per-file I/O statistics at the storage manager (SMGR) level. It intercepts all low-level I/O operations and records detailed metrics including operation counts, timing distributions, access patterns, and burstiness—providing visibility that core PostgreSQL statistics don't offer.

**Primary use cases:**
- **Storage tiering decisions**: Identify cold files suitable for offloading to cheaper storage (e.g., S3)
- **I/O performance analysis**: Understand latency distributions with percentile-ready histograms
- **Workload characterization**: Distinguish sequential scans from random access patterns
- **Capacity planning**: Track per-table I/O trends over time

The extension stores historical data in a regular table, enabling time-series analysis via standard SQL.

## Collected Statistics

| Metric | Description |
|--------|-------------|
| `reads`, `read_blocks` | Read operation count and total blocks read |
| `writes`, `write_blocks` | Write operation count and total blocks written |
| `extends`, `extend_blocks` | Relation extension operations and blocks added |
| `truncates` | Truncation operations |
| `fsyncs` | Immediate sync operations |
| `sequential_reads`, `random_reads` | Per-backend sequential vs random read classification |
| `sequential_writes`, `random_writes` | Per-backend sequential vs random write classification |
| `read_hist`, `write_hist` | 32-bin log2 timing histograms (for percentile analysis) |
| `read_min_us`, `read_max_us` | Min/max read latencies |
| `write_min_us`, `write_max_us` | Min/max write latencies |
| `read_iat_mean_us`, `read_iat_cov` | Read burstiness (inter-arrival time mean and coefficient of variation) |
| `write_iat_mean_us`, `write_iat_cov` | Write burstiness |
| `read_run_mean`, `read_run_cov` | Sequential read run length distribution (mean blocks per streak) |
| `write_run_mean`, `write_run_cov` | Sequential write run length distribution |
| `active_seconds` | Distinct seconds with any activity (duty cycle tracking) |
| `first_access`, `last_access` | Timestamps of first and most recent access |

Additionally, relation metadata (`reloid`, `relname`, `nspname`, `relkind`) is captured at entry creation, enabling queries by table name even after `VACUUM FULL` changes the underlying file.

## Architecture

```
┌─────────────────────┐        ┌───────────────────────────┐        ┌──────────────────┐
│   SMGR Modifier     │ ─────> │   Shared Memory (DSA)     │ ─────> │ Background       │
│   (any backend)     │        │                           │  timer │ Worker           │
│                     │        │   dshash                  │        │                  │
│   Intercepts:       │        │                           │        │ - Snapshot+reset │
│   readv, startreadv │        │   key → SmgrStatsEntry    │        │ - INSERT via SPI │
│   writev, extend    │        │                           │        │ - Retention      │
│   truncate, fsync   │        │                           │        │                  │
└─────────────────────┘        └───────────────────────────┘        └────────┬─────────┘
                                                                             │ SPI
                                                                             v
                                                                    ┌──────────────────┐
                                                                    │ smgr_stats.      │
                                                                    │ history table    │
                                                                    └──────────────────┘
```

**Key design points:**

- **SMGR chain modifier**: Hooks into PostgreSQL's storage manager layer via the SMGR extensibility patch, intercepting all file I/O operations
- **dshash for dynamic sizing**: No fixed entry limit, grows via DSA—no restart required as workload changes
- **128-partition locking**: Excellent concurrency; the hot path only locks one partition at a time
- **Per-backend per-file sequential detection**: Each backend tracks its own access patterns locally before updating shared counters, avoiding false "random" classification when multiple backends scan sequentially
- **AIO callback for async timing**: Uses `complete_local` callback to measure actual I/O latency for asynchronous reads
- **Log2 histograms**: 32-bin power-of-2 histograms enable percentile queries (P50/P95/P99) and are mergeable across time periods
- **Welford's algorithm**: Used for burstiness (CoV of inter-arrival times) and sequential run length statistics—minimal memory, streaming computation

## Configuration

### GUC Variables

| Variable | Default | Context | Description |
|----------|---------|---------|-------------|
| `smgr_stats.database` | `postgres` | POSTMASTER | Database where history table is stored |
| `smgr_stats.collection_interval` | `60` | SIGHUP | Seconds between stats collection cycles |
| `smgr_stats.retention_hours` | `168` (7 days) | SIGHUP | Hours to retain history (0 = forever) |
| `smgr_stats.track_temp_tables` | `aggregate` | SUSET | Temp table tracking: `off`, `individual`, or `aggregate` |

### Automatic Table Management

The background worker automatically:
1. Creates the extension (`CREATE EXTENSION IF NOT EXISTS pg_smgrstat`) in the configured database
2. Upgrades the extension (`ALTER EXTENSION ... UPDATE`) when a new version is installed
3. Periodically adds new entries and deletes rows older than `retention_hours`

No manual schema setup is required—just add the extension to `shared_preload_libraries` and restart.

### SQL Interface

Note: this interface is only available in the collection database (`smgr_stats.database`).

```sql
-- View current (not yet collected) stats from shared memory
SELECT * FROM smgr_stats.current();

-- Query historical stats with human-readable names
SELECT * FROM smgr_stats.history_v;

-- Compute P95 read latency from histogram
SELECT relname, smgr_stats.hist_percentile(read_hist, 0.95) AS p95_read_us
FROM smgr_stats.history
WHERE read_count > 0;

-- Track a table across VACUUM FULL/CLUSTER (follows relfilenode changes)
SELECT * FROM smgr_stats.get_table_history(
    (SELECT oid FROM pg_database WHERE datname = 'some_database'),
    'public', 'my_table'
);
```

## Required Patches

This extension requires PostgreSQL built with the **SMGR extensibility patch** (not yet in PostgreSQL core). The patch is available at:

**[github.com/percona/postgres](https://github.com/percona/postgres/tree/patch_smgr)** — `patch_smgr` branch

This branch is based on PostgreSQL development (currently targeting PG19) and includes:
- SMGR chain/modifier infrastructure for intercepting I/O operations
- AIO (asynchronous I/O) extensibility for timing async reads

Build PostgreSQL from this branch before building pg_smgrstat.

## Building

The extension uses **Meson** (not PGXS/Make).

```sh
# Configure (point to your patched PostgreSQL installation)
meson setup build -Dpg_config=/path/to/pg_config

# Build
meson compile -C build

# Install
meson install -C build
```

### Configuration

Add to `postgresql.conf`:

```
shared_preload_libraries = 'pg_smgrstat'

# Optional: customize settings
smgr_stats.database = 'postgres'          # Where to store history
smgr_stats.collection_interval = 60       # Seconds between snapshots
smgr_stats.retention_hours = 168          # Keep 7 days of history
smgr_stats.track_temp_tables = 'aggregate' # off/individual/aggregate
```

Restart PostgreSQL after changing `shared_preload_libraries`.

### Running Tests

Integration tests use RSpec (Ruby):

```sh
# Install dependencies (first time)
bundle install

# Run tests
PG_CONFIG=/path/to/pg_config bundle exec rspec
```

Or via Meson (after `meson install`):

```sh
meson test -C build
```

## Performance

Initial benchmarks show **no measurable overhead**.

**Test methodology:**
- Data directory on tmpfs (eliminates disk variance)
- 16MB shared_buffers with 2.5GB dataset (forces I/O-heavy workload)
- Up to 64 concurrent clients

| Workload | Base TPS | With Extension | Overhead |
|----------|----------|----------------|----------|
| pgbench scale=100, 16 clients | 35,052 ± 77 | 35,127 ± 335 | -0.2% |
| pgbench scale=100, 64 clients | 41,054 ± 82 | 41,145 ± 61 | -0.2% |

The slight negative overhead (extension faster) is within measurement noise—effectively zero impact.

