#pragma once

#include "postgres.h"

#include "common/relpath.h"
#include "storage/relfilelocator.h"

#include "utils/timestamp.h"

#include "smgr_stats_hist.h"
#include "smgr_stats_welford.h"

/* Metadata captured at entry creation time (from pg_class) */
typedef struct SmgrStatsEntryMeta {
  Oid reloid;          /* pg_class.oid - stable identifier */
  Oid main_reloid;     /* For TOAST/index: the main table's OID */
  char relkind;        /* 'r'=table, 'i'=index, 't'=toast, etc. */
  NameData relname;    /* Table/index name (64 bytes) */
  NameData nspname;    /* Schema name (64 bytes) */
  bool metadata_valid; /* True if metadata was successfully resolved */
} SmgrStatsEntryMeta;

/* Sequential run length distribution: Welford on completed streak lengths. */
typedef SmgrStatsWelford SmgrStatsRunDist;

/* Inter-arrival time burstiness: Welford on time between consecutive operations. */
typedef struct SmgrStatsBurstiness {
  SmgrStatsWelford iat;     /* Inter-arrival time statistics (microseconds) */
  TimestampTz last_op_time; /* Previous operation timestamp (NOT reset between periods) */
} SmgrStatsBurstiness;

typedef struct SmgrStatsKey {
  RelFileLocator locator;
  ForkNumber forknum;
} SmgrStatsKey;

typedef struct SmgrStatsEntry {
  SmgrStatsKey key; /* Must be first (dshash requirement) */

  /* Metadata from pg_class (captured at entry creation) */
  SmgrStatsEntryMeta meta;

  /* Operation counters */
  uint64 reads;
  uint64 read_blocks;
  uint64 writes;
  uint64 write_blocks;
  uint64 extends;
  uint64 extend_blocks;
  uint64 truncates;
  uint64 fsyncs;

  /* Timing histograms */
  SmgrStatsTimingHist read_timing;
  SmgrStatsTimingHist write_timing;

  /* Burstiness: inter-arrival time statistics */
  SmgrStatsBurstiness read_burst;
  SmgrStatsBurstiness write_burst;

  /* Sequential/random access counters */
  uint64 sequential_reads;
  uint64 random_reads;
  uint64 sequential_writes;
  uint64 random_writes;

  /* Sequential run length distribution (completed streaks) */
  SmgrStatsRunDist read_runs;
  SmgrStatsRunDist write_runs;

  /* Activity spread (for long collection intervals) */
  uint32 active_seconds;    /* Distinct seconds with any activity */
  int64 last_active_second; /* Truncated to second (for dedup) */

  /* Timestamps */
  TimestampTz first_access; /* Set once on entry creation */
  TimestampTz last_access;  /* Updated on every operation */
} SmgrStatsEntry;

/* Get or create an entry, returning it locked (exclusive). Caller must release. */
extern SmgrStatsEntry* smgr_stats_get_entry(const SmgrStatsKey* key, bool* found);

/* Find an existing entry (exclusive lock). Returns NULL if not found.
 * Safe in critical sections (no allocation). Hash must already be attached. */
extern SmgrStatsEntry* smgr_stats_find_entry(const SmgrStatsKey* key);

/* Release the lock on an entry obtained from smgr_stats_get_entry/find_entry. */
extern void smgr_stats_release_entry(SmgrStatsEntry* entry);

/* Iterate all entries (shared lock), snapshot without resetting.
 * Returns a palloc'd array of snapshots. Sets *count and *bucket_id
 * (the current in-progress bucket). */
extern SmgrStatsEntry* smgr_stats_snapshot(int* count, int64* bucket_id);

/* Iterate all entries with exclusive lock, snapshot and reset counters.
 * Returns a palloc'd array of snapshots. Sets *count and *bucket_id
 * (the bucket that was just completed). Advances the bucket counter. */
extern SmgrStatsEntry* smgr_stats_snapshot_and_reset(int* count, int64* bucket_id);

/* Resolve metadata from pg_class for an entry. Must be called from a backend with
 * the correct database connection. Returns true if metadata was resolved.
 * WARNING: This function accesses syscache which may trigger I/O. Do NOT call
 * while holding a dshash lock - use smgr_stats_lookup_metadata instead. */
extern bool smgr_stats_resolve_metadata(SmgrStatsEntry* entry, const SmgrStatsKey* key);

/* Lookup metadata from pg_class without modifying any entry. Returns metadata in
 * the output parameter. Safe to call without holding any locks. Returns true if
 * metadata was successfully resolved. */
extern bool smgr_stats_lookup_metadata(const SmgrStatsKey* key, SmgrStatsEntryMeta* meta_out);

/* Relfile association entry (for tracking VACUUM FULL/CLUSTER rewrites) */
typedef struct SmgrStatsRelfileAssoc {
  RelFileLocator old_locator;
  RelFileLocator new_locator;
  ForkNumber forknum;
  bool is_redo;
  /* Metadata resolved at create time */
  Oid reloid;
  NameData relname;
  NameData nspname;
} SmgrStatsRelfileAssoc;

/* Queue a relfile association for the background worker to persist. */
extern void smgr_stats_queue_relfile_assoc(const RelFileLocator* old_locator, const RelFileLocator* new_locator,
                                           ForkNumber forknum, bool is_redo);

/* Drain the relfile association queue. Returns a palloc'd array and sets *count. */
extern SmgrStatsRelfileAssoc* smgr_stats_drain_relfile_queue(int* count);
