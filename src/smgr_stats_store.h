#pragma once

#include "postgres.h"

#include "common/relpath.h"
#include "storage/relfilelocator.h"

#include "utils/timestamp.h"

#include "smgr_stats_hist.h"
#include "smgr_stats_welford.h"

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
