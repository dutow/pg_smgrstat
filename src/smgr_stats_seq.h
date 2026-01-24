#pragma once
#include "postgres.h"

#include "storage/block.h"

#include "smgr_stats_store.h"

typedef struct SmgrStatsSeqResult {
  bool is_sequential;
  uint64 completed_run; /* 0 if no run was completed */
} SmgrStatsSeqResult;

/* Check whether an I/O operation continues a sequential streak.
 * Uses a backend-local cache; safe to call from AIO complete_local callbacks. */
extern SmgrStatsSeqResult smgr_stats_check_sequential(const SmgrStatsKey* key, BlockNumber blocknum,
                                                      BlockNumber nblocks, bool is_read);

/* Flush any in-progress sequential runs to shared memory (on_shmem_exit callback). */
extern void smgr_stats_flush_runs(int code, Datum arg);
