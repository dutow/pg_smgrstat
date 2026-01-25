#include "postgres.h"

#include "datatype/timestamp.h"
#include "portability/instr_time.h"
#include "storage/aio.h"
#include "storage/smgr.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"

#include "smgr_stats_guc.h"
#include "smgr_stats_link.h"
#include "smgr_stats_metadata.h"
#include "smgr_stats_seq.h"
#include "smgr_stats_store.h"

/*
 * Determine the tracking key for an I/O operation, handling temp table modes.
 * Returns false if this operation should not be tracked (temp table with mode=off).
 */
static inline bool smgr_stats_determine_key(SMgrRelation reln, ForkNumber forknum, SmgrStatsKey* key_out) {
  if (SmgrIsTemp(reln)) {
    switch ((SmgrStatsTempTracking)smgr_stats_track_temp_tables) {
      case SMGR_STATS_TEMP_OFF:
        return false;
      case SMGR_STATS_TEMP_INDIVIDUAL:
        break; /* Use real key below */
      case SMGR_STATS_TEMP_AGGREGATE:
        *key_out = smgr_stats_temp_aggregate_key(reln->smgr_rlocator.locator.dbOid);
        return true;
    }
  }
  *key_out = (SmgrStatsKey){.locator = reln->smgr_rlocator.locator, .forknum = forknum};
  return true;
}

static inline void smgr_stats_update_activity(SmgrStatsEntry* entry, TimestampTz now) {
  if (entry->first_access == 0) {
    entry->first_access = now;
  }
  entry->last_access = now;
  int64 current_second = now / USECS_PER_SEC;
  if (current_second != entry->last_active_second) {
    entry->active_seconds++;
    entry->last_active_second = current_second;
  }
}

static inline void smgr_stats_record_burstiness(SmgrStatsBurstiness* burst, TimestampTz now) {
  if (burst->last_op_time != 0) {
    double iat_us = (double)(now - burst->last_op_time);
    smgr_stats_welford_record(&burst->iat, iat_us);
  }
  burst->last_op_time = now;
}

static PgAioHandleCallbackID smgr_stats_aio_cb_id = PGAIO_HCB_INVALID;

/* Per-AIO-slot state: populated at startreadv time, consumed at complete_local time. */
typedef struct SmgrStatsAioSlot {
  instr_time start_time;
  SmgrStatsSeqResult seq_result;
  SmgrStatsKey tracking_key;
  bool should_track;
} SmgrStatsAioSlot;

static SmgrStatsAioSlot* aio_slots = NULL;

/* Recursion guard for metadata resolution (which may trigger smgropen recursively) */

/* Flag to track when we're inside an I/O operation (prevents metadata resolution in smgr_open) */
static bool in_smgr_stats_io = false;

static PgAioResult smgr_stats_readv_complete(PgAioHandle* ioh, PgAioResult prior_result, uint8 cb_data) {
  (void)cb_data;
  if (prior_result.status != PGAIO_RS_OK) {
    return prior_result;
  }

  INJECTION_POINT("smgr-stats-aio-read-complete", NULL);

  if (!aio_slots) {
    return prior_result;
  }

  int slot = pgaio_io_get_id(ioh) % io_max_concurrency;

  /* Skip if we decided not to track this at startreadv time */
  if (!aio_slots[slot].should_track) {
    return prior_result;
  }

  PgAioTargetData* td = pgaio_io_get_target_data(ioh);
  SmgrStatsSeqResult seq = aio_slots[slot].seq_result;

  SmgrStatsEntry* entry = smgr_stats_find_entry(&aio_slots[slot].tracking_key);
  if (entry) {
    entry->reads++;
    entry->read_blocks += td->smgr.nblocks;
    if (seq.is_sequential) {
      entry->sequential_reads++;
    } else {
      entry->random_reads++;
    }
    if (seq.completed_run > 0) {
      smgr_stats_welford_record(&entry->read_runs, (double)seq.completed_run);
    }

    instr_time end;
    INSTR_TIME_SET_CURRENT(end);
    INSTR_TIME_SUBTRACT(end, aio_slots[slot].start_time);
    uint64 elapsed_us = INSTR_TIME_GET_MICROSEC(end);
    smgr_stats_hist_record(&entry->read_timing, elapsed_us);

    TimestampTz now = GetCurrentTimestamp();
    smgr_stats_record_burstiness(&entry->read_burst, now);
    smgr_stats_update_activity(entry, now);

    smgr_stats_release_entry(entry);

    /*
     * No metadata resolution here - AIO completion may trigger syscache access
     * which conflicts with AIO constraints. Metadata is resolved by the
     * background worker when collecting stats.
     */
  }

  return prior_result;
}

static const PgAioHandleCallbacks smgr_stats_aio_cbs = {
    .complete_local = smgr_stats_readv_complete,
};

static void smgr_stats_readv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, void** buffers,
                             BlockNumber nblocks, SmgrChainIndex chain_index) {
  instr_time start;
  INSTR_TIME_SET_CURRENT(start);

  in_smgr_stats_io = true;
  smgr_readv_next(reln, forknum, blocknum, buffers, nblocks, chain_index + 1);
  in_smgr_stats_io = false;
  INJECTION_POINT("smgr-stats-after-readv", NULL);

  SmgrStatsKey tracking_key;
  if (!smgr_stats_determine_key(reln, forknum, &tracking_key)) {
    return; /* Temp table with tracking=off */
  }

  instr_time end;
  INSTR_TIME_SET_CURRENT(end);
  INSTR_TIME_SUBTRACT(end, start);
  uint64 elapsed_us = INSTR_TIME_GET_MICROSEC(end);

  /* Use real key for sequential detection (preserves accuracy even in aggregate mode) */
  SmgrStatsKey real_key = {.locator = reln->smgr_rlocator.locator, .forknum = forknum};
  SmgrStatsSeqResult seq = smgr_stats_check_sequential(&real_key, blocknum, nblocks, true);

  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&tracking_key, &found);
  if (!found && !smgr_stats_is_temp_aggregate_key(&tracking_key)) {
    smgr_stats_add_pending_metadata(&tracking_key);
  }
  entry->reads++;
  entry->read_blocks += nblocks;
  if (seq.is_sequential) {
    entry->sequential_reads++;
  } else {
    entry->random_reads++;
  }
  if (seq.completed_run > 0) {
    smgr_stats_welford_record(&entry->read_runs, (double)seq.completed_run);
  }
  smgr_stats_hist_record(&entry->read_timing, elapsed_us);
  TimestampTz now = GetCurrentTimestamp();
  smgr_stats_record_burstiness(&entry->read_burst, now);
  smgr_stats_update_activity(entry, now);
  smgr_stats_release_entry(entry);
}

static void smgr_stats_startreadv(PgAioHandle* ioh, SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                                  void** buffers, BlockNumber nblocks, SmgrChainIndex chain_index) {
  /* Lazily allocate per-slot state array */
  if (!aio_slots) {
    aio_slots = MemoryContextAllocZero(TopMemoryContext, io_max_concurrency * sizeof(SmgrStatsAioSlot));
  }

  int slot = pgaio_io_get_id(ioh) % io_max_concurrency;

  SmgrStatsKey tracking_key;
  bool should_track = smgr_stats_determine_key(reln, forknum, &tracking_key);

  aio_slots[slot].should_track = should_track;
  if (!should_track) {
    smgr_startreadv_next(ioh, reln, forknum, blocknum, buffers, nblocks, chain_index + 1);
    return;
  }

  aio_slots[slot].tracking_key = tracking_key;

  /* Ensure entry exists before I/O (so completion callback can find it without allocating) */
  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&tracking_key, &found);
  if (!found && !smgr_stats_is_temp_aggregate_key(&tracking_key)) {
    smgr_stats_add_pending_metadata(&tracking_key);
  }
  smgr_stats_release_entry(entry);

  INSTR_TIME_SET_CURRENT(aio_slots[slot].start_time);

  /* Use real key for sequential detection (preserves accuracy even in aggregate mode) */
  SmgrStatsKey real_key = {.locator = reln->smgr_rlocator.locator, .forknum = forknum};
  aio_slots[slot].seq_result = smgr_stats_check_sequential(&real_key, blocknum, nblocks, true);

  pgaio_io_register_callbacks(ioh, smgr_stats_aio_cb_id, 0);
  smgr_startreadv_next(ioh, reln, forknum, blocknum, buffers, nblocks, chain_index + 1);
}

static void smgr_stats_writev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const void** buffers,
                              BlockNumber nblocks, bool skip_fsync, SmgrChainIndex chain_index) {
  instr_time start;
  INSTR_TIME_SET_CURRENT(start);

  in_smgr_stats_io = true;
  smgr_writev_next(reln, forknum, blocknum, buffers, nblocks, skip_fsync, chain_index + 1);
  in_smgr_stats_io = false;
  INJECTION_POINT("smgr-stats-after-writev", NULL);

  SmgrStatsKey tracking_key;
  if (!smgr_stats_determine_key(reln, forknum, &tracking_key)) {
    return; /* Temp table with tracking=off */
  }

  instr_time end;
  INSTR_TIME_SET_CURRENT(end);
  INSTR_TIME_SUBTRACT(end, start);
  uint64 elapsed_us = INSTR_TIME_GET_MICROSEC(end);

  /* Use real key for sequential detection (preserves accuracy even in aggregate mode) */
  SmgrStatsKey real_key = {.locator = reln->smgr_rlocator.locator, .forknum = forknum};
  SmgrStatsSeqResult seq = smgr_stats_check_sequential(&real_key, blocknum, nblocks, false);

  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&tracking_key, &found);
  if (!found && !smgr_stats_is_temp_aggregate_key(&tracking_key)) {
    smgr_stats_add_pending_metadata(&tracking_key);
  }
  entry->writes++;
  entry->write_blocks += nblocks;
  if (seq.is_sequential) {
    entry->sequential_writes++;
  } else {
    entry->random_writes++;
  }
  if (seq.completed_run > 0) {
    smgr_stats_welford_record(&entry->write_runs, (double)seq.completed_run);
  }
  smgr_stats_hist_record(&entry->write_timing, elapsed_us);
  TimestampTz now = GetCurrentTimestamp();
  smgr_stats_record_burstiness(&entry->write_burst, now);
  smgr_stats_update_activity(entry, now);
  smgr_stats_release_entry(entry);
}

static void smgr_stats_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const void* buffer,
                              bool skip_fsync, SmgrChainIndex chain_index) {
  smgr_extend_next(reln, forknum, blocknum, buffer, skip_fsync, chain_index + 1);

  SmgrStatsKey tracking_key;
  if (!smgr_stats_determine_key(reln, forknum, &tracking_key)) {
    return;
  }

  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&tracking_key, &found);
  if (!found && !smgr_stats_is_temp_aggregate_key(&tracking_key)) {
    smgr_stats_add_pending_metadata(&tracking_key);
  }
  entry->extends++;
  entry->extend_blocks++;
  smgr_stats_update_activity(entry, GetCurrentTimestamp());
  smgr_stats_release_entry(entry);
}

static void smgr_stats_zeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, int nblocks,
                                  bool skip_fsync, SmgrChainIndex chain_index) {
  smgr_zeroextend_next(reln, forknum, blocknum, nblocks, skip_fsync, chain_index + 1);

  SmgrStatsKey tracking_key;
  if (!smgr_stats_determine_key(reln, forknum, &tracking_key)) {
    return;
  }

  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&tracking_key, &found);
  if (!found && !smgr_stats_is_temp_aggregate_key(&tracking_key)) {
    smgr_stats_add_pending_metadata(&tracking_key);
  }
  entry->extends++;
  entry->extend_blocks += nblocks;
  smgr_stats_update_activity(entry, GetCurrentTimestamp());
  smgr_stats_release_entry(entry);
}

static void smgr_stats_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber old_nblocks, BlockNumber nblocks,
                                SmgrChainIndex chain_index) {
  smgr_truncate_next(reln, forknum, old_nblocks, nblocks, chain_index + 1);

  SmgrStatsKey tracking_key;
  if (!smgr_stats_determine_key(reln, forknum, &tracking_key)) {
    return;
  }

  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&tracking_key, &found);
  if (!found && !smgr_stats_is_temp_aggregate_key(&tracking_key)) {
    smgr_stats_add_pending_metadata(&tracking_key);
  }
  entry->truncates++;
  smgr_stats_update_activity(entry, GetCurrentTimestamp());
  smgr_stats_release_entry(entry);
}

static void smgr_stats_immedsync(SMgrRelation reln, ForkNumber forknum, SmgrChainIndex chain_index) {
  smgr_immedsync_next(reln, forknum, chain_index + 1);

  SmgrStatsKey tracking_key;
  if (!smgr_stats_determine_key(reln, forknum, &tracking_key)) {
    return;
  }

  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&tracking_key, &found);
  if (!found && !smgr_stats_is_temp_aggregate_key(&tracking_key)) {
    smgr_stats_add_pending_metadata(&tracking_key);
  }
  entry->fsyncs++;
  smgr_stats_update_activity(entry, GetCurrentTimestamp());
  smgr_stats_release_entry(entry);
}

static void smgr_stats_open(SMgrRelation reln, SmgrChainIndex chain_index) {
  smgr_open_next(reln, chain_index + 1);

  SmgrStatsKey tracking_key;
  if (!smgr_stats_determine_key(reln, MAIN_FORKNUM, &tracking_key)) {
    return;
  }

  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&tracking_key, &found);
  if (!found && !smgr_stats_is_temp_aggregate_key(&tracking_key)) {
    smgr_stats_add_pending_metadata(&tracking_key);
  }
  smgr_stats_release_entry(entry);
}

static void smgr_stats_create(RelFileLocator relold, SMgrRelation reln, ForkNumber forknum, bool is_redo,
                              SmgrChainIndex chain_index) {
  smgr_create_next(relold, reln, forknum, is_redo, chain_index + 1);

  /*
   * Track relfilenode associations for table rewrites (VACUUM FULL, CLUSTER,
   * TRUNCATE, REINDEX, ALTER TABLE SET TABLESPACE, etc.).
   * Skip for temp tables - they don't need relfile history tracking.
   */
  if (!SmgrIsTemp(reln) && relold.relNumber != 0 && relold.relNumber != reln->smgr_rlocator.locator.relNumber) {
    smgr_stats_queue_relfile_assoc(&relold, &reln->smgr_rlocator.locator, forknum, is_redo);
  }

  SmgrStatsKey tracking_key;
  if (!smgr_stats_determine_key(reln, forknum, &tracking_key)) {
    return;
  }

  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&tracking_key, &found);
  if (!found && !smgr_stats_is_temp_aggregate_key(&tracking_key)) {
    smgr_stats_add_pending_metadata(&tracking_key);
  }
  smgr_stats_release_entry(entry);
}

static const struct f_smgr smgr_stats_smgr = {
    .name = "smgr_stats",
    .chain_position = SMGR_CHAIN_MODIFIER,
    .smgr_open = smgr_stats_open,
    .smgr_create = smgr_stats_create,
    .smgr_readv = smgr_stats_readv,
    .smgr_startreadv = smgr_stats_startreadv,
    .smgr_writev = smgr_stats_writev,
    .smgr_extend = smgr_stats_extend,
    .smgr_zeroextend = smgr_stats_zeroextend,
    .smgr_truncate = smgr_stats_truncate,
    .smgr_immedsync = smgr_stats_immedsync,
};

void smgr_stats_register_link(void) {
  smgr_stats_aio_cb_id = pgaio_io_register_callback_entry(&smgr_stats_aio_cbs, "smgr_stats_readv");
  smgr_register(&smgr_stats_smgr, 0);
}
