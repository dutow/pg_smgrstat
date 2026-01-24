#include "postgres.h"

#include "datatype/timestamp.h"
#include "portability/instr_time.h"
#include "storage/aio.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"

#include "smgr_stats_link.h"
#include "smgr_stats_store.h"

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

static PgAioHandleCallbackID smgr_stats_aio_cb_id = PGAIO_HCB_INVALID;
static instr_time* aio_start_times = NULL;

static PgAioResult smgr_stats_readv_complete(PgAioHandle* ioh, PgAioResult prior_result, uint8 cb_data) {
  (void)cb_data;
  if (prior_result.status != PGAIO_RS_OK) {
    return prior_result;
  }

  INJECTION_POINT("smgr-stats-aio-read-complete", NULL);

  PgAioTargetData* td = pgaio_io_get_target_data(ioh);

  SmgrStatsKey key = {.locator = td->smgr.rlocator, .forknum = td->smgr.forkNum};
  SmgrStatsEntry* entry = smgr_stats_find_entry(&key);
  if (entry) {
    entry->reads++;
    entry->read_blocks += td->smgr.nblocks;

    if (aio_start_times) {
      int slot = pgaio_io_get_id(ioh) % io_max_concurrency;
      instr_time end;
      INSTR_TIME_SET_CURRENT(end);
      INSTR_TIME_SUBTRACT(end, aio_start_times[slot]);
      uint64 elapsed_us = INSTR_TIME_GET_MICROSEC(end);
      smgr_stats_hist_record(&entry->read_timing, elapsed_us);
    }

    smgr_stats_update_activity(entry, GetCurrentTimestamp());
    smgr_stats_release_entry(entry);
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

  smgr_readv_next(reln, forknum, blocknum, buffers, nblocks, chain_index + 1);
  INJECTION_POINT("smgr-stats-after-readv", NULL);

  instr_time end;
  INSTR_TIME_SET_CURRENT(end);
  INSTR_TIME_SUBTRACT(end, start);
  uint64 elapsed_us = INSTR_TIME_GET_MICROSEC(end);

  SmgrStatsKey key = {.locator = reln->smgr_rlocator.locator, .forknum = forknum};
  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&key, &found);
  entry->reads++;
  entry->read_blocks += nblocks;
  smgr_stats_hist_record(&entry->read_timing, elapsed_us);
  smgr_stats_update_activity(entry, GetCurrentTimestamp());
  smgr_stats_release_entry(entry);
}

static void smgr_stats_startreadv(PgAioHandle* ioh, SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                                  void** buffers, BlockNumber nblocks, SmgrChainIndex chain_index) {
  /* Ensure entry exists before I/O (so completion callback can find it without allocating) */
  SmgrStatsKey key = {.locator = reln->smgr_rlocator.locator, .forknum = forknum};
  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&key, &found);
  smgr_stats_release_entry(entry);

  /* Lazily allocate start times array */
  if (!aio_start_times) {
    aio_start_times = MemoryContextAllocZero(TopMemoryContext, io_max_concurrency * sizeof(instr_time));
  }

  /* Record start time for this I/O handle */
  int slot = pgaio_io_get_id(ioh) % io_max_concurrency;
  INSTR_TIME_SET_CURRENT(aio_start_times[slot]);

  pgaio_io_register_callbacks(ioh, smgr_stats_aio_cb_id, 0);
  smgr_startreadv_next(ioh, reln, forknum, blocknum, buffers, nblocks, chain_index + 1);
}

static void smgr_stats_writev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const void** buffers,
                              BlockNumber nblocks, bool skip_fsync, SmgrChainIndex chain_index) {
  instr_time start;
  INSTR_TIME_SET_CURRENT(start);

  smgr_writev_next(reln, forknum, blocknum, buffers, nblocks, skip_fsync, chain_index + 1);
  INJECTION_POINT("smgr-stats-after-writev", NULL);

  instr_time end;
  INSTR_TIME_SET_CURRENT(end);
  INSTR_TIME_SUBTRACT(end, start);
  uint64 elapsed_us = INSTR_TIME_GET_MICROSEC(end);

  SmgrStatsKey key = {.locator = reln->smgr_rlocator.locator, .forknum = forknum};
  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&key, &found);
  entry->writes++;
  entry->write_blocks += nblocks;
  smgr_stats_hist_record(&entry->write_timing, elapsed_us);
  smgr_stats_update_activity(entry, GetCurrentTimestamp());
  smgr_stats_release_entry(entry);
}

static void smgr_stats_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const void* buffer,
                              bool skip_fsync, SmgrChainIndex chain_index) {
  smgr_extend_next(reln, forknum, blocknum, buffer, skip_fsync, chain_index + 1);

  SmgrStatsKey key = {.locator = reln->smgr_rlocator.locator, .forknum = forknum};
  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&key, &found);
  entry->extends++;
  entry->extend_blocks++;
  smgr_stats_update_activity(entry, GetCurrentTimestamp());
  smgr_stats_release_entry(entry);
}

static void smgr_stats_zeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, int nblocks,
                                  bool skip_fsync, SmgrChainIndex chain_index) {
  smgr_zeroextend_next(reln, forknum, blocknum, nblocks, skip_fsync, chain_index + 1);

  SmgrStatsKey key = {.locator = reln->smgr_rlocator.locator, .forknum = forknum};
  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&key, &found);
  entry->extends++;
  entry->extend_blocks += nblocks;
  smgr_stats_update_activity(entry, GetCurrentTimestamp());
  smgr_stats_release_entry(entry);
}

static void smgr_stats_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber old_nblocks, BlockNumber nblocks,
                                SmgrChainIndex chain_index) {
  smgr_truncate_next(reln, forknum, old_nblocks, nblocks, chain_index + 1);

  SmgrStatsKey key = {.locator = reln->smgr_rlocator.locator, .forknum = forknum};
  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&key, &found);
  entry->truncates++;
  smgr_stats_update_activity(entry, GetCurrentTimestamp());
  smgr_stats_release_entry(entry);
}

static void smgr_stats_immedsync(SMgrRelation reln, ForkNumber forknum, SmgrChainIndex chain_index) {
  smgr_immedsync_next(reln, forknum, chain_index + 1);

  SmgrStatsKey key = {.locator = reln->smgr_rlocator.locator, .forknum = forknum};
  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&key, &found);
  entry->fsyncs++;
  smgr_stats_update_activity(entry, GetCurrentTimestamp());
  smgr_stats_release_entry(entry);
}

static const struct f_smgr smgr_stats_smgr = {
    .name = "smgr_stats",
    .chain_position = SMGR_CHAIN_MODIFIER,
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
