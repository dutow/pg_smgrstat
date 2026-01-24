#include "postgres.h"

#include "storage/aio.h"

#include "smgr_stats_link.h"
#include "smgr_stats_store.h"

static PgAioHandleCallbackID smgr_stats_aio_cb_id = PGAIO_HCB_INVALID;

static PgAioResult smgr_stats_readv_complete(PgAioHandle* ioh, PgAioResult prior_result, uint8 cb_data) {
  (void)cb_data;
  if (prior_result.status != PGAIO_RS_OK) {
    return prior_result;
  }

  PgAioTargetData* td = pgaio_io_get_target_data(ioh);

  SmgrStatsKey key = {.locator = td->smgr.rlocator, .forknum = td->smgr.forkNum};
  SmgrStatsEntry* entry = smgr_stats_find_entry(&key);
  if (entry) {
    entry->reads++;
    entry->read_blocks += td->smgr.nblocks;
    smgr_stats_release_entry(entry);
  }

  return prior_result;
}

static const PgAioHandleCallbacks smgr_stats_aio_cbs = {
    .complete_local = smgr_stats_readv_complete,
};

static void smgr_stats_readv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, void** buffers,
                             BlockNumber nblocks, SmgrChainIndex chain_index) {
  smgr_readv_next(reln, forknum, blocknum, buffers, nblocks, chain_index + 1);

  SmgrStatsKey key = {.locator = reln->smgr_rlocator.locator, .forknum = forknum};
  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&key, &found);
  entry->reads++;
  entry->read_blocks += nblocks;
  smgr_stats_release_entry(entry);
}

static void smgr_stats_startreadv(PgAioHandle* ioh, SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                                  void** buffers, BlockNumber nblocks, SmgrChainIndex chain_index) {
  /* Ensure entry exists before I/O (so completion callback can find it without allocating) */
  SmgrStatsKey key = {.locator = reln->smgr_rlocator.locator, .forknum = forknum};
  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&key, &found);
  smgr_stats_release_entry(entry);

  pgaio_io_register_callbacks(ioh, smgr_stats_aio_cb_id, 0);
  smgr_startreadv_next(ioh, reln, forknum, blocknum, buffers, nblocks, chain_index + 1);
}

static void smgr_stats_writev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const void** buffers,
                              BlockNumber nblocks, bool skip_fsync, SmgrChainIndex chain_index) {
  smgr_writev_next(reln, forknum, blocknum, buffers, nblocks, skip_fsync, chain_index + 1);

  SmgrStatsKey key = {.locator = reln->smgr_rlocator.locator, .forknum = forknum};
  bool found;
  SmgrStatsEntry* entry = smgr_stats_get_entry(&key, &found);
  entry->writes++;
  entry->write_blocks += nblocks;
  smgr_stats_release_entry(entry);
}

static const struct f_smgr smgr_stats_smgr = {
    .name = "smgr_stats",
    .chain_position = SMGR_CHAIN_MODIFIER,
    .smgr_readv = smgr_stats_readv,
    .smgr_startreadv = smgr_stats_startreadv,
    .smgr_writev = smgr_stats_writev,
};

void smgr_stats_register_link(void) {
  smgr_stats_aio_cb_id = pgaio_io_register_callback_entry(&smgr_stats_aio_cbs, "smgr_stats_readv");
  smgr_register(&smgr_stats_smgr, 0);
}
