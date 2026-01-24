#include "postgres.h"

#include "storage/ipc.h"
#include "utils/hsearch.h"

#include "smgr_stats_seq.h"
#include "smgr_stats_store.h"

typedef struct SmgrStatsLocalPattern {
  SmgrStatsKey key;
  BlockNumber last_read_block;
  BlockNumber last_write_block;
  uint64 current_read_run;
  uint64 current_write_run;
} SmgrStatsLocalPattern;

static HTAB* local_pattern_cache = NULL;

static HTAB* get_pattern_cache(void) {
  if (!local_pattern_cache) {
    HASHCTL ctl = {
        .keysize = sizeof(SmgrStatsKey),
        .entrysize = sizeof(SmgrStatsLocalPattern),
    };
    local_pattern_cache = hash_create("smgr_stats_pattern", 64, &ctl, HASH_ELEM | HASH_BLOBS);
    before_shmem_exit(smgr_stats_flush_runs, (Datum)0);
  }
  return local_pattern_cache;
}

SmgrStatsSeqResult smgr_stats_check_sequential(const SmgrStatsKey* key, BlockNumber blocknum, BlockNumber nblocks,
                                               bool is_read) {
  SmgrStatsSeqResult result = {.is_sequential = false, .completed_run = 0};
  bool found;
  SmgrStatsLocalPattern* pat = hash_search(get_pattern_cache(), key, HASH_ENTER, &found);

  if (!found) {
    pat->key = *key;
    pat->last_read_block = InvalidBlockNumber;
    pat->last_write_block = InvalidBlockNumber;
    pat->current_read_run = 0;
    pat->current_write_run = 0;
  }

  BlockNumber* last_block;
  uint64* current_run;
  if (is_read) {
    last_block = &pat->last_read_block;
    current_run = &pat->current_read_run;
  } else {
    last_block = &pat->last_write_block;
    current_run = &pat->current_write_run;
  }

  if (*last_block != InvalidBlockNumber && blocknum == *last_block + 1) {
    /* Sequential: extend current run */
    result.is_sequential = true;
    *current_run += nblocks;
  } else {
    /* Random (or first access): complete previous run, start new one */
    result.completed_run = *current_run;
    *current_run = nblocks;
  }

  *last_block = blocknum + nblocks - 1;
  return result;
}

void smgr_stats_flush_runs(int code, Datum arg) {
  (void)code;
  (void)arg;
  if (!local_pattern_cache) {
    return;
  }

  HASH_SEQ_STATUS seq;
  SmgrStatsLocalPattern* pat;
  hash_seq_init(&seq, local_pattern_cache);

  while ((pat = hash_seq_search(&seq)) != NULL) {
    bool has_read_run = pat->current_read_run > 0;
    bool has_write_run = pat->current_write_run > 0;
    if (!has_read_run && !has_write_run) {
      continue;
    }

    SmgrStatsEntry* entry = smgr_stats_find_entry(&pat->key);
    if (!entry) {
      continue;
    }
    if (has_read_run) {
      smgr_stats_welford_record(&entry->read_runs, (double)pat->current_read_run);
    }
    if (has_write_run) {
      smgr_stats_welford_record(&entry->write_runs, (double)pat->current_write_run);
    }
    smgr_stats_release_entry(entry);
  }
}
