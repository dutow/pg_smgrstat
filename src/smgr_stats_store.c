#include "postgres.h"

#include "lib/dshash.h"
#include "port/atomics.h"
#include "storage/dsm_registry.h"
#include "utils/memutils.h"

#include "smgr_stats_store.h"

typedef struct SmgrStatsControl {
  pg_atomic_uint64 bucket_id;
} SmgrStatsControl;

static dshash_table* stats_hash = NULL;
static SmgrStatsControl* stats_control = NULL;

static void stats_control_init(void* ptr, void* arg) {
  (void)arg;
  SmgrStatsControl* ctl = (SmgrStatsControl*)ptr;
  pg_atomic_init_u64(&ctl->bucket_id, 1);
}

static SmgrStatsControl* get_control(void) {
  if (!stats_control) {
    bool found;
    stats_control = GetNamedDSMSegment("pg_smgrstat_ctl", sizeof(SmgrStatsControl), stats_control_init, &found, NULL);
  }
  return stats_control;
}

static const dshash_parameters smgr_stats_hash_params = {
    .key_size = sizeof(SmgrStatsKey),
    .entry_size = sizeof(SmgrStatsEntry),
    .compare_function = dshash_memcmp,
    .hash_function = dshash_memhash,
    .copy_function = dshash_memcpy,
};

static dshash_table* get_hash(void) {
  if (!stats_hash) {
    bool found;
    stats_hash = GetNamedDSHash("pg_smgrstat", &smgr_stats_hash_params, &found);
  }
  return stats_hash;
}

static void smgr_stats_entry_reset(SmgrStatsEntry* entry) {
  entry->reads = 0;
  entry->read_blocks = 0;
  entry->writes = 0;
  entry->write_blocks = 0;
  entry->extends = 0;
  entry->extend_blocks = 0;
  entry->truncates = 0;
  entry->fsyncs = 0;
  smgr_stats_hist_reset(&entry->read_timing);
  smgr_stats_hist_reset(&entry->write_timing);
  entry->active_seconds = 0;
  /* last_active_second preserved for correct dedup across period boundaries */
  entry->first_access = 0;
  entry->last_access = 0;
}

SmgrStatsEntry* smgr_stats_get_entry(const SmgrStatsKey* key, bool* found) {
  SmgrStatsEntry* entry = dshash_find_or_insert(get_hash(), key, found);
  if (!*found) {
    smgr_stats_entry_reset(entry);
    entry->last_active_second = 0;
  }
  return entry;
}

SmgrStatsEntry* smgr_stats_find_entry(const SmgrStatsKey* key) { return dshash_find(get_hash(), key, true); }

void smgr_stats_release_entry(SmgrStatsEntry* entry) { dshash_release_lock(get_hash(), entry); }

static SmgrStatsEntry* snapshot_entries(int* count, bool reset) {
  dshash_table* hash = get_hash();
  dshash_seq_status seq;
  SmgrStatsEntry* entry;
  int capacity = 64;
  int n = 0;

  SmgrStatsEntry* result = palloc(sizeof(SmgrStatsEntry) * capacity);

  dshash_seq_init(&seq, hash, reset);
  while ((entry = dshash_seq_next(&seq)) != NULL) {
    /* Skip entries with no activity this period */
    if (entry->first_access == 0) {
      continue;
    }

    /* Grow array if needed */
    if (n >= capacity) {
      capacity *= 2;
      result = repalloc(result, sizeof(SmgrStatsEntry) * capacity);
    }

    /* Snapshot */
    result[n] = *entry;

    if (reset) {
      smgr_stats_entry_reset(entry);
    }

    n++;
  }
  dshash_seq_term(&seq);

  *count = n;
  return result;
}

SmgrStatsEntry* smgr_stats_snapshot(int* count, int64* bucket_id) {
  SmgrStatsControl* ctl = get_control();
  *bucket_id = (int64)pg_atomic_read_u64(&ctl->bucket_id);
  return snapshot_entries(count, false);
}

SmgrStatsEntry* smgr_stats_snapshot_and_reset(int* count, int64* bucket_id) {
  SmgrStatsControl* ctl = get_control();
  *bucket_id = (int64)pg_atomic_fetch_add_u64(&ctl->bucket_id, 1);
  return snapshot_entries(count, true);
}
