#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace_d.h"
#include "lib/dshash.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/dsm_registry.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relfilenumbermap.h"
#include "utils/syscache.h"

#include "smgr_stats_store.h"

/* Ring buffer size for relfile associations. Must be power of 2. */
#define RELFILE_ASSOC_QUEUE_SIZE 1024

/* Ring buffer for relfile associations */
typedef struct SmgrStatsRelfileQueue {
  pg_atomic_uint64 head; /* Next slot to write */
  pg_atomic_uint64 tail; /* Next slot to read */
  SmgrStatsRelfileAssoc entries[RELFILE_ASSOC_QUEUE_SIZE];
} SmgrStatsRelfileQueue;

typedef struct SmgrStatsControl {
  pg_atomic_uint64 bucket_id;
  SmgrStatsRelfileQueue relfile_queue;
} SmgrStatsControl;

static dshash_table* stats_hash = NULL;
static SmgrStatsControl* stats_control = NULL;

static void stats_control_init(void* ptr, void* arg) {
  (void)arg;
  SmgrStatsControl* ctl = (SmgrStatsControl*)ptr;
  pg_atomic_init_u64(&ctl->bucket_id, 1);
  pg_atomic_init_u64(&ctl->relfile_queue.head, 0);
  pg_atomic_init_u64(&ctl->relfile_queue.tail, 0);
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
  smgr_stats_welford_reset(&entry->read_burst.iat);
  smgr_stats_welford_reset(&entry->write_burst.iat);
  /* last_op_time preserved for correct IAT across period boundaries */
  entry->sequential_reads = 0;
  entry->random_reads = 0;
  entry->sequential_writes = 0;
  entry->random_writes = 0;
  smgr_stats_welford_reset(&entry->read_runs);
  smgr_stats_welford_reset(&entry->write_runs);
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
    entry->read_burst.last_op_time = 0;
    entry->write_burst.last_op_time = 0;
    /* Initialize metadata fields */
    entry->meta.reloid = InvalidOid;
    entry->meta.main_reloid = InvalidOid;
    entry->meta.relkind = '\0';
    memset(&entry->meta.relname, 0, sizeof(NameData));
    memset(&entry->meta.nspname, 0, sizeof(NameData));
    entry->meta.metadata_valid = false;
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

/*
 * Direct pg_class scan by (reltablespace, relfilenode) using the index.
 * This works for temp tables (which RelidByRelfilenumber skips) because
 * we're in the same backend and PostgreSQL's visibility rules show us
 * only our own temp tables.
 */
static Oid lookup_relid_by_relfilenode_direct(Oid spc_oid, RelFileNumber rel_number) {
  Oid result = InvalidOid;
  ScanKeyData skey[2];
  SysScanDesc scan;
  HeapTuple tup;
  Relation class_rel;

  /*
   * pg_class stores reltablespace=0 for the database's default tablespace.
   * Convert actual tablespace OIDs to 0 for the lookup.
   */
  Oid lookup_spc = spc_oid;
  if (lookup_spc == MyDatabaseTableSpace) {
    lookup_spc = InvalidOid;
  }
  /* Also handle DEFAULTTABLESPACE_OID (pg_default = 1663) */
  if (lookup_spc == DEFAULTTABLESPACE_OID) {
    lookup_spc = InvalidOid;
  }

  ScanKeyInit(&skey[0], Anum_pg_class_reltablespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(lookup_spc));
  ScanKeyInit(&skey[1], Anum_pg_class_relfilenode, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(rel_number));

  class_rel = table_open(RelationRelationId, AccessShareLock);
  scan = systable_beginscan(class_rel, ClassTblspcRelfilenodeIndexId, true, NULL, 2, skey);

  tup = systable_getnext(scan);
  if (HeapTupleIsValid(tup)) {
    result = ((Form_pg_class)GETSTRUCT(tup))->oid;
  }

  systable_endscan(scan);
  table_close(class_rel, AccessShareLock);

  return result;
}

/*
 * Lookup metadata from pg_class without modifying any entry. Returns metadata
 * in the output parameter. Safe to call without holding any locks since it only
 * reads from syscache (which may trigger I/O, but that's fine without locks).
 */
bool smgr_stats_lookup_metadata(const SmgrStatsKey* key, SmgrStatsEntryMeta* meta_out) {
  Oid reloid;
  HeapTuple tuple;
  Form_pg_class class_form;
  Form_pg_namespace nsp_form;

  /* Initialize output to invalid state */
  memset(meta_out, 0, sizeof(SmgrStatsEntryMeta));
  meta_out->reloid = InvalidOid;
  meta_out->main_reloid = InvalidOid;
  meta_out->metadata_valid = false;

  /* Handle synthetic aggregate key for temp tables */
  if (smgr_stats_is_temp_aggregate_key(key)) {
    meta_out->reloid = InvalidOid;
    meta_out->relkind = 'T'; /* Custom marker for temp aggregate */
    namestrcpy(&meta_out->relname, "<temporary tables>");
    namestrcpy(&meta_out->nspname, "pg_temp");
    meta_out->metadata_valid = true;
    return true;
  }

  /*
   * Skip if relNumber is 0 (defensive check). In practice, actual I/O uses
   * real relfilenodes - even mapped relations (system catalogs) have real
   * relfilenodes on disk (from pg_filenode.map), just not stored in
   * pg_class.relfilenode. RelidByRelfilenumber checks the map first.
   */
  if (key->locator.relNumber == 0) {
    return false;
  }

  /* Skip if no database connection (system processes during startup) */
  if (!OidIsValid(MyDatabaseId)) {
    return false;
  }

  /* Skip if we're too early in bootstrap (IsTransactionState() false) */
  if (!IsTransactionState()) {
    return false;
  }

  /* Skip if we're in a critical section (holding buffer locks, etc.) */
  if (CritSectionCount > 0) {
    return false;
  }

  /*
   * First try RelidByRelfilenumber for permanent relations.
   * This uses a cache and is faster for repeated lookups.
   * Note: RelidByRelfilenumber explicitly doesn't work for temp tables.
   */
  reloid = RelidByRelfilenumber(key->locator.spcOid, key->locator.relNumber);
  if (!OidIsValid(reloid) && key->locator.spcOid != InvalidOid) {
    /* Fallback: try with InvalidOid (stored as 0 in pg_class for default tablespace) */
    reloid = RelidByRelfilenumber(InvalidOid, key->locator.relNumber);
  }

  /*
   * If RelidByRelfilenumber failed, try direct pg_class scan.
   * This works for temp tables since we're in the same backend and
   * can see our own temp tables via normal visibility rules.
   */
  if (!OidIsValid(reloid)) {
    reloid = lookup_relid_by_relfilenode_direct(key->locator.spcOid, key->locator.relNumber);
  }

  if (!OidIsValid(reloid)) {
    return false;
  }

  /* Look up relation tuple via syscache */
  tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(reloid));
  if (!HeapTupleIsValid(tuple)) {
    return false;
  }

  class_form = (Form_pg_class)GETSTRUCT(tuple);
  meta_out->reloid = reloid;
  meta_out->relkind = class_form->relkind;
  namestrcpy(&meta_out->relname, NameStr(class_form->relname));

  /* Look up namespace name */
  HeapTuple nsp_tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(class_form->relnamespace));
  if (HeapTupleIsValid(nsp_tuple)) {
    nsp_form = (Form_pg_namespace)GETSTRUCT(nsp_tuple);
    namestrcpy(&meta_out->nspname, NameStr(nsp_form->nspname));
    ReleaseSysCache(nsp_tuple);
  }

  /* For TOAST: find main table */
  if (class_form->relkind == RELKIND_TOASTVALUE) {
    Relation rel = RelationIdGetRelation(reloid);
    if (RelationIsValid(rel)) {
      meta_out->main_reloid = rel->rd_toastoid != InvalidOid ? InvalidOid : reloid;
      /* Toast tables have rd_rel->reltoastrelid pointing to themselves, but we need the parent.
       * The parent is stored in pg_class.reltoastrelid pointing TO this toast table. */
      SysScanDesc scan;
      ScanKeyData skey[1];
      HeapTuple parent_tuple;

      ScanKeyInit(&skey[0], Anum_pg_class_reltoastrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(reloid));

      Relation class_rel = table_open(RelationRelationId, AccessShareLock);
      scan = systable_beginscan(class_rel, InvalidOid, false, NULL, 1, skey);
      parent_tuple = systable_getnext(scan);
      if (HeapTupleIsValid(parent_tuple)) {
        Form_pg_class parent_form = (Form_pg_class)GETSTRUCT(parent_tuple);
        meta_out->main_reloid = parent_form->oid;
      }
      systable_endscan(scan);
      table_close(class_rel, AccessShareLock);

      RelationClose(rel);
    }
  }

  /* For index: find indexed table */
  if (class_form->relkind == RELKIND_INDEX || class_form->relkind == RELKIND_PARTITIONED_INDEX) {
    Relation index_rel = RelationIdGetRelation(reloid);
    if (RelationIsValid(index_rel)) {
      meta_out->main_reloid = index_rel->rd_index->indrelid;
      RelationClose(index_rel);
    }
  }

  ReleaseSysCache(tuple);
  meta_out->metadata_valid = true;
  return true;
}

/*
 * Resolve metadata for an entry that is already locked. This is kept for
 * backward compatibility with code that already holds a lock and knows
 * it's safe (e.g., background worker on snapshot copies).
 * WARNING: Do NOT call while holding a dshash lock on a live entry - the
 * syscache access may trigger I/O which can deadlock. Use the
 * lookup-then-apply pattern instead.
 */
bool smgr_stats_resolve_metadata(SmgrStatsEntry* entry, const SmgrStatsKey* key) {
  /* Skip if metadata already resolved */
  if (entry->meta.metadata_valid) {
    return true;
  }

  SmgrStatsEntryMeta meta;
  if (!smgr_stats_lookup_metadata(key, &meta)) {
    return false;
  }

  /* Copy the resolved metadata to the entry */
  entry->meta = meta;
  return true;
}

void smgr_stats_queue_relfile_assoc(const RelFileLocator* old_locator, const RelFileLocator* new_locator,
                                    ForkNumber forknum, bool is_redo) {
  SmgrStatsControl* ctl = get_control();
  SmgrStatsRelfileQueue* q = &ctl->relfile_queue;

  /* Try to claim a slot (simple compare-and-swap loop) */
  uint64 head;
  uint64 next;
  for (;;) {
    head = pg_atomic_read_u64(&q->head);
    uint64 tail = pg_atomic_read_u64(&q->tail);

    /* Check if queue is full */
    next = (head + 1) & (RELFILE_ASSOC_QUEUE_SIZE - 1);
    if (next == (tail & (RELFILE_ASSOC_QUEUE_SIZE - 1))) {
      /* Queue full, drop this association (not ideal but better than blocking) */
      elog(DEBUG1, "pg_smgrstat: relfile association queue full, dropping entry");
      return;
    }

    /* Try to claim the slot */
    if (pg_atomic_compare_exchange_u64(&q->head, &head, head + 1)) {
      break;
    }
    /* CAS failed, retry */
  }

  /* Fill in the slot */
  uint64 slot = head & (RELFILE_ASSOC_QUEUE_SIZE - 1);
  SmgrStatsRelfileAssoc* entry = &q->entries[slot];
  entry->old_locator = *old_locator;
  entry->new_locator = *new_locator;
  entry->forknum = forknum;
  entry->is_redo = is_redo;

  /*
   * Don't resolve metadata here - we may be holding locks that are incompatible
   * with catalog access. The worker can look up metadata when draining, or we
   * just accept NULL metadata for relfile history entries.
   */
  entry->reloid = InvalidOid;
  memset(&entry->relname, 0, sizeof(NameData));
  memset(&entry->nspname, 0, sizeof(NameData));
}

SmgrStatsRelfileAssoc* smgr_stats_drain_relfile_queue(int* count) {
  SmgrStatsControl* ctl = get_control();
  SmgrStatsRelfileQueue* q = &ctl->relfile_queue;

  uint64 head = pg_atomic_read_u64(&q->head);
  uint64 tail = pg_atomic_read_u64(&q->tail);

  /* Wrap-around safe calculation of queue length */
  int n = (int)((head - tail) & (RELFILE_ASSOC_QUEUE_SIZE - 1));
  if (head < tail) {
    /* This shouldn't happen with proper wrapping, but handle it defensively */
    n = 0;
  }

  if (n == 0) {
    *count = 0;
    return NULL;
  }

  SmgrStatsRelfileAssoc* result = palloc(sizeof(SmgrStatsRelfileAssoc) * n);
  for (int i = 0; i < n; i++) {
    uint64 slot = (tail + i) & (RELFILE_ASSOC_QUEUE_SIZE - 1);
    result[i] = q->entries[slot];
  }

  /* Advance tail to consume the entries */
  pg_atomic_write_u64(&q->tail, head);

  *count = n;
  return result;
}
