#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "commands/dbcommands.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "smgr_stats_metadata.h"
#include "smgr_stats_store.h"

/* Backend-local list of keys needing metadata resolution */
static List* pending_metadata_keys = NIL;

/* Previous hook values for chaining */
static ExecutorEnd_hook_type prev_executor_end_hook = NULL;
static ProcessUtility_hook_type prev_process_utility_hook = NULL;
static bool hooks_registered = false;

void smgr_stats_add_pending_metadata(const SmgrStatsKey* key) {
  /* Only track entries for our database (or global entries) */
  if (key->locator.dbOid != MyDatabaseId && key->locator.dbOid != 0) {
    return;
  }

  MemoryContext old_ctx = MemoryContextSwitchTo(TopMemoryContext);
  SmgrStatsKey* key_copy = palloc(sizeof(SmgrStatsKey));
  *key_copy = *key;
  pending_metadata_keys = lappend(pending_metadata_keys, key_copy);
  MemoryContextSwitchTo(old_ctx);
}

void smgr_stats_resolve_pending_metadata(void) {
  if (pending_metadata_keys == NIL) {
    return;
  }

  /*
   * Resolve each pending entry using the release-lookup-reacquire pattern:
   * 1. Find entry and check if metadata needs resolution (holding lock)
   * 2. Release lock before syscache access (which may trigger I/O)
   * 3. Do syscache lookup (no lock held - I/O is safe)
   * 4. If lookup succeeded, re-acquire lock and set metadata if still needed
   */
  ListCell* lc;
  foreach (lc, pending_metadata_keys) {
    SmgrStatsKey* key = lfirst(lc);

    /* Resolve entries for our database and global/shared catalogs (dbOid=0) */
    if (key->locator.dbOid == MyDatabaseId || key->locator.dbOid == 0) {
      /* Step 1: Check if resolution is needed (holding lock) */
      SmgrStatsEntry* stats = smgr_stats_find_entry(key);
      if (stats != NULL && !stats->meta.metadata_valid) {
        /* Step 2: Release lock before syscache access */
        smgr_stats_release_entry(stats);

        /* Step 3: Do syscache lookup without holding any lock */
        SmgrStatsEntryMeta resolved_meta;
        if (smgr_stats_lookup_metadata(key, &resolved_meta)) {
          /* Step 4: Re-acquire lock and check again */
          stats = smgr_stats_find_entry(key);
          if (stats != NULL) {
            /* Check again - another backend or the background worker may have
             * already resolved the metadata while we didn't hold the lock */
            if (!stats->meta.metadata_valid) {
              stats->meta = resolved_meta;
            }
            smgr_stats_release_entry(stats);
          }
        }
        /* If lookup failed or entry was removed, nothing to do */
      } else if (stats != NULL) {
        /* Metadata already valid, just release the lock */
        smgr_stats_release_entry(stats);
      }
    }
  }

  list_free_deep(pending_metadata_keys);
  pending_metadata_keys = NIL;
}

/*
 * Update entry metadata and propagate to other forks.
 * Takes ownership of the entry lock (releases it before returning).
 */
static void update_entry_and_forks(SmgrStatsEntry* entry, SmgrStatsKey* key, const SmgrStatsEntryMeta* meta) {
  entry->meta = *meta;
  smgr_stats_release_entry(entry);

  /* Also update other forks if they have entries */
  for (ForkNumber forknum = MAIN_FORKNUM + 1; forknum <= MAX_FORKNUM; forknum++) {
    key->forknum = forknum;
    entry = smgr_stats_find_entry(key);
    if (entry != NULL) {
      if (!entry->meta.metadata_valid) {
        entry->meta = *meta;
      }
      smgr_stats_release_entry(entry);
    }
  }
}

/*
 * Check if a pg_class tuple should be processed for metadata resolution.
 * Returns the relfilenumber if valid, InvalidRelFileNumber otherwise.
 */
static RelFileNumber get_relfilenumber_if_trackable(Form_pg_class class_form) {
  if (class_form->relisshared) {
    return InvalidRelFileNumber;
  }
  if (!RELKIND_HAS_STORAGE(class_form->relkind)) {
    return InvalidRelFileNumber;
  }
  if (!RelFileNumberIsValid(class_form->relfilenode)) {
    return InvalidRelFileNumber;
  }
  return class_form->relfilenode;
}

/*
 * Temporary structure to hold pg_class info needed for metadata resolution.
 * We extract this while holding the buffer lock, then release both buffer
 * and dshash locks before doing syscache lookups.
 */
typedef struct PgClassMetaInfo {
  SmgrStatsKey key;
  Oid reloid;
  Oid relnamespace;
  char relkind;
  NameData relname;
  bool needs_resolution;
} PgClassMetaInfo;

/*
 * Build full metadata from extracted pg_class info.
 * This does syscache lookup so must NOT hold any dshash locks.
 */
static void build_metadata_from_info(const PgClassMetaInfo* info, SmgrStatsEntryMeta* meta) {
  memset(meta, 0, sizeof(SmgrStatsEntryMeta));
  meta->reloid = info->reloid;
  meta->relkind = info->relkind;
  namestrcpy(&meta->relname, NameStr(info->relname));

  HeapTuple nsp_tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(info->relnamespace));
  if (HeapTupleIsValid(nsp_tuple)) {
    Form_pg_namespace nsp_form = (Form_pg_namespace)GETSTRUCT(nsp_tuple);
    namestrcpy(&meta->nspname, NameStr(nsp_form->nspname));
    ReleaseSysCache(nsp_tuple);
  }

  meta->main_reloid = InvalidOid;
  meta->metadata_valid = true;
}

/*
 * Process a single pg_class page, collecting metadata info for any tracked relations.
 * Does NOT do syscache lookups - just extracts info from the page buffer.
 */
static int collect_metadata_from_page(Page page, BlockNumber blkno, Snapshot snapshot, Buffer buf, Oid db_oid,
                                      Oid tablespace_oid, PgClassMetaInfo* infos, int max_infos) {
  int count = 0;
  OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

  for (OffsetNumber offnum = FirstOffsetNumber; offnum <= maxoff && count < max_infos;
       offnum = OffsetNumberNext(offnum)) {
    ItemId itemid = PageGetItemId(page, offnum);

    if (!ItemIdIsUsed(itemid) || ItemIdIsDead(itemid) || ItemIdIsRedirected(itemid)) {
      continue;
    }

    HeapTupleData tuple;
    ItemPointerSet(&(tuple.t_self), blkno, offnum);
    tuple.t_data = (HeapTupleHeader)PageGetItem(page, itemid);
    tuple.t_len = ItemIdGetLength(itemid);
    tuple.t_tableOid = RelationRelationId;

    if (!HeapTupleSatisfiesVisibility(&tuple, snapshot, buf)) {
      continue;
    }

    Form_pg_class class_form = (Form_pg_class)GETSTRUCT(&tuple);
    RelFileNumber relfilenumber = get_relfilenumber_if_trackable(class_form);
    if (!RelFileNumberIsValid(relfilenumber)) {
      continue;
    }

    /* Build the key for this relation */
    PgClassMetaInfo* info = &infos[count];
    info->key.locator.spcOid = OidIsValid(class_form->reltablespace) ? class_form->reltablespace : tablespace_oid;
    info->key.locator.dbOid = db_oid;
    info->key.locator.relNumber = relfilenumber;
    info->key.forknum = MAIN_FORKNUM;

    /* Check if we have an entry that needs resolution */
    SmgrStatsEntry* entry = smgr_stats_find_entry(&info->key);
    if (entry == NULL) {
      continue;
    }

    info->needs_resolution = !entry->meta.metadata_valid;
    smgr_stats_release_entry(entry);

    if (!info->needs_resolution) {
      continue;
    }

    /* Extract info we need for metadata resolution */
    info->reloid = class_form->oid;
    info->relnamespace = class_form->relnamespace;
    info->relkind = class_form->relkind;
    namestrcpy(&info->relname, NameStr(class_form->relname));

    count++;
  }

  return count;
}

/*
 * Apply collected metadata to dshash entries.
 * Does syscache lookups and dshash updates.
 */
static void apply_collected_metadata(PgClassMetaInfo* infos, int count) {
  for (int i = 0; i < count; i++) {
    PgClassMetaInfo* info = &infos[i];

    /* Build full metadata (syscache lookup - no locks held) */
    SmgrStatsEntryMeta meta;
    build_metadata_from_info(info, &meta);

    /* Re-acquire entry and update if still needed */
    SmgrStatsEntry* entry = smgr_stats_find_entry(&info->key);
    if (entry != NULL) {
      if (!entry->meta.metadata_valid) {
        update_entry_and_forks(entry, &info->key, &meta);
      } else {
        smgr_stats_release_entry(entry);
      }
    }
  }
}

/*
 * Resolve metadata for all stats entries belonging to a newly created database
 * by scanning the database's pg_class directly (using ReadBufferWithoutRelcache).
 *
 * This is called after CREATE DATABASE completes. The new database's pg_class
 * is a copy of the template's pg_class and contains all the relfilenode -> relname
 * mappings we need.
 *
 * This technique is borrowed from PostgreSQL's ScanSourceDatabasePgClass() which
 * uses the same approach to identify relations during CREATE DATABASE.
 */
/* Maximum number of entries to collect per page (pg_class can have ~200 tuples per page) */
#define MAX_INFOS_PER_PAGE 256

static void resolve_metadata_for_new_database(Oid db_oid, Oid tablespace_oid) {
  RelFileLocator rlocator = {.spcOid = tablespace_oid, .dbOid = db_oid, .relNumber = RelationRelationId};

  SMgrRelation smgr = smgropen(rlocator, INVALID_PROC_NUMBER);
  BlockNumber nblocks = smgrnblocks(smgr, MAIN_FORKNUM);
  smgrclose(smgr);

  if (nblocks == 0) {
    return;
  }

  BufferAccessStrategy bstrategy = GetAccessStrategy(BAS_BULKREAD);
  Snapshot snapshot = RegisterSnapshot(GetLatestSnapshot());
  PgClassMetaInfo* infos = palloc(sizeof(PgClassMetaInfo) * MAX_INFOS_PER_PAGE);

  for (BlockNumber blkno = 0; blkno < nblocks; blkno++) {
    CHECK_FOR_INTERRUPTS();

    Buffer buf = ReadBufferWithoutRelcache(rlocator, MAIN_FORKNUM, blkno, RBM_NORMAL, bstrategy, true);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(buf);

    int count = 0;
    if (!PageIsNew(page) && !PageIsEmpty(page)) {
      /* Collect metadata info from page - this only accesses the page buffer, no syscache */
      count = collect_metadata_from_page(page, blkno, snapshot, buf, db_oid, tablespace_oid, infos, MAX_INFOS_PER_PAGE);
    }

    /* Release buffer lock BEFORE doing syscache lookups */
    UnlockReleaseBuffer(buf);

    /* Now apply the metadata - this does syscache lookups (safe, no locks held) */
    if (count > 0) {
      apply_collected_metadata(infos, count);
    }
  }

  pfree(infos);
  UnregisterSnapshot(snapshot);
  FreeAccessStrategy(bstrategy);
}

/* ExecutorEnd hook - called after DML queries complete */
static void smgr_stats_executor_end(QueryDesc* query_desc) {
  if (prev_executor_end_hook) {
    prev_executor_end_hook(query_desc);
  } else {
    standard_ExecutorEnd(query_desc);
  }

  /* Resolve pending metadata after query completes */
  smgr_stats_resolve_pending_metadata();
}

/* ProcessUtility hook - called after DDL/utility statements complete */
static void smgr_stats_process_utility(PlannedStmt* pstmt, const char* query_string, bool read_only_tree,
                                       ProcessUtilityContext context, ParamListInfo params, QueryEnvironment* query_env,
                                       DestReceiver* dest, QueryCompletion* qc) {
  Node* parse_tree = pstmt->utilityStmt;
  bool is_create_db = IsA(parse_tree, CreatedbStmt);
  char* new_db_name = NULL;

  /* Capture database name before execution for CREATE DATABASE */
  if (is_create_db) {
    CreatedbStmt* stmt = (CreatedbStmt*)parse_tree;
    new_db_name = pstrdup(stmt->dbname);
  }

  PG_TRY();
  {
    if (prev_process_utility_hook) {
      prev_process_utility_hook(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
    } else {
      standard_ProcessUtility(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
    }

    /* After successful CREATE DATABASE, resolve metadata for the new database */
    if (is_create_db && new_db_name != NULL) {
      Oid db_oid = get_database_oid(new_db_name, true);
      if (OidIsValid(db_oid)) {
        HeapTuple db_tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(db_oid));
        if (HeapTupleIsValid(db_tuple)) {
          Form_pg_database db_form = (Form_pg_database)GETSTRUCT(db_tuple);
          Oid tablespace_oid = db_form->dattablespace;
          ReleaseSysCache(db_tuple);

          /* Resolve metadata for all entries in the new database */
          resolve_metadata_for_new_database(db_oid, tablespace_oid);
        }
      }

      pfree(new_db_name);
      new_db_name = NULL;
    }
  }
  PG_FINALLY();
  {
    /* Resolve pending metadata even on failure */
    smgr_stats_resolve_pending_metadata();

    if (new_db_name != NULL) {
      pfree(new_db_name);
    }
  }
  PG_END_TRY();
}

/* before_shmem_exit callback - last chance to resolve metadata before backend exits */
static void smgr_stats_before_shmem_exit_callback(int code, Datum arg) {
  (void)code;
  (void)arg;
  smgr_stats_resolve_pending_metadata();
}

void smgr_stats_register_metadata_hooks(void) {
  if (hooks_registered) {
    return;
  }

  prev_executor_end_hook = ExecutorEnd_hook;
  ExecutorEnd_hook = smgr_stats_executor_end;

  prev_process_utility_hook = ProcessUtility_hook;
  ProcessUtility_hook = smgr_stats_process_utility;

  before_shmem_exit(smgr_stats_before_shmem_exit_callback, (Datum)0);

  hooks_registered = true;
}
