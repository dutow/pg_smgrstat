#include "postgres.h"

#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "storage/ipc.h"
#include "tcop/utility.h"
#include "utils/memutils.h"

#include "smgr_stats_metadata.h"
#include "smgr_stats_store.h"

/* Backend-local list of keys needing metadata resolution */
static List *pending_metadata_keys = NIL;

/* Previous hook values for chaining */
static ExecutorEnd_hook_type prev_executor_end_hook = NULL;
static ProcessUtility_hook_type prev_process_utility_hook = NULL;
static bool hooks_registered = false;

void smgr_stats_add_pending_metadata(const SmgrStatsKey *key) {
  /* Only track entries for our database (or global entries) */
  if (key->locator.dbOid != MyDatabaseId && key->locator.dbOid != 0) {
    return;
  }

  MemoryContext old_ctx = MemoryContextSwitchTo(TopMemoryContext);
  SmgrStatsKey *key_copy = palloc(sizeof(SmgrStatsKey));
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
  ListCell *lc;
  foreach (lc, pending_metadata_keys) {
    SmgrStatsKey *key = lfirst(lc);

    /* Resolve entries for our database and global/shared catalogs (dbOid=0) */
    if (key->locator.dbOid == MyDatabaseId || key->locator.dbOid == 0) {
      /* Step 1: Check if resolution is needed (holding lock) */
      SmgrStatsEntry *stats = smgr_stats_find_entry(key);
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

/* ExecutorEnd hook - called after DML queries complete */
static void smgr_stats_executor_end(QueryDesc *query_desc) {
  if (prev_executor_end_hook) {
    prev_executor_end_hook(query_desc);
  } else {
    standard_ExecutorEnd(query_desc);
  }

  /* Resolve pending metadata after query completes */
  smgr_stats_resolve_pending_metadata();
}

/* ProcessUtility hook - called after DDL/utility statements complete */
static void smgr_stats_process_utility(PlannedStmt *pstmt, const char *query_string, bool read_only_tree,
                                       ProcessUtilityContext context, ParamListInfo params,
                                       QueryEnvironment *query_env, DestReceiver *dest, QueryCompletion *qc) {
  PG_TRY();
  {
    if (prev_process_utility_hook) {
      prev_process_utility_hook(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
    } else {
      standard_ProcessUtility(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
    }
  }
  PG_FINALLY();
  {
    /* Resolve pending metadata even on failure */
    smgr_stats_resolve_pending_metadata();
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
