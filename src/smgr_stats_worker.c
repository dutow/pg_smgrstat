#include "postgres.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "smgr_stats_guc.h"
#include "smgr_stats_store.h"
#include "smgr_stats_worker.h"

/* Signal handling state */
static volatile sig_atomic_t got_sigterm = 0;
static volatile sig_atomic_t got_sighup = 0;

static void sigterm_handler(SIGNAL_ARGS) {
  (void)postgres_signal_arg;
  int save_errno = errno;
  got_sigterm = 1;
  SetLatch(MyLatch);
  errno = save_errno;
}

static void sighup_handler(SIGNAL_ARGS) {
  (void)postgres_signal_arg;
  int save_errno = errno;
  got_sighup = 1;
  SetLatch(MyLatch);
  errno = save_errno;
}

static void welford_to_query(StringInfo query, const SmgrStatsWelford* w) {
  if (w->count >= 2) {
    appendStringInfo(query, "%g, %g, ", w->mean, smgr_stats_welford_cov(w));
  } else {
    appendStringInfoString(query, "NULL, NULL, ");
  }
}

static void append_name_or_null(StringInfo query, const NameData* name) {
  if (name->data[0] != '\0') {
    appendStringInfo(query, "'%s'", NameStr(*name));
  } else {
    appendStringInfoString(query, "NULL");
  }
}

static void append_oid_or_null(StringInfo query, Oid oid) {
  if (OidIsValid(oid)) {
    appendStringInfo(query, "%u", oid);
  } else {
    appendStringInfoString(query, "NULL");
  }
}

static void smgr_stats_collect_and_insert(void) {
  int count = 0;
  int64 bucket_id;
  SmgrStatsEntry* snapshot = smgr_stats_snapshot_and_reset(&count, &bucket_id);

  if (count == 0) {
    pfree(snapshot);
    return;
  }

  SetCurrentStatementStartTimestamp();
  StartTransactionCommand();
  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());

  /*
   * Resolve metadata for entries that don't have it yet.
   * We can resolve metadata for entries belonging to our database or global/shared
   * catalogs (dbOid=0).
   */
  for (int i = 0; i < count; i++) {
    SmgrStatsEntry* e = &snapshot[i];
    if (!e->meta.metadata_valid && (e->key.locator.dbOid == MyDatabaseId || e->key.locator.dbOid == 0)) {
      smgr_stats_resolve_metadata(e, &e->key);
    }
  }

  PG_TRY();
  {
    for (int i = 0; i < count; i++) {
      SmgrStatsEntry* e = &snapshot[i];
      StringInfoData query;
      initStringInfo(&query);

      appendStringInfo(&query,
                       "INSERT INTO smgr_stats.history "
                       "(bucket_id, spcoid, dboid, relnumber, forknum,"
                       " reloid, main_reloid, relname, nspname, relkind,"
                       " reads, read_blocks, writes, write_blocks,"
                       " extends, extend_blocks, truncates, fsyncs,"
                       " read_hist, read_count, read_total_us, read_min_us, read_max_us,"
                       " write_hist, write_count, write_total_us, write_min_us, write_max_us,"
                       " read_iat_mean_us, read_iat_cov, write_iat_mean_us, write_iat_cov,"
                       " sequential_reads, random_reads, sequential_writes, random_writes,"
                       " read_run_mean, read_run_cov, read_run_count,"
                       " write_run_mean, write_run_cov, write_run_count,"
                       " active_seconds, first_access, last_access) "
                       "VALUES (%ld, %u, %u, %u, %d, ",
                       (long)bucket_id, e->key.locator.spcOid, e->key.locator.dbOid, e->key.locator.relNumber,
                       (int)e->key.forknum);

      /* Metadata columns */
      append_oid_or_null(&query, e->meta.reloid);
      appendStringInfoString(&query, ", ");
      append_oid_or_null(&query, e->meta.main_reloid);
      appendStringInfoString(&query, ", ");
      append_name_or_null(&query, &e->meta.relname);
      appendStringInfoString(&query, ", ");
      append_name_or_null(&query, &e->meta.nspname);
      appendStringInfoString(&query, ", ");
      if (e->meta.relkind != '\0') {
        appendStringInfo(&query, "'%c'", e->meta.relkind);
      } else {
        appendStringInfoString(&query, "NULL");
      }

      /* Stats columns */
      appendStringInfo(&query,
                       ", %lu, %lu, %lu, %lu,"
                       " %lu, %lu, %lu, %lu, ",
                       (unsigned long)e->reads, (unsigned long)e->read_blocks, (unsigned long)e->writes,
                       (unsigned long)e->write_blocks, (unsigned long)e->extends, (unsigned long)e->extend_blocks,
                       (unsigned long)e->truncates, (unsigned long)e->fsyncs);

      if (e->read_timing.count > 0) {
        appendStringInfoString(&query, "ARRAY[");
        for (int b = 0; b < SMGR_STATS_HIST_BINS; b++) {
          if (b > 0) {
            appendStringInfoChar(&query, ',');
          }
          appendStringInfo(&query, "%lu", (unsigned long)e->read_timing.bins[b]);
        }
        appendStringInfo(&query, "]::bigint[], %lu, %lu, %lu, %lu, ", (unsigned long)e->read_timing.count,
                         (unsigned long)e->read_timing.total_us, (unsigned long)e->read_timing.min_us,
                         (unsigned long)e->read_timing.max_us);
      } else {
        appendStringInfoString(&query, "NULL, NULL, NULL, NULL, NULL, ");
      }

      if (e->write_timing.count > 0) {
        appendStringInfoString(&query, "ARRAY[");
        for (int b = 0; b < SMGR_STATS_HIST_BINS; b++) {
          if (b > 0) {
            appendStringInfoChar(&query, ',');
          }
          appendStringInfo(&query, "%lu", (unsigned long)e->write_timing.bins[b]);
        }
        appendStringInfo(&query, "]::bigint[], %lu, %lu, %lu, %lu, ", (unsigned long)e->write_timing.count,
                         (unsigned long)e->write_timing.total_us, (unsigned long)e->write_timing.min_us,
                         (unsigned long)e->write_timing.max_us);
      } else {
        appendStringInfoString(&query, "NULL, NULL, NULL, NULL, NULL, ");
      }

      welford_to_query(&query, &e->read_burst.iat);
      welford_to_query(&query, &e->write_burst.iat);

      appendStringInfo(&query, "%lu, %lu, %lu, %lu, ", (unsigned long)e->sequential_reads,
                       (unsigned long)e->random_reads, (unsigned long)e->sequential_writes,
                       (unsigned long)e->random_writes);

      welford_to_query(&query, &e->read_runs);
      appendStringInfo(&query, "%lu, ", (unsigned long)e->read_runs.count);
      welford_to_query(&query, &e->write_runs);
      appendStringInfo(&query, "%lu, ", (unsigned long)e->write_runs.count);

      appendStringInfo(&query, "%u, '%s', '%s')", e->active_seconds, timestamptz_to_str(e->first_access),
                       timestamptz_to_str(e->last_access));

      SPI_execute(query.data, false, 0);
      pfree(query.data);
    }

    PopActiveSnapshot();
    CommitTransactionCommand();
  }
  PG_FINALLY();
  {
    SPI_finish();
  }
  PG_END_TRY();

  pfree(snapshot);
}

static void smgr_stats_insert_relfile_history(void) {
  int count = 0;
  SmgrStatsRelfileAssoc* assocs = smgr_stats_drain_relfile_queue(&count);

  if (count == 0) {
    return;
  }

  SetCurrentStatementStartTimestamp();
  StartTransactionCommand();
  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());

  PG_TRY();
  {
    for (int i = 0; i < count; i++) {
      SmgrStatsRelfileAssoc* a = &assocs[i];
      StringInfoData query;
      initStringInfo(&query);

      const char* is_redo_str = a->is_redo ? "true" : "false";  // NOLINT(readability-implicit-bool-conversion)
      appendStringInfo(&query,
                       "INSERT INTO smgr_stats.relfile_history "
                       "(spcoid, dboid, old_relnumber, new_relnumber, forknum, is_redo, reloid, relname, nspname) "
                       "VALUES (%u, %u, %u, %u, %d, %s, ",
                       a->new_locator.spcOid, a->new_locator.dbOid, a->old_locator.relNumber, a->new_locator.relNumber,
                       (int)a->forknum, is_redo_str);

      append_oid_or_null(&query, a->reloid);
      appendStringInfoString(&query, ", ");
      append_name_or_null(&query, &a->relname);
      appendStringInfoString(&query, ", ");
      append_name_or_null(&query, &a->nspname);
      appendStringInfoChar(&query, ')');

      SPI_execute(query.data, false, 0);
      pfree(query.data);
    }

    PopActiveSnapshot();
    CommitTransactionCommand();
  }
  PG_FINALLY();
  {
    SPI_finish();
  }
  PG_END_TRY();

  pfree(assocs);
}

static void smgr_stats_run_retention(void) {
  if (smgr_stats_retention_hours <= 0) {
    return; /* retention disabled */
  }

  SetCurrentStatementStartTimestamp();
  StartTransactionCommand();
  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());

  PG_TRY();
  {
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query, "DELETE FROM smgr_stats.history WHERE collected_at < now() - interval '%d hours'",
                     smgr_stats_retention_hours);
    SPI_execute(query.data, false, 0);
    pfree(query.data);

    PopActiveSnapshot();
    CommitTransactionCommand();
  }
  PG_FINALLY();
  {
    SPI_finish();
  }
  PG_END_TRY();
}

static void smgr_stats_collect_cycle(void) {
  pgstat_report_activity(STATE_RUNNING, "collecting smgr stats");
  smgr_stats_collect_and_insert();
  smgr_stats_insert_relfile_history();
  smgr_stats_run_retention();
  pgstat_report_activity(STATE_IDLE, NULL);
}

PGDLLEXPORT void smgr_stats_worker_main(Datum main_arg);

void smgr_stats_worker_main(Datum main_arg) {
  (void)main_arg;
  /* Set up signal handlers */
  pqsignal(SIGTERM, sigterm_handler);
  pqsignal(SIGHUP, sighup_handler);
  BackgroundWorkerUnblockSignals();

  /* Connect to the configured database */
  BackgroundWorkerInitializeConnection(smgr_stats_database, NULL, 0);

  /* Bootstrap: create the extension if it doesn't exist */
  SetCurrentStatementStartTimestamp();
  StartTransactionCommand();
  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());

  SPI_execute("CREATE EXTENSION IF NOT EXISTS pg_smgrstat", false, 0);

  PopActiveSnapshot();
  SPI_finish();
  CommitTransactionCommand();

  elog(LOG, "pg_smgrstat: worker started, collection_interval=%d", smgr_stats_collection_interval);

  /* Main loop */
  while (!got_sigterm) {
    int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, smgr_stats_collection_interval * 1000L,
                       PG_WAIT_EXTENSION);

    ResetLatch(MyLatch);

    if (got_sigterm) {
      break;
    }

    if (got_sighup) {
      got_sighup = 0;
      ProcessConfigFile(PGC_SIGHUP);
    }

    /* On timeout, collect stats */
    if (rc & WL_TIMEOUT) {
      smgr_stats_collect_cycle();
    }
  }

  /* Final collection: capture any stats flushed by exiting backends */
  smgr_stats_collect_cycle();

  proc_exit(0);
}

void smgr_stats_register_worker(void) {
  BackgroundWorker worker = {0};

  snprintf(worker.bgw_name, BGW_MAXLEN, "pg_smgrstat collector");
  snprintf(worker.bgw_type, BGW_MAXLEN, "pg_smgrstat collector");
  snprintf(worker.bgw_library_name, MAXPGPATH, "pg_smgrstat");
  snprintf(worker.bgw_function_name, BGW_MAXLEN, "smgr_stats_worker_main");

  worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
  worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
  worker.bgw_restart_time = 10;
  worker.bgw_main_arg = Int32GetDatum(0);
  worker.bgw_notify_pid = 0;

  RegisterBackgroundWorker(&worker);
}
