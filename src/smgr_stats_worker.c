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

  PG_TRY();
  {
    for (int i = 0; i < count; i++) {
      SmgrStatsEntry* e = &snapshot[i];
      StringInfoData query;
      initStringInfo(&query);

      appendStringInfo(&query,
                       "INSERT INTO smgr_stats.history "
                       "(bucket_id, spcoid, dboid, relnumber, forknum,"
                       " reads, read_blocks, writes, write_blocks,"
                       " extends, extend_blocks, truncates, fsyncs,"
                       " read_hist, read_count, read_total_us, read_min_us, read_max_us,"
                       " write_hist, write_count, write_total_us, write_min_us, write_max_us,"
                       " read_iat_mean_us, read_iat_cov, write_iat_mean_us, write_iat_cov,"
                       " active_seconds, first_access, last_access) "
                       "VALUES (%ld, %u, %u, %u, %d,"
                       " %lu, %lu, %lu, %lu,"
                       " %lu, %lu, %lu, %lu, ",
                       (long)bucket_id, e->key.locator.spcOid, e->key.locator.dbOid, e->key.locator.relNumber,
                       (int)e->key.forknum, (unsigned long)e->reads, (unsigned long)e->read_blocks,
                       (unsigned long)e->writes, (unsigned long)e->write_blocks, (unsigned long)e->extends,
                       (unsigned long)e->extend_blocks, (unsigned long)e->truncates, (unsigned long)e->fsyncs);

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

      if (e->read_burst.iat.count >= 2) {
        appendStringInfo(&query, "%g, %g, ", e->read_burst.iat.mean, smgr_stats_welford_cov(&e->read_burst.iat));
      } else {
        appendStringInfoString(&query, "NULL, NULL, ");
      }

      if (e->write_burst.iat.count >= 2) {
        appendStringInfo(&query, "%g, %g, ", e->write_burst.iat.mean, smgr_stats_welford_cov(&e->write_burst.iat));
      } else {
        appendStringInfoString(&query, "NULL, NULL, ");
      }

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
      pgstat_report_activity(STATE_RUNNING, "collecting smgr stats");
      smgr_stats_collect_and_insert();
    }
  }

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
