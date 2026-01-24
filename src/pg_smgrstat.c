#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "smgr_stats_guc.h"
#include "smgr_stats_link.h"
#include "smgr_stats_worker.h"

PG_MODULE_MAGIC;

void _PG_init(void) {
  if (!process_shared_preload_libraries_in_progress) {
    elog(ERROR, "pg_smgrstat must be loaded via shared_preload_libraries");
  }

  smgr_stats_register_gucs();
  smgr_stats_register_link();
  smgr_stats_register_worker();

  elog(LOG, "pg_smgrstat: loaded");
}
