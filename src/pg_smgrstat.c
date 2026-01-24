#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"

PG_MODULE_MAGIC;

void _PG_init(void);

void _PG_init(void) {
  elog(LOG, "pg_smgrstat: loaded");
}
