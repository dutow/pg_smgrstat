#include "postgres.h"

#include "access/htup_details.h"
#include "funcapi.h"
#include "utils/timestamp.h"

#include "smgr_stats_store.h"

typedef struct SmgrStatsCurrentCtx {
  SmgrStatsEntry* entries;
  int64 bucket_id;
  TimestampTz collected_at;
} SmgrStatsCurrentCtx;

PG_FUNCTION_INFO_V1(smgr_stats_current);

Datum smgr_stats_current(PG_FUNCTION_ARGS) {
  FuncCallContext* funcctx;
  SmgrStatsCurrentCtx* ctx;

  if (SRF_IS_FIRSTCALL()) {
    funcctx = SRF_FIRSTCALL_INIT();
    MemoryContext oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    ctx = palloc(sizeof(SmgrStatsCurrentCtx));
    int count;
    ctx->entries = smgr_stats_snapshot(&count, &ctx->bucket_id);
    ctx->collected_at = GetCurrentTimestamp();
    funcctx->user_fctx = ctx;
    funcctx->max_calls = count;

    TupleDesc tupdesc = CreateTemplateTupleDesc(10);
    TupleDescInitEntry(tupdesc, 1, "bucket_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 2, "collected_at", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, 3, "spcoid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, 4, "dboid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, 5, "relnumber", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, 6, "forknum", INT2OID, -1, 0);
    TupleDescInitEntry(tupdesc, 7, "reads", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 8, "read_blocks", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 9, "writes", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 10, "write_blocks", INT8OID, -1, 0);
    funcctx->tuple_desc = BlessTupleDesc(tupdesc);

    MemoryContextSwitchTo(oldctx);
  }

  funcctx = SRF_PERCALL_SETUP();
  ctx = funcctx->user_fctx;

  if (funcctx->call_cntr < funcctx->max_calls) {
    SmgrStatsEntry* e = &ctx->entries[funcctx->call_cntr];
    Datum values[10];
    bool nulls[10] = {false};

    values[0] = Int64GetDatum(ctx->bucket_id);
    values[1] = TimestampTzGetDatum(ctx->collected_at);
    values[2] = ObjectIdGetDatum(e->key.locator.spcOid);
    values[3] = ObjectIdGetDatum(e->key.locator.dbOid);
    values[4] = ObjectIdGetDatum(e->key.locator.relNumber);
    values[5] = Int16GetDatum((int16)e->key.forknum);
    values[6] = UInt64GetDatum(e->reads);
    values[7] = UInt64GetDatum(e->read_blocks);
    values[8] = UInt64GetDatum(e->writes);
    values[9] = UInt64GetDatum(e->write_blocks);

    HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
  }

  SRF_RETURN_DONE(funcctx);
}
