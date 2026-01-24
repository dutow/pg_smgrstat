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

    TupleDesc tupdesc = CreateTemplateTupleDesc(31);
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
    TupleDescInitEntry(tupdesc, 11, "extends", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 12, "extend_blocks", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 13, "truncates", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 14, "fsyncs", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 15, "read_hist", INT8ARRAYOID, -1, 0);
    TupleDescInitEntry(tupdesc, 16, "read_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 17, "read_total_us", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 18, "read_min_us", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 19, "read_max_us", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 20, "write_hist", INT8ARRAYOID, -1, 0);
    TupleDescInitEntry(tupdesc, 21, "write_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 22, "write_total_us", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 23, "write_min_us", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 24, "write_max_us", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 25, "read_iat_mean_us", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 26, "read_iat_cov", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 27, "write_iat_mean_us", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 28, "write_iat_cov", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 29, "active_seconds", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, 30, "first_access", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, 31, "last_access", TIMESTAMPTZOID, -1, 0);
    funcctx->tuple_desc = BlessTupleDesc(tupdesc);

    MemoryContextSwitchTo(oldctx);
  }

  funcctx = SRF_PERCALL_SETUP();
  ctx = funcctx->user_fctx;

  if (funcctx->call_cntr < funcctx->max_calls) {
    SmgrStatsEntry* e = &ctx->entries[funcctx->call_cntr];
    Datum values[31];
    bool nulls[31] = {false};

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
    values[10] = UInt64GetDatum(e->extends);
    values[11] = UInt64GetDatum(e->extend_blocks);
    values[12] = UInt64GetDatum(e->truncates);
    values[13] = UInt64GetDatum(e->fsyncs);

    if (e->read_timing.count > 0) {
      values[14] = smgr_stats_hist_to_array_datum(&e->read_timing);
      values[15] = Int64GetDatum((int64)e->read_timing.count);
      values[16] = Int64GetDatum((int64)e->read_timing.total_us);
      values[17] = Int64GetDatum((int64)e->read_timing.min_us);
      values[18] = Int64GetDatum((int64)e->read_timing.max_us);
    } else {
      nulls[14] = true;
      nulls[15] = true;
      nulls[16] = true;
      nulls[17] = true;
      nulls[18] = true;
    }

    if (e->write_timing.count > 0) {
      values[19] = smgr_stats_hist_to_array_datum(&e->write_timing);
      values[20] = Int64GetDatum((int64)e->write_timing.count);
      values[21] = Int64GetDatum((int64)e->write_timing.total_us);
      values[22] = Int64GetDatum((int64)e->write_timing.min_us);
      values[23] = Int64GetDatum((int64)e->write_timing.max_us);
    } else {
      nulls[19] = true;
      nulls[20] = true;
      nulls[21] = true;
      nulls[22] = true;
      nulls[23] = true;
    }

    if (e->read_burst.iat.count >= 2) {
      values[24] = Float8GetDatum(e->read_burst.iat.mean);
      values[25] = Float8GetDatum(smgr_stats_welford_cov(&e->read_burst.iat));
    } else {
      nulls[24] = true;
      nulls[25] = true;
    }

    if (e->write_burst.iat.count >= 2) {
      values[26] = Float8GetDatum(e->write_burst.iat.mean);
      values[27] = Float8GetDatum(smgr_stats_welford_cov(&e->write_burst.iat));
    } else {
      nulls[26] = true;
      nulls[27] = true;
    }

    values[28] = Int32GetDatum((int32)e->active_seconds);
    values[29] = TimestampTzGetDatum(e->first_access);
    values[30] = TimestampTzGetDatum(e->last_access);

    HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
  }

  SRF_RETURN_DONE(funcctx);
}
