#include "postgres.h"

#include "access/htup_details.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/timestamp.h"

#include "smgr_stats_store.h"

#define CURRENT_NUM_COLUMNS 46

typedef struct SmgrStatsCurrentCtx {
  SmgrStatsEntry* entries;
  int64 bucket_id;
  TimestampTz collected_at;
} SmgrStatsCurrentCtx;

static inline void welford_to_datum(const SmgrStatsWelford* w, Datum* values, bool* nulls, int idx) {
  if (w->count >= 2) {
    values[idx] = Float8GetDatum(w->mean);
    values[idx + 1] = Float8GetDatum(smgr_stats_welford_cov(w));
  } else {
    nulls[idx] = true;
    nulls[idx + 1] = true;
  }
}

static inline void set_oid_or_null(Oid oid, Datum* values, bool* nulls, int idx) {
  if (OidIsValid(oid)) {
    values[idx] = ObjectIdGetDatum(oid);
  } else {
    nulls[idx] = true;
  }
}

static inline void set_name_or_null(const NameData* name, Datum* values, bool* nulls, int idx) {
  if (name->data[0] != '\0') {
    values[idx] = NameGetDatum(name);
  } else {
    nulls[idx] = true;
  }
}

static inline void set_char_or_null(char c, Datum* values, bool* nulls, int idx) {
  if (c != '\0') {
    values[idx] = CharGetDatum(c);
  } else {
    nulls[idx] = true;
  }
}

static inline void timing_to_datum(const SmgrStatsTimingHist* h, Datum* values, bool* nulls, int idx) {
  if (h->count > 0) {
    values[idx] = smgr_stats_hist_to_array_datum(h);
    values[idx + 1] = Int64GetDatum((int64)h->count);
    values[idx + 2] = Int64GetDatum((int64)h->total_us);
    values[idx + 3] = Int64GetDatum((int64)h->min_us);
    values[idx + 4] = Int64GetDatum((int64)h->max_us);
  } else {
    nulls[idx] = true;
    nulls[idx + 1] = true;
    nulls[idx + 2] = true;
    nulls[idx + 3] = true;
    nulls[idx + 4] = true;
  }
}

static void resolve_snapshot_metadata(SmgrStatsEntry* entries, int count) {
  for (int i = 0; i < count; i++) {
    SmgrStatsEntry* e = &entries[i];
    const bool needs_resolve = !e->meta.metadata_valid;
    const bool can_resolve = (e->key.locator.dbOid == MyDatabaseId) || (e->key.locator.dbOid == 0);
    if (needs_resolve && can_resolve) {
      smgr_stats_resolve_metadata(e, &e->key);
    }
  }
}

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
    resolve_snapshot_metadata(ctx->entries, count);

    funcctx->user_fctx = ctx;
    funcctx->max_calls = count;

    TupleDesc tupdesc = CreateTemplateTupleDesc(CURRENT_NUM_COLUMNS);
    TupleDescInitEntry(tupdesc, 1, "bucket_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 2, "collected_at", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, 3, "spcoid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, 4, "dboid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, 5, "relnumber", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, 6, "forknum", INT2OID, -1, 0);
    /* Metadata columns */
    TupleDescInitEntry(tupdesc, 7, "reloid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, 8, "main_reloid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, 9, "relname", NAMEOID, -1, 0);
    TupleDescInitEntry(tupdesc, 10, "nspname", NAMEOID, -1, 0);
    TupleDescInitEntry(tupdesc, 11, "relkind", CHAROID, -1, 0);
    /* Stats columns */
    TupleDescInitEntry(tupdesc, 12, "reads", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 13, "read_blocks", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 14, "writes", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 15, "write_blocks", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 16, "extends", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 17, "extend_blocks", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 18, "truncates", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 19, "fsyncs", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 20, "read_hist", INT8ARRAYOID, -1, 0);
    TupleDescInitEntry(tupdesc, 21, "read_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 22, "read_total_us", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 23, "read_min_us", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 24, "read_max_us", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 25, "write_hist", INT8ARRAYOID, -1, 0);
    TupleDescInitEntry(tupdesc, 26, "write_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 27, "write_total_us", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 28, "write_min_us", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 29, "write_max_us", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 30, "read_iat_mean_us", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 31, "read_iat_cov", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 32, "write_iat_mean_us", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 33, "write_iat_cov", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 34, "sequential_reads", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 35, "random_reads", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 36, "sequential_writes", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 37, "random_writes", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 38, "read_run_mean", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 39, "read_run_cov", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 40, "read_run_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 41, "write_run_mean", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 42, "write_run_cov", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 43, "write_run_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 44, "active_seconds", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, 45, "first_access", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, 46, "last_access", TIMESTAMPTZOID, -1, 0);
    funcctx->tuple_desc = BlessTupleDesc(tupdesc);

    MemoryContextSwitchTo(oldctx);
  }

  funcctx = SRF_PERCALL_SETUP();
  ctx = funcctx->user_fctx;

  if (funcctx->call_cntr < funcctx->max_calls) {
    SmgrStatsEntry* e = &ctx->entries[funcctx->call_cntr];
    Datum values[CURRENT_NUM_COLUMNS];
    bool nulls[CURRENT_NUM_COLUMNS] = {false};

    values[0] = Int64GetDatum(ctx->bucket_id);
    values[1] = TimestampTzGetDatum(ctx->collected_at);
    values[2] = ObjectIdGetDatum(e->key.locator.spcOid);
    values[3] = ObjectIdGetDatum(e->key.locator.dbOid);
    values[4] = ObjectIdGetDatum(e->key.locator.relNumber);
    values[5] = Int16GetDatum((int16)e->key.forknum);

    /* Metadata columns */
    set_oid_or_null(e->meta.reloid, values, nulls, 6);
    set_oid_or_null(e->meta.main_reloid, values, nulls, 7);
    set_name_or_null(&e->meta.relname, values, nulls, 8);
    set_name_or_null(&e->meta.nspname, values, nulls, 9);
    set_char_or_null(e->meta.relkind, values, nulls, 10);

    /* Stats columns */
    values[11] = UInt64GetDatum(e->reads);
    values[12] = UInt64GetDatum(e->read_blocks);
    values[13] = UInt64GetDatum(e->writes);
    values[14] = UInt64GetDatum(e->write_blocks);
    values[15] = UInt64GetDatum(e->extends);
    values[16] = UInt64GetDatum(e->extend_blocks);
    values[17] = UInt64GetDatum(e->truncates);
    values[18] = UInt64GetDatum(e->fsyncs);

    timing_to_datum(&e->read_timing, values, nulls, 19);
    timing_to_datum(&e->write_timing, values, nulls, 24);

    welford_to_datum(&e->read_burst.iat, values, nulls, 29);
    welford_to_datum(&e->write_burst.iat, values, nulls, 31);

    values[33] = UInt64GetDatum(e->sequential_reads);
    values[34] = UInt64GetDatum(e->random_reads);
    values[35] = UInt64GetDatum(e->sequential_writes);
    values[36] = UInt64GetDatum(e->random_writes);

    welford_to_datum(&e->read_runs, values, nulls, 37);
    values[39] = Int64GetDatum((int64)e->read_runs.count);
    welford_to_datum(&e->write_runs, values, nulls, 40);
    values[42] = Int64GetDatum((int64)e->write_runs.count);

    values[43] = Int32GetDatum((int32)e->active_seconds);
    values[44] = TimestampTzGetDatum(e->first_access);
    values[45] = TimestampTzGetDatum(e->last_access);

    HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
  }

  SRF_RETURN_DONE(funcctx);
}
