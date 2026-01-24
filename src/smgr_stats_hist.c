#include "postgres.h"

#include <math.h>

#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/injection_point.h"

#include "smgr_stats_hist.h"

Datum smgr_stats_hist_to_array_datum(const SmgrStatsTimingHist* hist) {
  Datum elems[SMGR_STATS_HIST_BINS];
  for (int i = 0; i < SMGR_STATS_HIST_BINS; i++) {
    elems[i] = Int64GetDatum((int64)hist->bins[i]);
  }
  ArrayType* arr = construct_array_builtin(elems, SMGR_STATS_HIST_BINS, INT8OID);
  return PointerGetDatum(arr);
}

PG_FUNCTION_INFO_V1(smgr_stats_hist_percentile);

Datum smgr_stats_hist_percentile(PG_FUNCTION_ARGS) {
  ArrayType* hist_arr = PG_GETARG_ARRAYTYPE_P(0);
  float8 pct = PG_GETARG_FLOAT8(1);

  if (pct < 0.0 || pct > 1.0) {
    ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    errmsg("percentile must be between 0.0 and 1.0, got %g", pct)));
  }

  Datum* elems;
  int nelems;
  bool* elem_nulls;
  deconstruct_array(hist_arr, INT8OID, 8, true, 'd', &elems, &elem_nulls, &nelems);

  if (nelems != SMGR_STATS_HIST_BINS) {
    ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
                    errmsg("histogram array must have %d elements, got %d", SMGR_STATS_HIST_BINS, nelems)));
  }

  int64 total = 0;
  for (int i = 0; i < nelems; i++) {
    if (!elem_nulls[i]) {
      total += DatumGetInt64(elems[i]);
    }
  }

  if (total == 0) {
    PG_RETURN_NULL();
  }

  int64 target = (int64)ceil((double)total * pct);
  if (target == 0) {
    target = 1;
  }

  int64 cumulative = 0;
  for (int i = 0; i < nelems; i++) {
    if (!elem_nulls[i]) {
      cumulative += DatumGetInt64(elems[i]);
    }
    if (cumulative >= target) {
      float8 lower_bound = (i == 0) ? 0.0 : pow(2.0, i - 1);
      PG_RETURN_FLOAT8(lower_bound);
    }
  }

  pg_unreachable();
}

/*
 * Debug functions for injecting artificial I/O delays via injection points.
 * These require PostgreSQL built with --enable-injection-points.
 * When injection points are not available, these functions report an error.
 */

#ifdef USE_INJECTION_POINTS

PGDLLEXPORT void smgr_stats_delay_cb(const char* name, const void* private_data, void* arg);

void smgr_stats_delay_cb(const char* name, const void* private_data, void* arg) {
  (void)name;
  (void)arg;
  long delay_us = *(const long*)private_data;
  pg_usleep(delay_us);
}

PG_FUNCTION_INFO_V1(smgr_stats_debug_set_read_delay);

Datum smgr_stats_debug_set_read_delay(PG_FUNCTION_ARGS) {
  int64 delay_us = PG_GETARG_INT64(0);
  long delay = (long)delay_us;
  InjectionPointAttach("smgr-stats-aio-read-complete", "pg_smgrstat", "smgr_stats_delay_cb", &delay, sizeof(delay));
  InjectionPointLoad("smgr-stats-aio-read-complete");
  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(smgr_stats_debug_clear_read_delay);

Datum smgr_stats_debug_clear_read_delay(PG_FUNCTION_ARGS) {
  (void)fcinfo;
  InjectionPointDetach("smgr-stats-aio-read-complete");
  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(smgr_stats_debug_set_write_delay);

Datum smgr_stats_debug_set_write_delay(PG_FUNCTION_ARGS) {
  int64 delay_us = PG_GETARG_INT64(0);
  long delay = (long)delay_us;
  InjectionPointAttach("smgr-stats-after-writev", "pg_smgrstat", "smgr_stats_delay_cb", &delay, sizeof(delay));
  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(smgr_stats_debug_clear_write_delay);

Datum smgr_stats_debug_clear_write_delay(PG_FUNCTION_ARGS) {
  (void)fcinfo;
  InjectionPointDetach("smgr-stats-after-writev");
  PG_RETURN_VOID();
}

#else /* !USE_INJECTION_POINTS */

static pg_noreturn void debug_no_injection_points_error(void) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("pg_smgrstat debug delay functions require PostgreSQL built with --enable-injection-points")));
}

PG_FUNCTION_INFO_V1(smgr_stats_debug_set_read_delay);

Datum smgr_stats_debug_set_read_delay(PG_FUNCTION_ARGS) {
  (void)fcinfo;
  debug_no_injection_points_error();
}

PG_FUNCTION_INFO_V1(smgr_stats_debug_clear_read_delay);

Datum smgr_stats_debug_clear_read_delay(PG_FUNCTION_ARGS) {
  (void)fcinfo;
  debug_no_injection_points_error();
}

PG_FUNCTION_INFO_V1(smgr_stats_debug_set_write_delay);

Datum smgr_stats_debug_set_write_delay(PG_FUNCTION_ARGS) {
  (void)fcinfo;
  debug_no_injection_points_error();
}

PG_FUNCTION_INFO_V1(smgr_stats_debug_clear_write_delay);

Datum smgr_stats_debug_clear_write_delay(PG_FUNCTION_ARGS) {
  (void)fcinfo;
  debug_no_injection_points_error();
}

#endif /* USE_INJECTION_POINTS */
