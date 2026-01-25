#include "postgres.h"

#include <math.h>

#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "utils/array.h"

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
