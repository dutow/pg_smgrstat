#pragma once

#include "port/pg_bitutils.h"
#include "postgres.h"

#define SMGR_STATS_HIST_BINS 32

typedef struct SmgrStatsTimingHist {
  uint64 bins[SMGR_STATS_HIST_BINS]; /* 256 bytes */
  uint64 count;                      /* Total observations */
  uint64 total_us;                   /* Cumulative microseconds */
  uint64 min_us;                     /* Minimum observed (PG_UINT64_MAX sentinel when empty) */
  uint64 max_us;                     /* Maximum observed */
} SmgrStatsTimingHist;               /* 288 bytes */

/*
 * Record a timing observation into the histogram.
 *
 * Bin calculation (log2-based, O(1)):
 *   - Bin 0: exactly 0 us (cache hit / instant)
 *   - Bin i (1 <= i <= 30): covers [2^(i-1), 2^i) us
 *   - Bin 31: overflow, >= 2^30 us (approx 1073 seconds)
 *
 * pg_leftmost_one_pos64(v) returns floor(log2(v)) (position of highest
 * set bit, 0-indexed). Adding 1 maps it to the correct bin:
 *   value 1       -> bit pos 0 -> bin 1  (covers [1, 2))
 *   value 2-3     -> bit pos 1 -> bin 2  (covers [2, 4))
 *   value 4-7     -> bit pos 2 -> bin 3  (covers [4, 8))
 *   ...
 *   value 2^30+   -> clamped   -> bin 31 (overflow)
 */
static inline void smgr_stats_hist_record(SmgrStatsTimingHist* hist, uint64 value_us) {
  int bin;
  if (value_us == 0) {
    bin = 0;
  } else {
    bin = Min(pg_leftmost_one_pos64(value_us) + 1, SMGR_STATS_HIST_BINS - 1);
  }
  hist->bins[bin]++;
  hist->count++;
  hist->total_us += value_us;
  if (value_us < hist->min_us) {
    hist->min_us = value_us;
  }
  if (value_us > hist->max_us) {
    hist->max_us = value_us;
  }
}

/* Reset histogram to empty state. Used for both initialization and collection reset. */
static inline void smgr_stats_hist_reset(SmgrStatsTimingHist* hist) {
  memset(hist->bins, 0, sizeof(hist->bins));
  hist->count = 0;
  hist->total_us = 0;
  hist->min_us = PG_UINT64_MAX;
  hist->max_us = 0;
}

/* Convert a histogram to a SQL bigint[] Datum. */
extern Datum smgr_stats_hist_to_array_datum(const SmgrStatsTimingHist* hist);
