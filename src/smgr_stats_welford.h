#pragma once

#include "postgres.h"

#include <math.h>

/*
 * Generic Welford online statistics: tracks count, mean, and M2 (sum of
 * squared deviations from the current mean). From these we can derive
 * variance, standard deviation, and coefficient of variation (CoV).
 */
typedef struct SmgrStatsWelford {
  uint64 count;
  double mean;
  double m2; /* Sum of (x_i - mean)^2; variance = m2 / (count - 1) */
} SmgrStatsWelford;

static inline void smgr_stats_welford_record(SmgrStatsWelford* w, double value) {
  w->count++;
  double delta = value - w->mean;
  w->mean += delta / (double)w->count;
  double delta2 = value - w->mean;
  w->m2 += delta * delta2;
}

static inline void smgr_stats_welford_reset(SmgrStatsWelford* w) {
  w->count = 0;
  w->mean = 0.0;
  w->m2 = 0.0;
}

static inline double smgr_stats_welford_variance(const SmgrStatsWelford* w) {
  if (w->count < 2) {
    return 0.0;
  }
  return w->m2 / (double)(w->count - 1);
}

/* Coefficient of variation: stddev / mean. >1 means high variability. */
static inline double smgr_stats_welford_cov(const SmgrStatsWelford* w) {
  if (w->count < 2 || w->mean == 0.0) {
    return 0.0;
  }
  double variance = w->m2 / (double)(w->count - 1);
  return sqrt(variance) / fabs(w->mean);
}
