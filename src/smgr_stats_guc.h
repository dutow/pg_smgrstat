#pragma once
#include "postgres.h"

typedef enum SmgrStatsTempTracking {
  SMGR_STATS_TEMP_OFF = 0,
  SMGR_STATS_TEMP_INDIVIDUAL = 1,
  SMGR_STATS_TEMP_AGGREGATE = 2
} SmgrStatsTempTracking;

extern char* smgr_stats_database;
extern int smgr_stats_collection_interval;
extern int smgr_stats_track_temp_tables;
extern int smgr_stats_retention_hours;

extern void smgr_stats_register_gucs(void);
