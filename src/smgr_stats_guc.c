#include "postgres.h"

#include "utils/guc.h"

#include "smgr_stats_guc.h"

/* GUC variables */
char* smgr_stats_database = "postgres";
int smgr_stats_collection_interval = 60;
int smgr_stats_track_temp_tables = SMGR_STATS_TEMP_AGGREGATE;

static const struct config_enum_entry track_temp_tables_options[] = {{"off", SMGR_STATS_TEMP_OFF, false},
                                                                     {"individual", SMGR_STATS_TEMP_INDIVIDUAL, false},
                                                                     {"aggregate", SMGR_STATS_TEMP_AGGREGATE, false},
                                                                     {NULL, 0, false}};

void smgr_stats_register_gucs(void) {
  DefineCustomStringVariable("smgr_stats.database", "Database where the history table is stored.", NULL,
                             &smgr_stats_database, "postgres", PGC_POSTMASTER, 0, NULL, NULL, NULL);

  DefineCustomIntVariable("smgr_stats.collection_interval", "Seconds between stats collections.", NULL,
                          &smgr_stats_collection_interval, 60, 1, 3600, PGC_SIGHUP, 0, NULL, NULL, NULL);

  DefineCustomEnumVariable("smgr_stats.track_temp_tables",
                           "How to track temporary table I/O (off, individual, aggregate).", NULL,
                           &smgr_stats_track_temp_tables, SMGR_STATS_TEMP_AGGREGATE, track_temp_tables_options,
                           PGC_SUSET, 0, NULL, NULL, NULL);
}
