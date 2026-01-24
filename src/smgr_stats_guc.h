#pragma once
#include "postgres.h"

extern char* smgr_stats_database;
extern int smgr_stats_collection_interval;

extern void smgr_stats_register_gucs(void);
