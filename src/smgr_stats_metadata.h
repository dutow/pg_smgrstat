#pragma once

#include "postgres.h"

#include "smgr_stats_store.h"

/*
 * Backend-local pending metadata tracking.
 *
 * When entries are created in the dshash during I/O operations, we cannot
 * resolve metadata immediately (syscache access conflicts with AIO and buffer
 * locks). Instead, we add the key to a backend-local pending list and resolve
 * metadata later in safe contexts:
 *   - ExecutorEnd_hook (after DML queries complete)
 *   - ProcessUtility_hook (after DDL/utility statements complete)
 *   - before_shmem_exit callback (last chance before backend exits)
 *
 * This approach ensures each backend resolves metadata for entries it creates
 * (which are always in its connected database), providing cross-database
 * metadata resolution since each backend operates on its own database.
 */

/* Add a key to the backend-local pending metadata list. Called from SMGR hooks
 * when a new entry is created. The key is copied to TopMemoryContext. */
extern void smgr_stats_add_pending_metadata(const SmgrStatsKey* key);

/* Resolve all pending metadata entries for this backend's database.
 * Safe to call from hooks after operations complete. Uses the
 * release-lookup-reacquire pattern to avoid holding dshash locks
 * during syscache access. */
extern void smgr_stats_resolve_pending_metadata(void);

/* Register ExecutorEnd, ProcessUtility, and before_shmem_exit hooks.
 * Called from _PG_init(). */
extern void smgr_stats_register_metadata_hooks(void);
