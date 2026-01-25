#include "postgres.h"

#include "fmgr.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "utils/injection_point.h"

PG_MODULE_MAGIC;

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
  InjectionPointAttach("smgr-stats-aio-read-complete", "pg_smgrstat_debug", "smgr_stats_delay_cb", &delay,
                       sizeof(delay));
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
  InjectionPointAttach("smgr-stats-after-writev", "pg_smgrstat_debug", "smgr_stats_delay_cb", &delay, sizeof(delay));
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
                  errmsg("pg_smgrstat_debug delay functions require PostgreSQL built with --enable-injection-points")));
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

/*
 * smgr_stats_debug_flush_local_buffers - flush all dirty local buffers
 *
 * This function iterates through all local buffers (used by temporary tables)
 * and flushes any dirty buffers to disk. This is useful for testing since
 * local buffers are normally only written when evicted or at backend exit.
 *
 * Returns the number of buffers flushed.
 */
PG_FUNCTION_INFO_V1(smgr_stats_debug_flush_local_buffers);

Datum smgr_stats_debug_flush_local_buffers(PG_FUNCTION_ARGS) {
  (void)fcinfo; /* unused */
  int flushed = 0;

  for (int i = 0; i < NLocBuffer; i++) {
    BufferDesc* buf_hdr = GetLocalBufferDescriptor(i);
    uint64 buf_state = pg_atomic_read_u64(&buf_hdr->state);

    /* Only flush valid, dirty, unpinned buffers */
    if ((buf_state & BM_TAG_VALID) && (buf_state & BM_DIRTY) && BUF_STATE_GET_REFCOUNT(buf_state) == 0) {
      PinLocalBuffer(buf_hdr, false);
      FlushLocalBuffer(buf_hdr, NULL);
      Buffer buf = BufferDescriptorGetBuffer(buf_hdr);
      UnpinLocalBuffer(buf);

      flushed++;
    }
  }

  PG_RETURN_INT32(flushed);
}
