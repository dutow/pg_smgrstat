CREATE FUNCTION smgr_stats_debug.set_read_delay(delay_us bigint)
RETURNS void LANGUAGE c VOLATILE
AS 'MODULE_PATHNAME', 'smgr_stats_debug_set_read_delay';

CREATE FUNCTION smgr_stats_debug.clear_read_delay()
RETURNS void LANGUAGE c VOLATILE
AS 'MODULE_PATHNAME', 'smgr_stats_debug_clear_read_delay';

CREATE FUNCTION smgr_stats_debug.set_write_delay(delay_us bigint)
RETURNS void LANGUAGE c VOLATILE
AS 'MODULE_PATHNAME', 'smgr_stats_debug_set_write_delay';

CREATE FUNCTION smgr_stats_debug.clear_write_delay()
RETURNS void LANGUAGE c VOLATILE
AS 'MODULE_PATHNAME', 'smgr_stats_debug_clear_write_delay';

CREATE FUNCTION smgr_stats_debug.flush_local_buffers()
RETURNS integer LANGUAGE c VOLATILE
AS 'MODULE_PATHNAME', 'smgr_stats_debug_flush_local_buffers';
