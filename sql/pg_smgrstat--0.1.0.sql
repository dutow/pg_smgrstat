CREATE TABLE smgr_stats.history (
    bucket_id bigint NOT NULL,
    collected_at timestamptz NOT NULL DEFAULT now(),
    spcoid oid NOT NULL,
    dboid oid NOT NULL,
    relnumber oid NOT NULL,
    forknum int2 NOT NULL,
    reloid oid,              -- Stable relation OID from pg_class
    main_reloid oid,         -- For TOAST/indexes: the parent table's OID
    relname name,            -- Table/index name
    nspname name,            -- Schema name
    relkind "char",          -- Relation kind ('r'=table, 'i'=index, 't'=toast, etc.)
    reads int8 NOT NULL DEFAULT 0,
    read_blocks int8 NOT NULL DEFAULT 0,
    writes int8 NOT NULL DEFAULT 0,
    write_blocks int8 NOT NULL DEFAULT 0,
    extends int8 NOT NULL DEFAULT 0,
    extend_blocks int8 NOT NULL DEFAULT 0,
    truncates int8 NOT NULL DEFAULT 0,
    fsyncs int8 NOT NULL DEFAULT 0,
    read_hist bigint[],
    read_count bigint,
    read_total_us bigint,
    read_min_us bigint,
    read_max_us bigint,
    write_hist bigint[],
    write_count bigint,
    write_total_us bigint,
    write_min_us bigint,
    write_max_us bigint,
    read_iat_mean_us double precision,
    read_iat_cov double precision,
    write_iat_mean_us double precision,
    write_iat_cov double precision,
    sequential_reads int8 NOT NULL DEFAULT 0,
    random_reads int8 NOT NULL DEFAULT 0,
    sequential_writes int8 NOT NULL DEFAULT 0,
    random_writes int8 NOT NULL DEFAULT 0,
    read_run_mean double precision,
    read_run_cov double precision,
    read_run_count bigint NOT NULL DEFAULT 0,
    write_run_mean double precision,
    write_run_cov double precision,
    write_run_count bigint NOT NULL DEFAULT 0,
    active_seconds integer NOT NULL DEFAULT 0,
    first_access timestamptz,
    last_access timestamptz
);

CREATE INDEX ON smgr_stats.history USING BRIN (bucket_id);
CREATE INDEX ON smgr_stats.history USING BRIN (collected_at);
CREATE INDEX ON smgr_stats.history (reloid) WHERE reloid IS NOT NULL;
CREATE INDEX ON smgr_stats.history (dboid, nspname, relname) WHERE relname IS NOT NULL;

SELECT pg_catalog.pg_extension_config_dump('smgr_stats.history', '');

-- Track relfilenode associations for VACUUM FULL / CLUSTER / REINDEX / etc.
CREATE TABLE smgr_stats.relfile_history (
    created_at timestamptz NOT NULL DEFAULT now(),
    spcoid oid NOT NULL,
    dboid oid NOT NULL,
    old_relnumber oid NOT NULL,
    new_relnumber oid NOT NULL,
    forknum int2 NOT NULL,
    is_redo bool NOT NULL DEFAULT false,
    reloid oid,
    relname name,
    nspname name
);

CREATE INDEX ON smgr_stats.relfile_history (old_relnumber);
CREATE INDEX ON smgr_stats.relfile_history (new_relnumber);
CREATE INDEX ON smgr_stats.relfile_history (dboid, reloid) WHERE reloid IS NOT NULL;
CREATE INDEX ON smgr_stats.relfile_history USING BRIN (created_at);

SELECT pg_catalog.pg_extension_config_dump('smgr_stats.relfile_history', '');

CREATE FUNCTION smgr_stats.current(
    OUT bucket_id bigint,
    OUT collected_at timestamptz,
    OUT spcoid oid,
    OUT dboid oid,
    OUT relnumber oid,
    OUT forknum int2,
    OUT reloid oid,
    OUT main_reloid oid,
    OUT relname name,
    OUT nspname name,
    OUT relkind "char",
    OUT reads int8,
    OUT read_blocks int8,
    OUT writes int8,
    OUT write_blocks int8,
    OUT extends int8,
    OUT extend_blocks int8,
    OUT truncates int8,
    OUT fsyncs int8,
    OUT read_hist bigint[],
    OUT read_count bigint,
    OUT read_total_us bigint,
    OUT read_min_us bigint,
    OUT read_max_us bigint,
    OUT write_hist bigint[],
    OUT write_count bigint,
    OUT write_total_us bigint,
    OUT write_min_us bigint,
    OUT write_max_us bigint,
    OUT read_iat_mean_us double precision,
    OUT read_iat_cov double precision,
    OUT write_iat_mean_us double precision,
    OUT write_iat_cov double precision,
    OUT sequential_reads int8,
    OUT random_reads int8,
    OUT sequential_writes int8,
    OUT random_writes int8,
    OUT read_run_mean double precision,
    OUT read_run_cov double precision,
    OUT read_run_count bigint,
    OUT write_run_mean double precision,
    OUT write_run_cov double precision,
    OUT write_run_count bigint,
    OUT active_seconds integer,
    OUT first_access timestamptz,
    OUT last_access timestamptz
) RETURNS SETOF record
LANGUAGE c AS 'MODULE_PATHNAME', 'smgr_stats_current';

CREATE FUNCTION smgr_stats.hist_percentile(hist bigint[], pct double precision)
RETURNS double precision
LANGUAGE c IMMUTABLE STRICT
AS 'MODULE_PATHNAME', 'smgr_stats_hist_percentile';

-- Convenience view: history with human-readable names
CREATE VIEW smgr_stats.history_v AS
SELECT
    h.bucket_id,
    h.collected_at,
    h.spcoid,
    h.dboid,
    h.relnumber,
    h.forknum,
    CASE h.forknum
        WHEN 0 THEN 'main'
        WHEN 1 THEN 'fsm'
        WHEN 2 THEN 'vm'
        WHEN 3 THEN 'init'
        ELSE 'unknown'
    END AS fork_name,
    h.reloid,
    h.main_reloid,
    h.relname,
    h.nspname,
    h.relkind,
    CASE h.relkind
        WHEN 'r' THEN 'table'
        WHEN 'i' THEN 'index'
        WHEN 't' THEN 'toast'
        WHEN 'S' THEN 'sequence'
        WHEN 'm' THEN 'matview'
        WHEN 'p' THEN 'partitioned table'
        WHEN 'I' THEN 'partitioned index'
        ELSE 'other'
    END AS relkind_name,
    h.reads,
    h.read_blocks,
    h.writes,
    h.write_blocks,
    h.extends,
    h.extend_blocks,
    h.truncates,
    h.fsyncs,
    h.read_count,
    h.read_total_us,
    h.read_min_us,
    h.read_max_us,
    CASE WHEN h.read_count > 0 THEN h.read_total_us::double precision / h.read_count ELSE NULL END AS read_avg_us,
    h.write_count,
    h.write_total_us,
    h.write_min_us,
    h.write_max_us,
    CASE WHEN h.write_count > 0 THEN h.write_total_us::double precision / h.write_count ELSE NULL END AS write_avg_us,
    h.read_iat_mean_us,
    h.read_iat_cov,
    h.write_iat_mean_us,
    h.write_iat_cov,
    h.sequential_reads,
    h.random_reads,
    h.sequential_writes,
    h.random_writes,
    h.read_run_mean,
    h.read_run_cov,
    h.read_run_count,
    h.write_run_mean,
    h.write_run_cov,
    h.write_run_count,
    h.active_seconds,
    h.first_access,
    h.last_access
FROM smgr_stats.history h;

-- Get all history for a table, following relfilenode lineage through rewrites
CREATE FUNCTION smgr_stats.get_table_history(rel regclass)
RETURNS SETOF smgr_stats.history
LANGUAGE sql STABLE
AS $$
    WITH RECURSIVE lineage AS (
        -- Start with current relfilenode
        SELECT relfilenode AS relnumber FROM pg_class WHERE oid = rel
        UNION ALL
        -- Follow the chain backwards through rewrites
        SELECT rh.old_relnumber
        FROM smgr_stats.relfile_history rh
        JOIN lineage l ON rh.new_relnumber = l.relnumber
    )
    SELECT h.*
    FROM smgr_stats.history h
    JOIN lineage l ON h.relnumber = l.relnumber
    ORDER BY h.collected_at;
$$;

-- Debug functions for injecting artificial I/O delays.
-- Require PostgreSQL built with --enable-injection-points; otherwise they
-- return an error when called.
CREATE FUNCTION smgr_stats._debug_set_read_delay(delay_us bigint)
RETURNS void LANGUAGE c VOLATILE
AS 'MODULE_PATHNAME', 'smgr_stats_debug_set_read_delay';

CREATE FUNCTION smgr_stats._debug_clear_read_delay()
RETURNS void LANGUAGE c VOLATILE
AS 'MODULE_PATHNAME', 'smgr_stats_debug_clear_read_delay';

CREATE FUNCTION smgr_stats._debug_set_write_delay(delay_us bigint)
RETURNS void LANGUAGE c VOLATILE
AS 'MODULE_PATHNAME', 'smgr_stats_debug_set_write_delay';

CREATE FUNCTION smgr_stats._debug_clear_write_delay()
RETURNS void LANGUAGE c VOLATILE
AS 'MODULE_PATHNAME', 'smgr_stats_debug_clear_write_delay';
