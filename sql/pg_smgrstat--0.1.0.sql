CREATE TABLE smgr_stats.history (
    bucket_id bigint NOT NULL,
    collected_at timestamptz NOT NULL DEFAULT now(),
    spcoid oid NOT NULL,
    dboid oid NOT NULL,
    relnumber oid NOT NULL,
    forknum int2 NOT NULL,
    reads int8 NOT NULL DEFAULT 0,
    read_blocks int8 NOT NULL DEFAULT 0,
    writes int8 NOT NULL DEFAULT 0,
    write_blocks int8 NOT NULL DEFAULT 0,
    read_hist bigint[],
    read_count bigint,
    read_total_us bigint,
    read_min_us bigint,
    read_max_us bigint,
    write_hist bigint[],
    write_count bigint,
    write_total_us bigint,
    write_min_us bigint,
    write_max_us bigint
);

CREATE INDEX ON smgr_stats.history USING BRIN (bucket_id);
CREATE INDEX ON smgr_stats.history USING BRIN (collected_at);

CREATE FUNCTION smgr_stats.current(
    OUT bucket_id bigint,
    OUT collected_at timestamptz,
    OUT spcoid oid,
    OUT dboid oid,
    OUT relnumber oid,
    OUT forknum int2,
    OUT reads int8,
    OUT read_blocks int8,
    OUT writes int8,
    OUT write_blocks int8,
    OUT read_hist bigint[],
    OUT read_count bigint,
    OUT read_total_us bigint,
    OUT read_min_us bigint,
    OUT read_max_us bigint,
    OUT write_hist bigint[],
    OUT write_count bigint,
    OUT write_total_us bigint,
    OUT write_min_us bigint,
    OUT write_max_us bigint
) RETURNS SETOF record
LANGUAGE c AS 'MODULE_PATHNAME', 'smgr_stats_current';

CREATE FUNCTION smgr_stats.hist_percentile(hist bigint[], pct double precision)
RETURNS double precision
LANGUAGE c IMMUTABLE STRICT
AS 'MODULE_PATHNAME', 'smgr_stats_hist_percentile';
