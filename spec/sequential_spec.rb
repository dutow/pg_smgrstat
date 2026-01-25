RSpec.describe "pg_smgrstat sequential detection" do
  include_context "pg instance"

  def seq_stats_for(table_name)
    rfn = lookup_relfilenode(conn, table_name)
    stats_conn.exec(<<~SQL)
      SELECT sequential_reads, random_reads,
             read_run_mean, read_run_cov, read_run_count
      FROM smgr_stats.current() c
      WHERE c.relnumber = #{rfn}
        AND c.forknum = 0
    SQL
  end

  context "sequential reads" do
    it "counts a full table scan as mostly sequential" do
      conn.exec("CREATE TABLE test_seq_full (id int, data text)")
      conn.exec("INSERT INTO test_seq_full SELECT g, repeat('x', 1000) FROM generate_series(1, 2000) g")
      conn.exec("CHECKPOINT")

      pg.evict_buffers(dbname: TEST_DATABASE)
      conn.exec("SELECT count(*) FROM test_seq_full")

      result = seq_stats_for("test_seq_full")
      expect(result.ntuples).to eq(1)
      seq = result[0]["sequential_reads"].to_i
      rnd = result[0]["random_reads"].to_i
      # The first read is "random" (no previous block to be contiguous with);
      # all subsequent contiguous reads are sequential.
      # Multi-block readv batches reduce the total operation count,
      # but every operation after the first should be sequential.
      expect(rnd).to eq(1)
      expect(seq).to be >= 5
    end

    it "counts non-contiguous block reads as random" do
      conn.exec("CREATE TABLE test_seq_random (id int, data text)")
      conn.exec("INSERT INTO test_seq_random SELECT g, repeat('x', 1000) FROM generate_series(1, 2000) g")
      conn.exec("CHECKPOINT")

      pg.evict_buffers(dbname: TEST_DATABASE)
      # Read individual non-contiguous blocks via TID scan
      [10, 5, 20, 2, 15].each do |blk|
        conn.exec("SELECT * FROM test_seq_random WHERE ctid >= '(#{blk},0)' AND ctid < '(#{blk + 1},0)'")
      end

      result = seq_stats_for("test_seq_random")
      expect(result.ntuples).to eq(1)
      rnd = result[0]["random_reads"].to_i
      seq = result[0]["sequential_reads"].to_i
      # All reads are to non-adjacent blocks, so all should be random
      expect(rnd).to eq(5)
      expect(seq).to eq(0)
    end

    it "detects sequential-then-random transitions" do
      conn.exec("CREATE TABLE test_seq_mixed (id int, data text)")
      conn.exec("INSERT INTO test_seq_mixed SELECT g, repeat('x', 1000) FROM generate_series(1, 2000) g")
      conn.exec("CHECKPOINT")

      pg.evict_buffers(dbname: TEST_DATABASE)
      # First: sequential scan
      conn.exec("SELECT count(*) FROM test_seq_mixed")
      # Then: random single-block reads (breaks the sequential streak)
      pg.evict_buffers(dbname: TEST_DATABASE)
      [50, 10, 30].each do |blk|
        conn.exec("SELECT * FROM test_seq_mixed WHERE ctid >= '(#{blk},0)' AND ctid < '(#{blk + 1},0)'")
      end

      result = seq_stats_for("test_seq_mixed")
      expect(result.ntuples).to eq(1)
      seq = result[0]["sequential_reads"].to_i
      rnd = result[0]["random_reads"].to_i
      # Sequential scan: 1 random (first block) + many sequential.
      # TID reads on blocks 50, 10, 30: each non-contiguous = 3 random.
      # Total random = 4.
      expect(seq).to be >= 5
      expect(rnd).to eq(4)
    end
  end

  context "run length distribution" do
    it "returns NULL mean/cov with fewer than 2 completed runs" do
      conn.exec("CREATE TABLE test_run_null (id int, data text)")
      conn.exec("INSERT INTO test_run_null SELECT g, repeat('x', 1000) FROM generate_series(1, 500) g")
      conn.exec("CHECKPOINT")

      pg.evict_buffers(dbname: TEST_DATABASE)
      conn.exec("SELECT count(*) FROM test_run_null")

      result = seq_stats_for("test_run_null")
      expect(result.ntuples).to eq(1)
      # A single sequential scan produces at most one run (flushed at exit),
      # so mean/cov should be NULL
      expect(result[0]["read_run_mean"]).to be_nil
      expect(result[0]["read_run_cov"]).to be_nil
    end

    it "records run count after multiple sequential streaks" do
      conn.exec("CREATE TABLE test_run_count (id int, data text)")
      conn.exec("INSERT INTO test_run_count SELECT g, repeat('x', 1000) FROM generate_series(1, 2000) g")
      conn.exec("CHECKPOINT")

      # Multiple sequential scans; each new scan breaks the previous streak
      4.times do
        pg.evict_buffers(dbname: TEST_DATABASE)
        conn.exec("SELECT count(*) FROM test_run_count")
      end

      result = seq_stats_for("test_run_count")
      expect(result.ntuples).to eq(1)
      run_count = result[0]["read_run_count"].to_i
      # Each scan after the first breaks the previous streak, recording it.
      # With 4 scans: runs recorded when scans 2, 3, 4 start = 3 completed runs.
      expect(run_count).to be >= 3
    end

    it "reports high mean run length for sequential scans" do
      conn.exec("CREATE TABLE test_run_long (id int, data text)")
      conn.exec("INSERT INTO test_run_long SELECT g, repeat('x', 1000) FROM generate_series(1, 2000) g")
      conn.exec("CHECKPOINT")

      # Three sequential scans; each new scan breaks the previous streak.
      # This produces at least 2 completed runs (needed for mean/cov).
      3.times do
        pg.evict_buffers(dbname: TEST_DATABASE)
        conn.exec("SELECT count(*) FROM test_run_long")
      end

      result = seq_stats_for("test_run_long")
      expect(result.ntuples).to eq(1)
      expect(result[0]["read_run_mean"]).not_to be_nil
      mean = result[0]["read_run_mean"].to_f
      # Each sequential scan reads many blocks (table is ~280+ pages);
      # the run mean should reflect those long streaks
      expect(mean).to be > 50
    end

    it "reports low mean run length for random access" do
      conn.exec("CREATE TABLE test_run_short (id int, data text)")
      conn.exec("INSERT INTO test_run_short SELECT g, repeat('x', 1000) FROM generate_series(1, 2000) g")
      conn.exec("CHECKPOINT")

      pg.evict_buffers(dbname: TEST_DATABASE)
      # Read single non-contiguous blocks; each breaks the previous "run" of length 1
      [10, 5, 20, 2, 15, 8, 25, 3].each do |blk|
        conn.exec("SELECT * FROM test_run_short WHERE ctid >= '(#{blk},0)' AND ctid < '(#{blk + 1},0)'")
      end

      result = seq_stats_for("test_run_short")
      expect(result.ntuples).to eq(1)
      expect(result[0]["read_run_count"].to_i).to be >= 2
      mean = result[0]["read_run_mean"].to_f
      # Each access reads ~1 block and breaks the previous streak,
      # so completed runs should all be ~1 block long
      expect(mean).to be <= 2.0
    end

    it "flushes in-progress runs at backend exit" do
      conn.exec("CREATE TABLE test_run_flush (id int, data text)")
      conn.exec("INSERT INTO test_run_flush SELECT g, repeat('x', 1000) FROM generate_series(1, 2000) g")
      conn.exec("CHECKPOINT")

      # Do a sequential scan in a separate connection, then close it
      flush_conn = pg.connect(dbname: TEST_DATABASE)
      pg.evict_buffers(dbname: TEST_DATABASE)
      flush_conn.exec("SELECT count(*) FROM test_run_flush")
      flush_conn.close

      # The backend exit should have flushed the in-progress run
      result = seq_stats_for("test_run_flush")
      expect(result.ntuples).to eq(1)
      run_count = result[0]["read_run_count"].to_i
      expect(run_count).to be >= 1
    end
  end
end
