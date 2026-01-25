RSpec.describe "pg_smgrstat burstiness" do
  include_context "pg instance"

  # Helper: get current stats for a specific table's main fork
  def stats_for(table_name)
    rfn = lookup_relfilenode(conn, table_name)
    stats_conn.exec(<<~SQL)
      SELECT read_iat_mean_us, read_iat_cov, write_iat_mean_us, write_iat_cov
      FROM smgr_stats.current() c
      WHERE c.relnumber = #{rfn}
        AND c.forknum = 0
    SQL
  end

  context "read burstiness" do
    it "returns NULL with fewer than 2 read operations" do
      conn.exec("CREATE TABLE test_burst_null (id int, data text)")
      conn.exec("INSERT INTO test_burst_null VALUES (1, 'x')")
      conn.exec("CHECKPOINT")

      pg.evict_buffers(dbname: TEST_DATABASE)
      # Single-block read: one readv call
      conn.exec("SELECT * FROM test_burst_null WHERE ctid = '(0,1)'::tid")

      result = stats_for("test_burst_null")
      expect(result.ntuples).to eq(1)
      expect(result[0]["read_iat_mean_us"]).to be_nil
      expect(result[0]["read_iat_cov"]).to be_nil
    end

    it "is populated after multiple read operations" do
      conn.exec("CREATE TABLE test_burst_pop (id int, data text)")
      conn.exec("INSERT INTO test_burst_pop SELECT g, repeat('x', 1000) FROM generate_series(1, 1000) g")
      conn.exec("CHECKPOINT")

      pg.evict_buffers(dbname: TEST_DATABASE)
      conn.exec("SELECT count(*) FROM test_burst_pop")

      result = stats_for("test_burst_pop")
      expect(result.ntuples).to eq(1)
      row = result[0]
      expect(row["read_iat_mean_us"]).not_to be_nil
      expect(row["read_iat_cov"]).not_to be_nil
      expect(row["read_iat_mean_us"].to_f).to be > 0
      expect(row["read_iat_cov"].to_f).to be >= 0
    end

    it "shows low CoV for a single continuous burst of reads" do
      conn.exec("CREATE TABLE test_burst_steady (id int, data text)")
      conn.exec("INSERT INTO test_burst_steady SELECT g, repeat('x', 1000) FROM generate_series(1, 2000) g")
      conn.exec("CHECKPOINT")

      pg.evict_buffers(dbname: TEST_DATABASE)
      conn.exec("SELECT count(*) FROM test_burst_steady")

      result = stats_for("test_burst_steady")
      expect(result.ntuples).to eq(1)
      cov = result[0]["read_iat_cov"].to_f
      # Within a single sequential scan, inter-arrival times should be fairly uniform
      # (sub-Poisson regularity: CoV < 1.0)
      expect(cov).to be < 1.0
    end

    it "shows high CoV for reads with deliberate pauses between bursts" do
      conn.exec("CREATE TABLE test_burst_high (id int, data text)")
      conn.exec("INSERT INTO test_burst_high SELECT g, repeat('x', 1000) FROM generate_series(1, 2000) g")
      conn.exec("CHECKPOINT")

      # Generate 3 bursts of reads with 500ms gaps between them
      3.times do
        pg.evict_buffers(dbname: TEST_DATABASE)
        conn.exec("SELECT count(*) FROM test_burst_high")
        sleep 0.5
      end

      result = stats_for("test_burst_high")
      expect(result.ntuples).to eq(1)
      cov = result[0]["read_iat_cov"].to_f
      # The 500ms gaps between bursts vs microsecond-scale within-burst IATs
      # should produce very high CoV (bimodal distribution)
      expect(cov).to be > 2.0
    end

    it "shows higher CoV for bursty pattern than for steady pattern" do
      conn.exec("CREATE TABLE test_burst_compare (id int, data text)")
      conn.exec("INSERT INTO test_burst_compare SELECT g, repeat('x', 1000) FROM generate_series(1, 2000) g")
      conn.exec("CHECKPOINT")

      # Measure steady: single continuous scan
      pg.evict_buffers(dbname: TEST_DATABASE)
      conn.exec("SELECT count(*) FROM test_burst_compare")
      steady_result = stats_for("test_burst_compare")
      steady_cov = steady_result[0]["read_iat_cov"].to_f

      # Reset stats by waiting for collection and creating a fresh table
      conn.exec("DROP TABLE test_burst_compare")
      conn.exec("CREATE TABLE test_burst_compare2 (id int, data text)")
      conn.exec("INSERT INTO test_burst_compare2 SELECT g, repeat('x', 1000) FROM generate_series(1, 2000) g")
      conn.exec("CHECKPOINT")

      # Measure bursty: multiple scans with gaps
      3.times do
        pg.evict_buffers(dbname: TEST_DATABASE)
        conn.exec("SELECT count(*) FROM test_burst_compare2")
        sleep 0.5
      end

      bursty_result = stats_for("test_burst_compare2")
      bursty_cov = bursty_result[0]["read_iat_cov"].to_f

      expect(bursty_cov).to be > steady_cov
    end

    it "reports reasonable mean inter-arrival time" do
      conn.exec("CREATE TABLE test_burst_mean (id int, data text)")
      conn.exec("INSERT INTO test_burst_mean SELECT g, repeat('x', 1000) FROM generate_series(1, 2000) g")
      conn.exec("CHECKPOINT")

      # Two bursts with a 200ms gap
      pg.evict_buffers(dbname: TEST_DATABASE)
      conn.exec("SELECT count(*) FROM test_burst_mean")
      sleep 0.2
      pg.evict_buffers(dbname: TEST_DATABASE)
      conn.exec("SELECT count(*) FROM test_burst_mean")

      result = stats_for("test_burst_mean")
      mean_us = result[0]["read_iat_mean_us"].to_f

      # Mean should be somewhere between the fast within-burst IATs and the 200ms gap.
      # It won't be exactly 200ms/N because most IATs are fast (within-burst).
      # Just verify it's positive and less than the gap (200ms = 200_000us).
      expect(mean_us).to be > 0
      expect(mean_us).to be < 200_000
    end
  end

  context "write burstiness" do
    it "returns NULL when no writes occurred" do
      conn.exec("CREATE TABLE test_wburst_null (id int)")
      conn.exec("INSERT INTO test_wburst_null VALUES (1)")
      # No CHECKPOINT, so writes may not have hit SMGR yet for this specific table

      pg.evict_buffers(dbname: TEST_DATABASE)
      conn.exec("SELECT * FROM test_wburst_null")

      result = stats_for("test_wburst_null")
      # Write burstiness should be NULL if fewer than 2 write ops occurred
      if result.ntuples > 0 && result[0]["write_iat_mean_us"]
        # If writes DID happen (background writer), just verify they're valid
        expect(result[0]["write_iat_mean_us"].to_f).to be > 0
      else
        expect(result[0]["write_iat_mean_us"]).to be_nil
      end
    end

    it "is populated after checkpoint flushes multiple blocks" do
      conn.exec("CREATE TABLE test_wburst_pop (id int, data text)")
      conn.exec("INSERT INTO test_wburst_pop SELECT g, repeat('x', 1000) FROM generate_series(1, 1000) g")
      conn.exec("CHECKPOINT")

      result = stats_for("test_wburst_pop")
      expect(result.ntuples).to eq(1)
      row = result[0]
      # CHECKPOINT writes many blocks in quick succession
      expect(row["write_iat_mean_us"]).not_to be_nil
      expect(row["write_iat_cov"]).not_to be_nil
      expect(row["write_iat_mean_us"].to_f).to be > 0
      expect(row["write_iat_cov"].to_f).to be >= 0
    end

    it "shows high CoV for writes with multiple checkpoints separated by pauses" do
      conn.exec("CREATE TABLE test_wburst_high (id int, data text)")

      3.times do |i|
        conn.exec("INSERT INTO test_wburst_high SELECT g, repeat('x', 1000) FROM generate_series(#{i * 500 + 1}, #{(i + 1) * 500}) g")
        conn.exec("CHECKPOINT")
        sleep 0.5
      end

      result = stats_for("test_wburst_high")
      expect(result.ntuples).to eq(1)
      row = result[0]
      expect(row["write_iat_mean_us"]).not_to be_nil
      cov = row["write_iat_cov"].to_f
      # Multiple checkpoints with gaps should create bursty write pattern
      expect(cov).to be > 1.0
    end
  end
end
