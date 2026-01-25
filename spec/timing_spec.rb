RSpec.describe "pg_smgrstat timing histograms" do
  include_context "pg instance"

  context "current() timing columns" do
    it "returns read timing columns as NULL for a write-only entry" do
      conn.exec("CREATE TABLE test_write_only (id int, data text)")
      conn.exec("INSERT INTO test_write_only SELECT g, repeat('x', 100) FROM generate_series(1, 1000) g")
      conn.exec("CHECKPOINT")

      result = stats_conn.exec(<<~SQL)
        SELECT read_hist, read_count, read_total_us, read_min_us, read_max_us
        FROM smgr_stats.current()
        WHERE writes > 0 AND reads = 0
        LIMIT 1
      SQL
      expect(result.ntuples).to be >= 1
      row = result[0]
      expect(row["read_hist"]).to be_nil
      expect(row["read_count"]).to be_nil
      expect(row["read_total_us"]).to be_nil
      expect(row["read_min_us"]).to be_nil
      expect(row["read_max_us"]).to be_nil
    end

    it "reports write timing histogram after writes" do
      conn.exec("CREATE TABLE test_write_timing (id int, data text)")
      conn.exec("INSERT INTO test_write_timing SELECT g, repeat('x', 100) FROM generate_series(1, 1000) g")
      conn.exec("CHECKPOINT")

      result = stats_conn.exec(<<~SQL)
        SELECT write_hist, write_count, write_total_us, write_min_us, write_max_us
        FROM smgr_stats.current()
        WHERE write_count IS NOT NULL AND write_count > 0
        LIMIT 1
      SQL
      expect(result.ntuples).to be >= 1
      row = result[0]
      expect(row["write_count"].to_i).to be > 0
      expect(row["write_total_us"].to_i).to be > 0
      expect(row["write_min_us"].to_i).to be <= row["write_max_us"].to_i

      hist = row["write_hist"]
      expect(hist).not_to be_nil
      bins = hist.gsub(/[{}]/, '').split(',').map(&:to_i)
      expect(bins.length).to eq(32)
      expect(bins.sum).to eq(row["write_count"].to_i)
      expect(bins.any? { |b| b > 0 }).to be true
    end

    it "reports read timing histogram after reads" do
      pg.evict_buffers(dbname: TEST_DATABASE)
      conn.exec("SELECT count(*) FROM test_write_timing")

      result = stats_conn.exec(<<~SQL)
        SELECT read_hist, read_count, read_total_us, read_min_us, read_max_us
        FROM smgr_stats.current()
        WHERE read_count IS NOT NULL AND read_count > 0
        LIMIT 1
      SQL
      expect(result.ntuples).to be >= 1
      row = result[0]
      expect(row["read_count"].to_i).to be > 0
      expect(row["read_total_us"].to_i).to be > 0
      expect(row["read_min_us"].to_i).to be <= row["read_max_us"].to_i

      hist = row["read_hist"]
      expect(hist).not_to be_nil
      bins = hist.gsub(/[{}]/, '').split(',').map(&:to_i)
      expect(bins.length).to eq(32)
      expect(bins.sum).to eq(row["read_count"].to_i)
      expect(bins.any? { |b| b > 0 }).to be true
    end

    it "histogram array has 32 elements" do
      result = stats_conn.exec(<<~SQL)
        SELECT array_length(write_hist, 1) AS len
        FROM smgr_stats.current()
        WHERE write_count IS NOT NULL
        LIMIT 1
      SQL
      expect(result[0]["len"].to_i).to eq(32)
    end

    it "histogram bin sum equals count" do
      result = stats_conn.exec(<<~SQL)
        SELECT write_count,
               (SELECT sum(v) FROM unnest(write_hist) AS v) AS bin_sum
        FROM smgr_stats.current()
        WHERE write_count IS NOT NULL AND write_count > 0
        LIMIT 1
      SQL
      expect(result[0]["bin_sum"].to_i).to eq(result[0]["write_count"].to_i)
    end
  end

  context "hist_percentile function" do
    it "returns NULL for empty histogram" do
      result = stats_conn.exec("SELECT smgr_stats.hist_percentile(ARRAY[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]::bigint[], 0.5)")
      expect(result[0]["hist_percentile"]).to be_nil
    end

    it "returns 0 for p50 when all values are in bin 0" do
      hist = "ARRAY[100,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]::bigint[]"
      result = stats_conn.exec("SELECT smgr_stats.hist_percentile(#{hist}, 0.5)")
      expect(result[0]["hist_percentile"].to_f).to eq(0.0)
    end

    it "returns correct bin lower bound for concentrated histogram" do
      # All 100 observations in bin 5 (covers [16, 32) us)
      hist = "ARRAY[0,0,0,0,0,100,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]::bigint[]"
      result = stats_conn.exec("SELECT smgr_stats.hist_percentile(#{hist}, 0.5)")
      expect(result[0]["hist_percentile"].to_f).to eq(16.0) # 2^(5-1) = 16
    end

    it "correctly walks cumulative bins for split histogram" do
      # 50 in bin 3 (covers [4,8)), 50 in bin 7 (covers [64,128))
      hist = "ARRAY[0,0,0,50,0,0,0,50,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]::bigint[]"
      # P25 should be in bin 3 -> 4.0
      r25 = stats_conn.exec("SELECT smgr_stats.hist_percentile(#{hist}, 0.25)")
      expect(r25[0]["hist_percentile"].to_f).to eq(4.0)
      # P75 should be in bin 7 -> 64.0
      r75 = stats_conn.exec("SELECT smgr_stats.hist_percentile(#{hist}, 0.75)")
      expect(r75[0]["hist_percentile"].to_f).to eq(64.0)
    end

    it "rejects percentile out of range" do
      hist = "ARRAY[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]::bigint[]"
      expect { stats_conn.exec("SELECT smgr_stats.hist_percentile(#{hist}, 1.5)") }.to raise_error(PG::NumericValueOutOfRange)
      expect { stats_conn.exec("SELECT smgr_stats.hist_percentile(#{hist}, -0.1)") }.to raise_error(PG::NumericValueOutOfRange)
    end
  end

end

RSpec.describe "pg_smgrstat write bucket precision" do
  include_context "pg instance"

  # Delay of 10ms (10000 us) lands in bin 14: [2^13, 2^14) = [8192, 16384) us
  # pg_leftmost_one_pos64(10000) = 13, so bin = 13 + 1 = 14
  WRITE_DELAY_US = 10_000
  WRITE_EXPECTED_BIN = 14  # 0-indexed

  before(:all) do
    @pg.connect(dbname: TEST_DATABASE) { |c| c.exec("CREATE EXTENSION IF NOT EXISTS pg_smgrstat_debug") }
  end

  after do
    conn.exec("SELECT smgr_stats_debug.clear_write_delay()")
  end

  it "places writes into the expected histogram bin" do
    # Capture baseline before setting delay (pre-existing writes from setup)
    baseline_bins = Array.new(32, 0)
    baseline = stats_conn.exec(<<~SQL)
      SELECT write_hist FROM smgr_stats.current()
      WHERE write_count IS NOT NULL AND write_count > 0
    SQL
    baseline.each do |row|
      bins = row["write_hist"].gsub(/[{}]/, '').split(',').map(&:to_i)
      bins.each_with_index { |v, i| baseline_bins[i] += v }
    end

    conn.exec("SELECT smgr_stats_debug.set_write_delay(#{WRITE_DELAY_US})")

    conn.exec("CREATE TABLE test_write_bucket (id int, data text)")
    conn.exec("INSERT INTO test_write_bucket SELECT g, repeat('x', 200) FROM generate_series(1, 50) g")
    conn.exec("CHECKPOINT")

    result = stats_conn.exec(<<~SQL)
      SELECT write_hist, write_count
      FROM smgr_stats.current()
      WHERE write_count IS NOT NULL AND write_count > 0
    SQL
    expect(result.ntuples).to be >= 1

    # Aggregate all write histograms and subtract baseline
    total_bins = Array.new(32, 0)
    result.each do |row|
      bins = row["write_hist"].gsub(/[{}]/, '').split(',').map(&:to_i)
      bins.each_with_index { |v, i| total_bins[i] += v }
    end
    delta_bins = total_bins.zip(baseline_bins).map { |a, b| [a - b, 0].max }

    expect(delta_bins[WRITE_EXPECTED_BIN]).to be > 0,
      "Expected bin #{WRITE_EXPECTED_BIN} to have new entries, got delta: #{delta_bins}"

    # All delayed writes should land in the expected bin
    delta_count = delta_bins.sum
    expect(delta_bins[WRITE_EXPECTED_BIN].to_f / delta_count).to be >= 0.9,
      "Expected bin #{WRITE_EXPECTED_BIN} to contain >= 90% of new writes, " \
      "got #{delta_bins[WRITE_EXPECTED_BIN]}/#{delta_count}. Delta: #{delta_bins}"
  end

  it "min_us and max_us reflect the injected delay" do
    conn.exec("SELECT smgr_stats_debug.set_write_delay(#{WRITE_DELAY_US})")

    conn.exec("CREATE TABLE test_write_minmax (id int)")
    conn.exec("INSERT INTO test_write_minmax SELECT g FROM generate_series(1, 10) g")
    conn.exec("CHECKPOINT")

    result = stats_conn.exec(<<~SQL)
      SELECT write_min_us, write_max_us
      FROM smgr_stats.current()
      WHERE write_count IS NOT NULL AND write_count > 0
      LIMIT 1
    SQL
    expect(result.ntuples).to eq(1)
    min_us = result[0]["write_min_us"].to_i
    max_us = result[0]["write_max_us"].to_i
    # With 10ms delay, min should be at least close to the delay
    expect(min_us).to be >= WRITE_DELAY_US * 0.8
    expect(max_us).to be >= min_us
  end

  it "hist_percentile returns expected bin lower bound for delayed writes" do
    conn.exec("SELECT smgr_stats_debug.set_write_delay(#{WRITE_DELAY_US})")

    conn.exec("CREATE TABLE test_pct_bucket (id int, data text)")
    conn.exec("INSERT INTO test_pct_bucket SELECT g, repeat('x', 200) FROM generate_series(1, 50) g")
    conn.exec("CHECKPOINT")

    result = stats_conn.exec(<<~SQL)
      SELECT smgr_stats.hist_percentile(write_hist, 0.5) AS p50
      FROM smgr_stats.current()
      WHERE write_count IS NOT NULL AND write_count > 0
      LIMIT 1
    SQL
    expect(result.ntuples).to eq(1)
    p50 = result[0]["p50"].to_f
    # Bin 14 lower bound is 2^(14-1) = 8192
    expect(p50).to eq(8192.0)
  end
end

RSpec.describe "pg_smgrstat read bucket precision" do
  include_context "pg instance"

  # Delay of 10ms (10000 us) lands in bin 14: [2^13, 2^14) = [8192, 16384) us
  READ_DELAY_US = 10_000
  READ_EXPECTED_BIN = 14  # 0-indexed

  before(:all) do
    @pg.connect(dbname: TEST_DATABASE) do |c|
      c.exec("CREATE EXTENSION IF NOT EXISTS pg_smgrstat_debug")
      c.exec("CREATE EXTENSION IF NOT EXISTS pg_buffercache")
      c.exec("CREATE TABLE test_read_bucket (id int, data text)")
      c.exec("INSERT INTO test_read_bucket SELECT g, repeat('x', 200) FROM generate_series(1, 50) g")
      c.exec("CHECKPOINT")
    end
  end

  after do
    conn.exec("SELECT smgr_stats_debug.clear_read_delay()")
  end

  it "places reads into the expected histogram bin" do
    # Capture baseline before setting delay (pre-existing reads from setup)
    baseline_bins = Array.new(32, 0)
    baseline = stats_conn.exec(<<~SQL)
      SELECT read_hist FROM smgr_stats.current()
      WHERE read_count IS NOT NULL AND read_count > 0
    SQL
    baseline.each do |row|
      bins = row["read_hist"].gsub(/[{}]/, '').split(',').map(&:to_i)
      bins.each_with_index { |v, i| baseline_bins[i] += v }
    end

    conn.exec("SELECT smgr_stats_debug.set_read_delay(#{READ_DELAY_US})")
    conn.exec("SELECT pg_buffercache_evict_all()")
    conn.exec("SELECT count(*) FROM test_read_bucket")

    result = stats_conn.exec(<<~SQL)
      SELECT read_hist, read_count
      FROM smgr_stats.current()
      WHERE read_count IS NOT NULL AND read_count > 0
    SQL
    expect(result.ntuples).to be >= 1

    # Aggregate all read histograms and subtract baseline
    total_bins = Array.new(32, 0)
    result.each do |row|
      bins = row["read_hist"].gsub(/[{}]/, '').split(',').map(&:to_i)
      bins.each_with_index { |v, i| total_bins[i] += v }
    end
    delta_bins = total_bins.zip(baseline_bins).map { |a, b| [a - b, 0].max }

    expect(delta_bins[READ_EXPECTED_BIN]).to be > 0,
      "Expected bin #{READ_EXPECTED_BIN} to have new entries, got delta: #{delta_bins}"

    # All delayed reads should land in the expected bin
    delta_count = delta_bins.sum
    expect(delta_bins[READ_EXPECTED_BIN].to_f / delta_count).to be >= 0.9,
      "Expected bin #{READ_EXPECTED_BIN} to contain >= 90% of new reads, " \
      "got #{delta_bins[READ_EXPECTED_BIN]}/#{delta_count}. Delta: #{delta_bins}"
  end

  it "min_us and max_us reflect the injected delay" do
    # Get table identifiers before setting delay
    relfilenode = lookup_relfilenode(conn, "test_read_bucket")
    db_oid = test_db_oid(conn)

    conn.exec("SELECT smgr_stats_debug.set_read_delay(#{READ_DELAY_US})")
    conn.exec("SELECT pg_buffercache_evict_all()")
    conn.exec("SELECT count(*) FROM test_read_bucket")
    # Clear injection point BEFORE opening stats_conn to avoid crash when new
    # connection authenticates (reads pg_authid) while injection point is active
    # but not loaded in that backend
    conn.exec("SELECT smgr_stats_debug.clear_read_delay()")

    result = stats_conn.exec(<<~SQL)
      SELECT read_min_us, read_max_us
      FROM smgr_stats.current()
      WHERE relnumber = #{relfilenode} AND dboid = #{db_oid} AND forknum = 0
    SQL
    expect(result.ntuples).to eq(1)
    min_us = result[0]["read_min_us"].to_i
    max_us = result[0]["read_max_us"].to_i
    expect(min_us).to be >= READ_DELAY_US * 0.8
    expect(max_us).to be >= min_us
  end
end

RSpec.describe "pg_smgrstat timing background worker",
               extra_config: {"smgr_stats.collection_interval" => "2"} do
  include_context "pg instance"

  context "histogram export" do
    it "exports timing histograms to history table" do
      conn.exec("CREATE TABLE test_timing_history (id int, data text)")
      conn.exec("INSERT INTO test_timing_history SELECT g, repeat('x', 100) FROM generate_series(1, 1000) g")
      conn.exec("CHECKPOINT")
      pg.evict_buffers(dbname: TEST_DATABASE)
      conn.exec("SELECT count(*) FROM test_timing_history")

      sleep 3  # wait for collection

      result = stats_conn.exec(<<~SQL)
        SELECT read_hist, read_count, write_hist, write_count
        FROM smgr_stats.history
        WHERE (read_count IS NOT NULL AND read_count > 0)
           OR (write_count IS NOT NULL AND write_count > 0)
        LIMIT 1
      SQL
      expect(result.ntuples).to be >= 1
    end
  end
end
