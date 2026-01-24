RSpec.describe "pg_smgrstat timing histograms" do
  include_context "pg instance"

  context "current() timing columns" do
    it "returns read timing columns as NULL for a write-only entry" do
      conn.exec("CREATE TABLE test_write_only (id int, data text)")
      conn.exec("INSERT INTO test_write_only SELECT g, repeat('x', 100) FROM generate_series(1, 1000) g")
      conn.exec("CHECKPOINT")

      result = conn.exec(<<~SQL)
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

      result = conn.exec(<<~SQL)
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
      pg.evict_buffers
      conn.exec("SELECT count(*) FROM test_write_timing")

      result = conn.exec(<<~SQL)
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
      result = conn.exec(<<~SQL)
        SELECT array_length(write_hist, 1) AS len
        FROM smgr_stats.current()
        WHERE write_count IS NOT NULL
        LIMIT 1
      SQL
      expect(result[0]["len"].to_i).to eq(32)
    end

    it "histogram bin sum equals count" do
      result = conn.exec(<<~SQL)
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
      result = conn.exec("SELECT smgr_stats.hist_percentile(ARRAY[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]::bigint[], 0.5)")
      expect(result[0]["hist_percentile"]).to be_nil
    end

    it "returns 0 for p50 when all values are in bin 0" do
      hist = "ARRAY[100,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]::bigint[]"
      result = conn.exec("SELECT smgr_stats.hist_percentile(#{hist}, 0.5)")
      expect(result[0]["hist_percentile"].to_f).to eq(0.0)
    end

    it "returns correct bin lower bound for concentrated histogram" do
      # All 100 observations in bin 5 (covers [16, 32) us)
      hist = "ARRAY[0,0,0,0,0,100,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]::bigint[]"
      result = conn.exec("SELECT smgr_stats.hist_percentile(#{hist}, 0.5)")
      expect(result[0]["hist_percentile"].to_f).to eq(16.0) # 2^(5-1) = 16
    end

    it "correctly walks cumulative bins for split histogram" do
      # 50 in bin 3 (covers [4,8)), 50 in bin 7 (covers [64,128))
      hist = "ARRAY[0,0,0,50,0,0,0,50,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]::bigint[]"
      # P25 should be in bin 3 -> 4.0
      r25 = conn.exec("SELECT smgr_stats.hist_percentile(#{hist}, 0.25)")
      expect(r25[0]["hist_percentile"].to_f).to eq(4.0)
      # P75 should be in bin 7 -> 64.0
      r75 = conn.exec("SELECT smgr_stats.hist_percentile(#{hist}, 0.75)")
      expect(r75[0]["hist_percentile"].to_f).to eq(64.0)
    end

    it "rejects percentile out of range" do
      hist = "ARRAY[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]::bigint[]"
      expect { conn.exec("SELECT smgr_stats.hist_percentile(#{hist}, 1.5)") }.to raise_error(PG::NumericValueOutOfRange)
      expect { conn.exec("SELECT smgr_stats.hist_percentile(#{hist}, -0.1)") }.to raise_error(PG::NumericValueOutOfRange)
    end
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
      pg.evict_buffers
      conn.exec("SELECT count(*) FROM test_timing_history")

      sleep 3  # wait for collection

      result = conn.exec(<<~SQL)
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
