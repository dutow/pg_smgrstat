RSpec.describe "pg_smgrstat temporary table tracking" do
  include_context "pg instance"

  describe "track_temp_tables GUC" do
    it "has default value of 'aggregate'" do
      result = conn.exec("SHOW smgr_stats.track_temp_tables")
      expect(result[0]["smgr_stats.track_temp_tables"]).to eq("aggregate")
    end

    it "accepts valid values" do
      conn.exec("SET smgr_stats.track_temp_tables = 'off'")
      expect(conn.exec("SHOW smgr_stats.track_temp_tables")[0]["smgr_stats.track_temp_tables"]).to eq("off")

      conn.exec("SET smgr_stats.track_temp_tables = 'individual'")
      expect(conn.exec("SHOW smgr_stats.track_temp_tables")[0]["smgr_stats.track_temp_tables"]).to eq("individual")

      conn.exec("SET smgr_stats.track_temp_tables = 'aggregate'")
      expect(conn.exec("SHOW smgr_stats.track_temp_tables")[0]["smgr_stats.track_temp_tables"]).to eq("aggregate")
    end

    it "rejects invalid values" do
      expect {
        conn.exec("SET smgr_stats.track_temp_tables = 'invalid'")
      }.to raise_error(PG::InvalidParameterValue)
    end
  end

  describe "mode: off" do
    before(:each) do
      conn.exec("SET smgr_stats.track_temp_tables = 'off'")
    end

    it "does not track temp table I/O" do
      conn.exec("CREATE TEMP TABLE temp_off_test (id int, data text)")
      conn.exec("INSERT INTO temp_off_test SELECT g, repeat('x', 1000) FROM generate_series(1, 1000) g")
      conn.exec("SELECT smgr_stats._debug_flush_local_buffers()")

      # Get the relfilenode of the temp table
      relfilenode = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'temp_off_test'")[0]["relfilenode"].to_i

      # Check that no entry exists for this relfilenode
      result = conn.exec("SELECT count(*) AS n FROM smgr_stats.current() WHERE relnumber = #{relfilenode}")
      expect(result[0]["n"].to_i).to eq(0)

      # Also check that no aggregate entry exists
      result = conn.exec("SELECT count(*) AS n FROM smgr_stats.current() WHERE spcoid = 0 AND relnumber = 0")
      expect(result[0]["n"].to_i).to eq(0)
    end
  end

  describe "mode: individual" do
    before(:each) do
      conn.exec("SET smgr_stats.track_temp_tables = 'individual'")
    end

    it "tracks each temp table separately" do
      conn.exec("CREATE TEMP TABLE temp_ind_1 (id int, data text)")
      conn.exec("CREATE TEMP TABLE temp_ind_2 (id int, data text)")
      conn.exec("INSERT INTO temp_ind_1 SELECT g, repeat('x', 1000) FROM generate_series(1, 500) g")
      conn.exec("INSERT INTO temp_ind_2 SELECT g, repeat('x', 1000) FROM generate_series(1, 500) g")
      conn.exec("SELECT smgr_stats._debug_flush_local_buffers()")

      relfilenode1 = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'temp_ind_1'")[0]["relfilenode"].to_i
      relfilenode2 = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'temp_ind_2'")[0]["relfilenode"].to_i

      # Both should have separate entries with writes
      result1 = conn.exec("SELECT writes, write_blocks FROM smgr_stats.current() WHERE relnumber = #{relfilenode1} AND forknum = 0")
      result2 = conn.exec("SELECT writes, write_blocks FROM smgr_stats.current() WHERE relnumber = #{relfilenode2} AND forknum = 0")

      expect(result1.ntuples).to eq(1)
      expect(result2.ntuples).to eq(1)
      expect(result1[0]["write_blocks"].to_i).to be > 0
      expect(result2[0]["write_blocks"].to_i).to be > 0
    end

    it "resolves temp table metadata correctly" do
      conn.exec("CREATE TEMP TABLE temp_ind_meta (id int, data text)")
      conn.exec("INSERT INTO temp_ind_meta SELECT g, repeat('x', 1000) FROM generate_series(1, 500) g")
      conn.exec("SELECT smgr_stats._debug_flush_local_buffers()")

      relfilenode = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'temp_ind_meta'")[0]["relfilenode"].to_i

      result = conn.exec("SELECT relname, nspname, relkind, writes, write_blocks FROM smgr_stats.current() WHERE relnumber = #{relfilenode} AND forknum = 0")
      expect(result.ntuples).to eq(1)
      expect(result[0]["write_blocks"].to_i).to be > 0
      expect(result[0]["relname"]).to eq("temp_ind_meta")
      expect(result[0]["nspname"]).to match(/^pg_temp/) # pg_temp_N
      expect(result[0]["relkind"]).to eq("r") # regular table
    end
  end

  describe "mode: aggregate (default)" do
    before(:each) do
      conn.exec("SET smgr_stats.track_temp_tables = 'aggregate'")
    end

    it "combines all temp tables into one entry" do
      conn.exec("CREATE TEMP TABLE temp_agg_1 (id int, data text)")
      conn.exec("CREATE TEMP TABLE temp_agg_2 (id int, data text)")
      conn.exec("INSERT INTO temp_agg_1 SELECT g, repeat('x', 1000) FROM generate_series(1, 500) g")
      conn.exec("INSERT INTO temp_agg_2 SELECT g, repeat('x', 1000) FROM generate_series(1, 500) g")
      conn.exec("SELECT smgr_stats._debug_flush_local_buffers()")

      relfilenode1 = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'temp_agg_1'")[0]["relfilenode"].to_i
      relfilenode2 = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'temp_agg_2'")[0]["relfilenode"].to_i

      # Individual relfilenodes should NOT have entries
      result1 = conn.exec("SELECT count(*) AS n FROM smgr_stats.current() WHERE relnumber = #{relfilenode1}")
      result2 = conn.exec("SELECT count(*) AS n FROM smgr_stats.current() WHERE relnumber = #{relfilenode2}")
      expect(result1[0]["n"].to_i).to eq(0)
      expect(result2[0]["n"].to_i).to eq(0)

      # Aggregate entry should exist with synthetic key (spcoid=0, relnumber=0)
      result = conn.exec("SELECT writes, write_blocks FROM smgr_stats.current() WHERE spcoid = 0 AND relnumber = 0")
      expect(result.ntuples).to eq(1)
      expect(result[0]["write_blocks"].to_i).to be > 0
    end

    it "shows special metadata for aggregate entry" do
      conn.exec("CREATE TEMP TABLE temp_agg_meta (id int, data text)")
      conn.exec("INSERT INTO temp_agg_meta SELECT g, repeat('x', 1000) FROM generate_series(1, 100) g")
      conn.exec("SELECT smgr_stats._debug_flush_local_buffers()")

      result = conn.exec("SELECT relname, nspname, relkind FROM smgr_stats.current() WHERE spcoid = 0 AND relnumber = 0")
      expect(result.ntuples).to eq(1)
      expect(result[0]["relname"]).to eq("<temporary tables>")
      expect(result[0]["nspname"]).to eq("pg_temp")
      expect(result[0]["relkind"]).to eq("T")
    end

    it "accumulates stats across multiple temp tables" do
      conn.exec("CREATE TEMP TABLE temp_agg_accum1 (id int, data text)")
      conn.exec("INSERT INTO temp_agg_accum1 SELECT g, repeat('x', 1000) FROM generate_series(1, 250) g")
      conn.exec("SELECT smgr_stats._debug_flush_local_buffers()")

      first_writes = conn.exec("SELECT write_blocks FROM smgr_stats.current() WHERE spcoid = 0 AND relnumber = 0")[0]["write_blocks"].to_i

      conn.exec("CREATE TEMP TABLE temp_agg_accum2 (id int, data text)")
      conn.exec("INSERT INTO temp_agg_accum2 SELECT g, repeat('x', 1000) FROM generate_series(1, 250) g")
      conn.exec("SELECT smgr_stats._debug_flush_local_buffers()")

      second_writes = conn.exec("SELECT write_blocks FROM smgr_stats.current() WHERE spcoid = 0 AND relnumber = 0")[0]["write_blocks"].to_i

      expect(second_writes).to be > first_writes
    end
  end

  describe "write timing tracking" do
    before(:each) do
      conn.exec("SET smgr_stats.track_temp_tables = 'aggregate'")
    end

    it "tracks write timing for temp table I/O" do
      conn.exec("CREATE TEMP TABLE temp_timing (id int, data text)")
      conn.exec("INSERT INTO temp_timing SELECT g, repeat('x', 1000) FROM generate_series(1, 500) g")
      conn.exec("SELECT smgr_stats._debug_flush_local_buffers()")

      result = conn.exec(<<~SQL)
        SELECT active_seconds, writes, write_blocks, write_count
        FROM smgr_stats.current()
        WHERE spcoid = 0 AND relnumber = 0
      SQL
      expect(result.ntuples).to eq(1)
      expect(result[0]["write_blocks"].to_i).to be > 0
      expect(result[0]["write_count"].to_i).to be > 0
    end
  end

  describe "mode switching mid-session" do
    it "affects only subsequent I/O" do
      # Start in aggregate mode
      conn.exec("SET smgr_stats.track_temp_tables = 'aggregate'")
      conn.exec("CREATE TEMP TABLE temp_switch1 (id int, data text)")
      conn.exec("INSERT INTO temp_switch1 SELECT g, repeat('x', 1000) FROM generate_series(1, 100) g")
      conn.exec("SELECT smgr_stats._debug_flush_local_buffers()")

      # Should have aggregate entry
      agg_result = conn.exec("SELECT count(*) AS n FROM smgr_stats.current() WHERE spcoid = 0 AND relnumber = 0")
      expect(agg_result[0]["n"].to_i).to eq(1)

      # Switch to individual mode
      conn.exec("SET smgr_stats.track_temp_tables = 'individual'")
      conn.exec("CREATE TEMP TABLE temp_switch2 (id int, data text)")
      conn.exec("INSERT INTO temp_switch2 SELECT g, repeat('x', 1000) FROM generate_series(1, 100) g")
      conn.exec("SELECT smgr_stats._debug_flush_local_buffers()")

      relfilenode2 = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'temp_switch2'")[0]["relfilenode"].to_i

      # New table should have individual entry (main fork only)
      ind_result = conn.exec("SELECT count(*) AS n FROM smgr_stats.current() WHERE relnumber = #{relfilenode2} AND forknum = 0")
      expect(ind_result[0]["n"].to_i).to eq(1)

      # Switch to off mode
      conn.exec("SET smgr_stats.track_temp_tables = 'off'")
      conn.exec("CREATE TEMP TABLE temp_switch3 (id int, data text)")
      conn.exec("INSERT INTO temp_switch3 SELECT g, repeat('x', 1000) FROM generate_series(1, 100) g")
      conn.exec("SELECT smgr_stats._debug_flush_local_buffers()")

      relfilenode3 = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'temp_switch3'")[0]["relfilenode"].to_i

      # New table should have no entry
      off_result = conn.exec("SELECT count(*) AS n FROM smgr_stats.current() WHERE relnumber = #{relfilenode3}")
      expect(off_result[0]["n"].to_i).to eq(0)
    end
  end
end

RSpec.describe "pg_smgrstat temp table history collection",
               extra_config: {"smgr_stats.collection_interval" => "2"} do
  include_context "pg instance"

  it "collects only aggregate entry in history, not individual temp tables" do
    conn.exec("SET smgr_stats.track_temp_tables = 'aggregate'")
    conn.exec("CREATE TEMP TABLE temp_hist1 (id int, data text)")
    conn.exec("CREATE TEMP TABLE temp_hist2 (id int, data text)")
    conn.exec("INSERT INTO temp_hist1 SELECT g, repeat('x', 1000) FROM generate_series(1, 500) g")
    conn.exec("INSERT INTO temp_hist2 SELECT g, repeat('x', 1000) FROM generate_series(1, 500) g")
    conn.exec("SELECT smgr_stats._debug_flush_local_buffers()")

    relfilenode1 = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'temp_hist1'")[0]["relfilenode"].to_i
    relfilenode2 = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'temp_hist2'")[0]["relfilenode"].to_i

    sleep 3

    # Aggregate entry should exist with writes
    agg_result = conn.exec(<<~SQL)
      SELECT count(*) AS n
      FROM smgr_stats.history
      WHERE spcoid = 0 AND relnumber = 0 AND write_blocks > 0
    SQL
    expect(agg_result[0]["n"].to_i).to be >= 1

    # Individual temp table entries should NOT exist
    ind_result1 = conn.exec("SELECT count(*) AS n FROM smgr_stats.history WHERE relnumber = #{relfilenode1}")
    ind_result2 = conn.exec("SELECT count(*) AS n FROM smgr_stats.history WHERE relnumber = #{relfilenode2}")
    expect(ind_result1[0]["n"].to_i).to eq(0)
    expect(ind_result2[0]["n"].to_i).to eq(0)
  end
end
