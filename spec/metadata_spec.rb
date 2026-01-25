RSpec.describe "pg_smgrstat metadata" do
  include_context "pg instance"

  context "current() function metadata columns" do
    it "reports metadata columns are present" do
      # Generate some I/O to ensure we have entries
      conn.exec("CREATE TABLE test_columns_check (id int)")
      conn.exec("INSERT INTO test_columns_check VALUES (1)")
      conn.exec("CHECKPOINT")

      # Simply query the function and check that columns are present
      result = conn.exec(<<~SQL)
        SELECT reloid, main_reloid, relname, nspname, relkind
        FROM smgr_stats.current()
        LIMIT 1
      SQL

      expect(result.fields).to include("reloid")
      expect(result.fields).to include("main_reloid")
      expect(result.fields).to include("relname")
      expect(result.fields).to include("nspname")
      expect(result.fields).to include("relkind")
    end

    it "populates metadata for regular tables" do
      conn.exec("CREATE TABLE test_meta_table (id int, data text)")
      conn.exec("INSERT INTO test_meta_table SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")

      relfilenode = conn.exec(
        "SELECT relfilenode FROM pg_class WHERE relname = 'test_meta_table'"
      )[0]["relfilenode"]

      result = conn.exec(<<~SQL)
        SELECT reloid, relname, nspname, relkind, relnumber
        FROM smgr_stats.current()
        WHERE relnumber = #{relfilenode} AND forknum = 0
      SQL

      expect(result.ntuples).to eq(1), "Expected to find entry for relnumber=#{relfilenode}"

      row = result[0]
      expect(row["reloid"]).not_to be_nil, "reloid should be populated"
      expect(row["relname"]).to eq("test_meta_table")
      expect(row["nspname"]).to eq("public")
      expect(row["relkind"]).to eq("r")
    end

    it "populates metadata for indexes with main_reloid pointing to indexed table" do
      conn.exec("CREATE TABLE test_meta_indexed (id int PRIMARY KEY, data text)")
      conn.exec("INSERT INTO test_meta_indexed SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
      conn.exec("CHECKPOINT")

      table_oid = conn.exec("SELECT oid FROM pg_class WHERE relname = 'test_meta_indexed'")[0]["oid"]
      # Find the index entry (primary key)
      result = conn.exec(<<~SQL)
        SELECT reloid, main_reloid, relname, nspname, relkind
        FROM smgr_stats.current()
        WHERE relkind = 'i' AND main_reloid = #{table_oid}
      SQL

      expect(result.ntuples).to be >= 1
      row = result[0]
      expect(row["reloid"]).not_to be_nil
      expect(row["main_reloid"]).to eq(table_oid)
      expect(row["relkind"]).to eq("i")
      expect(row["nspname"]).to eq("public")
    end

    it "resolves metadata for all entries including system catalogs" do
      # Generate some I/O to ensure we have entries
      conn.exec("CREATE TABLE test_full_coverage (id int, data text)")
      conn.exec("INSERT INTO test_full_coverage SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")

      # Query metadata coverage
      result = conn.exec(<<~SQL)
        SELECT
          COUNT(*) AS total,
          COUNT(*) FILTER (WHERE reloid IS NOT NULL) AS with_metadata,
          COUNT(*) FILTER (WHERE reloid IS NULL) AS without_metadata
        FROM smgr_stats.current()
        WHERE forknum = 0
      SQL

      row = result[0]
      total = row["total"].to_i
      with_metadata = row["with_metadata"].to_i
      without_metadata = row["without_metadata"].to_i

      # All entries should have metadata resolved
      expect(total).to be > 0, "Expected some entries to exist"
      expect(with_metadata).to eq(total), "Expected all #{total} entries to have metadata, but #{without_metadata} are missing"
    end
  end

  context "metadata for TOAST tables" do
    it "populates metadata for TOAST tables with main_reloid pointing to parent" do
      conn.exec("CREATE TABLE test_meta_toast (id int, large_data text)")

      parent_info = conn.exec(<<~SQL)
        SELECT c.oid AS parent_oid, c.reltoastrelid AS toast_oid
        FROM pg_class c
        WHERE c.relname = 'test_meta_toast'
      SQL
      parent_oid = parent_info[0]["parent_oid"]
      toast_oid = parent_info[0]["toast_oid"]

      expect(toast_oid.to_i).to be > 0, "Table should have a TOAST table"

      toast_relfilenode = conn.exec(
        "SELECT relfilenode FROM pg_class WHERE oid = #{toast_oid}"
      )[0]["relfilenode"]

      # Insert random data that won't compress well (TOAST compression would shrink repeat('x', 10000) too much)
      # md5() returns 32 hex chars, we need ~3KB uncompressible data per row to exceed inline threshold (~2KB)
      conn.exec(<<~SQL)
        INSERT INTO test_meta_toast
        SELECT g, string_agg(md5(g::text || i::text), '')
        FROM generate_series(1, 10) g,
             generate_series(1, 100) i
        GROUP BY g
      SQL

      # Force a checkpoint to flush any pending TOAST writes
      conn.exec("CHECKPOINT")

      # Evict buffers and force TOAST reads by accessing the large data
      pg.evict_buffers
      conn.exec("SELECT length(large_data) FROM test_meta_toast")

      result = conn.exec(<<~SQL)
        SELECT reloid, main_reloid, relname, nspname, relkind, reads, writes, extends, extend_blocks
        FROM smgr_stats.current()
        WHERE relnumber = #{toast_relfilenode} AND forknum = 0
      SQL

      expect(result.ntuples).to eq(1), "Expected to find TOAST table entry for relnumber=#{toast_relfilenode}"

      row = result[0]
      expect(row["relkind"]).to eq("t"), "Expected relkind to be 't' for TOAST table"
      expect(row["main_reloid"]).to eq(parent_oid), "TOAST main_reloid should point to parent table"
      expect(row["relname"]).to start_with("pg_toast_")
      expect(row["nspname"]).to eq("pg_toast")

      total_io = row["reads"].to_i + row["writes"].to_i + row["extends"].to_i
      expect(total_io).to be > 0, "Expected some I/O on TOAST table"
    end
  end
end

RSpec.describe "pg_smgrstat metadata in history",
               extra_config: {"smgr_stats.collection_interval" => "2"} do
  include_context "pg instance"

  it "preserves metadata columns in history after collection" do
    conn.exec("CREATE TABLE test_hist_meta (id int, data text)")
    conn.exec("INSERT INTO test_hist_meta SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
    conn.exec("CHECKPOINT")

    relfilenode = conn.exec(
      "SELECT relfilenode FROM pg_class WHERE relname = 'test_hist_meta'"
    )[0]["relfilenode"]

    sleep 3 # Wait for collection

    result = conn.exec(<<~SQL)
      SELECT reloid, relname, nspname, relkind
      FROM smgr_stats.history
      WHERE relnumber = #{relfilenode} AND forknum = 0
      ORDER BY collected_at DESC
      LIMIT 1
    SQL

    expect(result.ntuples).to eq(1)
    # Metadata in history matches what was captured
    row = result[0]
    if row["reloid"]
      expect(row["relname"]).to eq("test_hist_meta")
      expect(row["nspname"]).to eq("public")
      expect(row["relkind"]).to eq("r")
    end
  end

  it "history_v view provides human-readable names" do
    conn.exec("CREATE TABLE test_view_table (id int, data text)")
    conn.exec("INSERT INTO test_view_table SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
    conn.exec("CHECKPOINT")

    sleep 3

    result = conn.exec(<<~SQL)
      SELECT fork_name, relkind_name
      FROM smgr_stats.history_v
      WHERE relname = 'test_view_table'
      LIMIT 1
    SQL

    if result.ntuples > 0
      expect(result[0]["fork_name"]).to eq("main")
      expect(result[0]["relkind_name"]).to eq("table")
    end
  end
end

RSpec.describe "pg_smgrstat relfile history",
               extra_config: {"smgr_stats.collection_interval" => "2"} do
  include_context "pg instance"

  it "creates relfile_history table" do
    result = conn.exec(<<~SQL)
      SELECT 1 FROM information_schema.tables
      WHERE table_schema = 'smgr_stats' AND table_name = 'relfile_history'
    SQL
    expect(result.ntuples).to eq(1)
  end

  it "records relfilenode changes after TRUNCATE" do
    conn.exec("CREATE TABLE test_rewrite (id int, data text)")
    conn.exec("INSERT INTO test_rewrite SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
    conn.exec("CHECKPOINT")

    # Get current relfilenode
    old_relfilenode = conn.exec(
      "SELECT relfilenode FROM pg_class WHERE relname = 'test_rewrite'"
    )[0]["relfilenode"]

    # TRUNCATE creates a new relfilenode via RelationSetNewRelfilenumber
    conn.exec("TRUNCATE test_rewrite")

    # Get new relfilenode
    new_relfilenode = conn.exec(
      "SELECT relfilenode FROM pg_class WHERE relname = 'test_rewrite'"
    )[0]["relfilenode"]

    # Verify the relfilenode actually changed
    expect(new_relfilenode).not_to eq(old_relfilenode), "TRUNCATE should change relfilenode"

    # Wait for worker to drain the queue
    sleep 3

    # Check relfile_history for the association
    result = conn.exec(<<~SQL)
      SELECT old_relnumber, new_relnumber, forknum
      FROM smgr_stats.relfile_history
      WHERE old_relnumber = #{old_relfilenode}
        AND new_relnumber = #{new_relfilenode}
    SQL

    if result.ntuples == 0
      # Debug: show what's in relfile_history
      all_history = conn.exec("SELECT * FROM smgr_stats.relfile_history LIMIT 10")
      puts "\nDEBUG: No matching entry. relfile_history contents:"
      all_history.each { |r| puts "  #{r.inspect}" }
    end

    expect(result.ntuples).to be >= 1

    # Verify the recorded association is correct
    row = result[0]
    expect(row["old_relnumber"]).to eq(old_relfilenode)
    expect(row["new_relnumber"]).to eq(new_relfilenode)
  end

  it "captures relfilenode changes from VACUUM FULL" do
    conn.exec("CREATE TABLE test_vacuum_full (id int, data text)")
    conn.exec("INSERT INTO test_vacuum_full SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
    conn.exec("CHECKPOINT")

    old_relfilenode = conn.exec(
      "SELECT relfilenode FROM pg_class WHERE relname = 'test_vacuum_full'"
    )[0]["relfilenode"]

    conn.exec("VACUUM FULL test_vacuum_full")

    new_relfilenode = conn.exec(
      "SELECT relfilenode FROM pg_class WHERE relname = 'test_vacuum_full'"
    )[0]["relfilenode"]

    # VACUUM FULL should change the relfilenode
    expect(new_relfilenode).not_to eq(old_relfilenode)

    sleep 3

    # The relfile_history should have an entry for this
    result = conn.exec(<<~SQL)
      SELECT old_relnumber, new_relnumber
      FROM smgr_stats.relfile_history
      WHERE old_relnumber = #{old_relfilenode}
        AND new_relnumber = #{new_relfilenode}
    SQL

    expect(result.ntuples).to eq(1)
    row = result[0]
    expect(row["old_relnumber"]).to eq(old_relfilenode)
    expect(row["new_relnumber"]).to eq(new_relfilenode)
  end

  # get_table_history() follows the relfilenode lineage chain via relfile_history.
  # Each rewrite operation (VACUUM FULL, TRUNCATE, CLUSTER, etc.) creates a new relfilenode.

  it "get_table_history() finds history for a table with no rewrites (1 relfilenode)" do
    conn.exec("CREATE TABLE test_no_rewrite (id int, data text)")
    relfilenode = conn.exec(
      "SELECT relfilenode FROM pg_class WHERE relname = 'test_no_rewrite'"
    )[0]["relfilenode"]

    conn.exec("INSERT INTO test_no_rewrite SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
    conn.exec("CHECKPOINT")
    sleep 3 # Wait for collection

    result = conn.exec(<<~SQL)
      SELECT DISTINCT relnumber
      FROM smgr_stats.get_table_history('test_no_rewrite'::regclass)
      WHERE forknum = 0
    SQL

    # No rewrites = lineage contains only 1 relfilenode
    expect(result.ntuples).to eq(1)
    expect(result[0]["relnumber"]).to eq(relfilenode)
  end

  it "get_table_history() follows lineage through VACUUM FULL (2 relfilenodes)" do
    conn.exec("CREATE TABLE test_one_rewrite (id int, data text)")
    conn.exec("INSERT INTO test_one_rewrite SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
    conn.exec("CHECKPOINT")

    rfn1 = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'test_one_rewrite'")[0]["relfilenode"]
    sleep 3 # Collect stats for rfn1

    conn.exec("VACUUM FULL test_one_rewrite")
    rfn2 = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'test_one_rewrite'")[0]["relfilenode"]
    expect(rfn2).not_to eq(rfn1)

    conn.exec("INSERT INTO test_one_rewrite VALUES (999, 'after rewrite')")
    conn.exec("CHECKPOINT")
    sleep 3 # Collect stats for rfn2

    result = conn.exec(<<~SQL)
      SELECT DISTINCT relnumber
      FROM smgr_stats.get_table_history('test_one_rewrite'::regclass)
      WHERE forknum = 0
      ORDER BY relnumber
    SQL

    # 1 rewrite = lineage contains 2 relfilenodes
    expect(result.ntuples).to eq(2)
    expect(result.map { |r| r["relnumber"] }).to contain_exactly(rfn1, rfn2)
  end

  it "get_table_history() follows lineage through multiple rewrites (3 relfilenodes)" do
    conn.exec("CREATE TABLE test_multi_rewrite (id int, data text)")
    conn.exec("INSERT INTO test_multi_rewrite VALUES (1, 'initial')")
    conn.exec("CHECKPOINT")

    rfn1 = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'test_multi_rewrite'")[0]["relfilenode"]
    sleep 3

    # First rewrite
    conn.exec("TRUNCATE test_multi_rewrite")
    rfn2 = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'test_multi_rewrite'")[0]["relfilenode"]
    expect(rfn2).not_to eq(rfn1)

    conn.exec("INSERT INTO test_multi_rewrite VALUES (2, 'after truncate')")
    conn.exec("CHECKPOINT")
    sleep 3

    # Second rewrite
    conn.exec("VACUUM FULL test_multi_rewrite")
    rfn3 = conn.exec("SELECT relfilenode FROM pg_class WHERE relname = 'test_multi_rewrite'")[0]["relfilenode"]
    expect(rfn3).not_to eq(rfn2)

    conn.exec("INSERT INTO test_multi_rewrite VALUES (3, 'after vacuum full')")
    conn.exec("CHECKPOINT")
    sleep 3

    result = conn.exec(<<~SQL)
      SELECT DISTINCT relnumber
      FROM smgr_stats.get_table_history('test_multi_rewrite'::regclass)
      WHERE forknum = 0
    SQL

    # 2 rewrites = lineage contains 3 relfilenodes
    expect(result.ntuples).to eq(3)
    expect(result.map { |r| r["relnumber"] }).to contain_exactly(rfn1, rfn2, rfn3)
  end
end

RSpec.describe "pg_smgrstat metadata hook resolution" do
  include_context "pg instance"

  # Note: These tests verify that metadata IS resolved after operations complete.
  # They don't isolate which specific hook resolved metadata, since:
  # - ProcessUtility fires for DDL (CREATE, TRUNCATE, VACUUM, COPY)
  # - ExecutorEnd fires for DML (INSERT, SELECT, UPDATE, DELETE)
  # But comments try to explain how it is handled internally

  context "ProcessUtility hook (DDL operations)" do
    # These tests create NEW relfilenodes, so metadata must be resolved by ProcessUtility
    # (no prior operation could have resolved it)

    it "resolves metadata for newly created index" do
      conn.exec("CREATE TABLE test_idx_hook (id int, data text)")
      conn.exec("INSERT INTO test_idx_hook SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
      conn.exec("CREATE INDEX idx_test_idx_hook ON test_idx_hook(id)")

      # Index relfilenode is new - only ProcessUtility could have resolved it
      index_relfilenode = conn.exec(
        "SELECT relfilenode FROM pg_class WHERE relname = 'idx_test_idx_hook'"
      )[0]["relfilenode"]

      result = conn.exec(<<~SQL)
        SELECT reloid, relname, nspname, relkind
        FROM smgr_stats.current()
        WHERE relnumber = #{index_relfilenode} AND forknum = 0
      SQL

      expect(result.ntuples).to eq(1)
      row = result[0]
      expect(row["relname"]).to eq("idx_test_idx_hook")
      expect(row["nspname"]).to eq("public")
      expect(row["relkind"]).to eq("i")
    end

    it "resolves metadata for new relfilenode after TRUNCATE" do
      conn.exec("CREATE TABLE test_truncate_hook (id int, data text)")
      conn.exec("INSERT INTO test_truncate_hook SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")

      old_relfilenode = conn.exec(
        "SELECT relfilenode FROM pg_class WHERE relname = 'test_truncate_hook'"
      )[0]["relfilenode"]

      conn.exec("TRUNCATE test_truncate_hook")

      new_relfilenode = conn.exec(
        "SELECT relfilenode FROM pg_class WHERE relname = 'test_truncate_hook'"
      )[0]["relfilenode"]

      # Verify relfilenode changed
      expect(new_relfilenode).not_to eq(old_relfilenode)

      # Insert to trigger I/O on new relfilenode (current() only shows active entries)
      conn.exec("INSERT INTO test_truncate_hook VALUES (1, 'test')")

      # New relfilenode - only ProcessUtility (for TRUNCATE) or ExecutorEnd (for INSERT) could resolve
      result = conn.exec(<<~SQL)
        SELECT reloid, relname, nspname, relkind
        FROM smgr_stats.current()
        WHERE relnumber = #{new_relfilenode} AND forknum = 0
      SQL

      expect(result.ntuples).to eq(1)
      row = result[0]
      expect(row["relname"]).to eq("test_truncate_hook")
      expect(row["nspname"]).to eq("public")
      expect(row["relkind"]).to eq("r")
    end
  end

  context "ExecutorEnd hook (DML operations) with server restart" do
    # To test ExecutorEnd specifically, we need INSERT/SELECT to be the FIRST operation
    # that creates the stats entry. This requires restarting the server to clear the
    # in-memory dshash, then doing DML as the first operation.

    it "resolves metadata when INSERT is first operation after restart" do
      # Create table (metadata resolved by ProcessUtility)
      conn.exec("CREATE TABLE test_insert_restart (id int, data text)")

      # Get relfilenode before restart
      relfilenode = conn.exec(
        "SELECT relfilenode FROM pg_class WHERE relname = 'test_insert_restart'"
      )[0]["relfilenode"]

      # Restart server - this clears the in-memory dshash
      pg.restart

      # Get fresh connection after restart
      pg.connect do |c|
        # INSERT is now the first operation - creates new entry, ExecutorEnd must resolve
        c.exec("INSERT INTO test_insert_restart VALUES (1, 'test')")

        result = c.exec(<<~SQL)
          SELECT reloid, relname, nspname, relkind
          FROM smgr_stats.current()
          WHERE relnumber = #{relfilenode} AND forknum = 0
        SQL

        expect(result.ntuples).to eq(1)
        row = result[0]
        expect(row["relname"]).to eq("test_insert_restart")
        expect(row["nspname"]).to eq("public")
        expect(row["relkind"]).to eq("r")
      end
    end

    it "resolves metadata when SELECT is first operation after restart" do
      # Create and populate table
      conn.exec("CREATE TABLE test_select_restart (id int, data text)")
      conn.exec("INSERT INTO test_select_restart SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
      conn.exec("CHECKPOINT")

      relfilenode = conn.exec(
        "SELECT relfilenode FROM pg_class WHERE relname = 'test_select_restart'"
      )[0]["relfilenode"]

      # Restart server - clears dshash
      pg.restart

      # Evict buffers and SELECT - creates new entry, ExecutorEnd must resolve
      pg.evict_buffers
      pg.connect do |c|
        c.exec("SELECT * FROM test_select_restart WHERE id > 0")

        result = c.exec(<<~SQL)
          SELECT reloid, relname, nspname, relkind
          FROM smgr_stats.current()
          WHERE relnumber = #{relfilenode} AND forknum = 0
        SQL

        expect(result.ntuples).to eq(1)
        row = result[0]
        expect(row["relname"]).to eq("test_select_restart")
        expect(row["nspname"]).to eq("public")
        expect(row["relkind"]).to eq("r")
      end
    end
  end
end

