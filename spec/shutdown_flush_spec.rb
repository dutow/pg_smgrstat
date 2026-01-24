RSpec.describe "pg_smgrstat shutdown flush",
               extra_config: {"smgr_stats.collection_interval" => "3600"} do
  include_context "pg instance"

  it "persists pending stats to history on server stop" do
    conn.exec("CREATE TABLE test_flush (id int, data text)")
    conn.exec("INSERT INTO test_flush SELECT g, repeat('x', 1000) FROM generate_series(1, 2000) g")
    conn.exec("CHECKPOINT")

    pg.evict_buffers
    conn.exec("SELECT count(*) FROM test_flush")

    # Verify stats are in shared memory but NOT yet in history
    # (collection_interval is 3600s so no periodic collection has fired)
    current = conn.exec(<<~SQL)
      SELECT reads, read_blocks, writes, write_blocks,
             sequential_reads, random_reads
      FROM smgr_stats.current() c
      WHERE c.relnumber = (SELECT relfilenode FROM pg_class WHERE relname = 'test_flush')
        AND c.forknum = 0
    SQL
    expect(current.ntuples).to eq(1)
    expect(current[0]["reads"].to_i).to be > 0

    history_before = conn.exec("SELECT count(*) AS n FROM smgr_stats.history")
    expect(history_before[0]["n"].to_i).to eq(0)

    @conn = nil

    # Restart: stop sends SIGTERM to worker which triggers final collection,
    # then start brings the server back up.
    pg.restart

    new_conn = pg.connect
    begin
      history = new_conn.exec(<<~SQL)
        SELECT reads, read_blocks, writes, write_blocks,
               sequential_reads, random_reads
        FROM smgr_stats.history
        WHERE relnumber = (SELECT relfilenode FROM pg_class WHERE relname = 'test_flush')
          AND forknum = 0
      SQL
      expect(history.ntuples).to be >= 1

      row = history[0]
      expect(row["reads"].to_i).to be > 0
      expect(row["read_blocks"].to_i).to be > 0
      expect(row["writes"].to_i).to be > 0
      expect(row["write_blocks"].to_i).to be > 0
      expect(row["sequential_reads"].to_i).to be > 0
      expect(row["random_reads"].to_i).to eq(1)
    ensure
      new_conn.close
    end
  end
end
