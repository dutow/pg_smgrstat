RSpec.describe "pg_smgrstat retention policy",
               extra_config: {"smgr_stats.collection_interval" => "2", "smgr_stats.retention_hours" => "1"} do
  include_context "pg instance"

  it "deletes history rows older than retention_hours" do
    # Generate some I/O so we have current entries
    conn.exec("CREATE TABLE test_retention (id int, data text)")
    conn.exec("INSERT INTO test_retention SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
    conn.exec("CHECKPOINT")

    # Wait for collection to capture current activity
    sleep 3

    # Verify we have recent entries
    result = stats_conn.exec("SELECT count(*) AS n FROM smgr_stats.history")
    expect(result[0]["n"].to_i).to be > 0

    # Insert old entries manually (2 hours ago, older than retention_hours=1)
    stats_conn.exec(<<~SQL)
      INSERT INTO smgr_stats.history (bucket_id, spcoid, dboid, relnumber, forknum, collected_at,
                                      reads, read_blocks, writes, write_blocks, extends, extend_blocks,
                                      truncates, fsyncs, sequential_reads, random_reads,
                                      sequential_writes, random_writes, active_seconds,
                                      first_access, last_access)
      VALUES (0, 1663, 0, 99999, 0, now() - interval '2 hours',
              100, 100, 50, 50, 0, 0, 0, 0, 80, 20, 40, 10, 5, now() - interval '2 hours', now() - interval '2 hours')
    SQL

    # Verify old entry exists
    old_count = stats_conn.exec("SELECT count(*) AS n FROM smgr_stats.history WHERE relnumber = 99999")
    expect(old_count[0]["n"].to_i).to eq(1)

    # Wait for next collection cycle (which runs retention)
    sleep 3

    # Old entry should be deleted
    old_count_after = stats_conn.exec("SELECT count(*) AS n FROM smgr_stats.history WHERE relnumber = 99999")
    expect(old_count_after[0]["n"].to_i).to eq(0)

    # Recent entries should still exist
    recent_count = stats_conn.exec("SELECT count(*) AS n FROM smgr_stats.history WHERE collected_at > now() - interval '1 hour'")
    expect(recent_count[0]["n"].to_i).to be > 0
  end
end

RSpec.describe "pg_smgrstat retention disabled",
               extra_config: {"smgr_stats.collection_interval" => "2", "smgr_stats.retention_hours" => "0"} do
  include_context "pg instance"

  it "keeps old entries when retention is disabled" do
    # Generate some I/O
    conn.exec("CREATE TABLE test_no_retention (id int)")
    conn.exec("INSERT INTO test_no_retention VALUES (1)")
    conn.exec("CHECKPOINT")

    sleep 3

    # Insert old entry (would be deleted if retention were enabled)
    stats_conn.exec(<<~SQL)
      INSERT INTO smgr_stats.history (bucket_id, spcoid, dboid, relnumber, forknum, collected_at,
                                      reads, read_blocks, writes, write_blocks, extends, extend_blocks,
                                      truncates, fsyncs, sequential_reads, random_reads,
                                      sequential_writes, random_writes, active_seconds,
                                      first_access, last_access)
      VALUES (0, 1663, 0, 88888, 0, now() - interval '1000 hours',
              100, 100, 50, 50, 0, 0, 0, 0, 80, 20, 40, 10, 5, now() - interval '1000 hours', now() - interval '1000 hours')
    SQL

    old_count = stats_conn.exec("SELECT count(*) AS n FROM smgr_stats.history WHERE relnumber = 88888")
    expect(old_count[0]["n"].to_i).to eq(1)

    # Wait for collection cycles
    sleep 5

    # Old entry should still exist (retention disabled)
    old_count_after = stats_conn.exec("SELECT count(*) AS n FROM smgr_stats.history WHERE relnumber = 88888")
    expect(old_count_after[0]["n"].to_i).to eq(1)
  end
end
