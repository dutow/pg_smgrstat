RSpec.describe "pg_smgrstat metrics" do
  include_context "pg instance"

  it "auto-creates the extension" do
    result = conn.exec("SELECT 1 FROM pg_extension WHERE extname = 'pg_smgrstat'")
    expect(result.ntuples).to eq(1)
  end

  it "creates the history table" do
    result = conn.exec(<<~SQL)
      SELECT 1 FROM information_schema.tables
      WHERE table_schema = 'smgr_stats' AND table_name = 'history'
    SQL
    expect(result.ntuples).to eq(1)
  end

  context "current() function" do
    it "reports write metrics immediately" do
      conn.exec("CREATE TABLE test_current_writes (id int, data text)")
      conn.exec("INSERT INTO test_current_writes SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
      conn.exec("CHECKPOINT")

      result = conn.exec("SELECT sum(writes) AS w, sum(write_blocks) AS wb FROM smgr_stats.current()")
      expect(result[0]["w"].to_i).to be > 0
      expect(result[0]["wb"].to_i).to be > 0
    end

    it "reports read metrics immediately" do
      pg.evict_buffers
      conn.exec("SELECT count(*) FROM test_current_writes")

      result = conn.exec("SELECT sum(reads) AS r, sum(read_blocks) AS rb FROM smgr_stats.current()")
      expect(result[0]["r"].to_i).to be > 0
      expect(result[0]["rb"].to_i).to be > 0
    end
  end
end

RSpec.describe "pg_smgrstat background worker",
               extra_config: {"smgr_stats.collection_interval" => "2"} do
  include_context "pg instance"

  it "collects write metrics with a bucket_id" do
    conn.exec("CREATE TABLE test_worker_writes (id int, data text)")
    conn.exec("INSERT INTO test_worker_writes SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
    conn.exec("CHECKPOINT")

    sleep 3

    result = conn.exec("SELECT DISTINCT bucket_id FROM smgr_stats.history WHERE writes > 0")
    expect(result.ntuples).to be >= 1
    expect(result[0]["bucket_id"].to_i).to be >= 1
  end

  it "collects read metrics with a bucket_id" do
    pg.evict_buffers
    conn.exec("SELECT count(*) FROM test_worker_writes")

    sleep 3

    result = conn.exec("SELECT DISTINCT bucket_id FROM smgr_stats.history WHERE reads > 0")
    expect(result.ntuples).to be >= 1
    expect(result[0]["bucket_id"].to_i).to be >= 1
  end

  it "assigns increasing bucket_ids across multiple collections" do
    # Clear history from previous tests
    conn.exec("TRUNCATE smgr_stats.history")

    # First bucket: generate writes
    conn.exec("CREATE TABLE test_bucket_1 (id int, data text)")
    conn.exec("INSERT INTO test_bucket_1 SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
    conn.exec("CHECKPOINT")

    sleep 3

    first_buckets = conn.exec("SELECT DISTINCT bucket_id FROM smgr_stats.history ORDER BY bucket_id")
    expect(first_buckets.ntuples).to be >= 1
    first_bucket_id = first_buckets[first_buckets.ntuples - 1]["bucket_id"].to_i

    # Second bucket: generate more writes
    conn.exec("CREATE TABLE test_bucket_2 (id int, data text)")
    conn.exec("INSERT INTO test_bucket_2 SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
    conn.exec("CHECKPOINT")

    sleep 3

    all_buckets = conn.exec("SELECT DISTINCT bucket_id FROM smgr_stats.history ORDER BY bucket_id")
    last_bucket_id = all_buckets[all_buckets.ntuples - 1]["bucket_id"].to_i

    expect(last_bucket_id).to be > first_bucket_id
  end

  it "groups multiple entries under the same bucket_id" do
    conn.exec("TRUNCATE smgr_stats.history")

    # Generate I/O across multiple relations in the same interval
    conn.exec("CREATE TABLE test_bucket_a (id int, data text)")
    conn.exec("CREATE TABLE test_bucket_b (id int, data text)")
    conn.exec("INSERT INTO test_bucket_a SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
    conn.exec("INSERT INTO test_bucket_b SELECT g, repeat('x', 100) FROM generate_series(1, 100) g")
    conn.exec("CHECKPOINT")

    sleep 3

    entries = conn.exec("SELECT count(*) AS n FROM smgr_stats.history WHERE writes > 0")
    expect(entries[0]["n"].to_i).to be > 1

    buckets = conn.exec("SELECT DISTINCT bucket_id FROM smgr_stats.history WHERE writes > 0")
    expect(buckets.ntuples).to eq(1)
  end
end
