require "socket"
require_relative "support/pg_instance"

PG_CONFIG = ENV.fetch("PG_CONFIG", "pg_config")
TEST_DATABASE = "testdb"

module CrossDbHelpers
  def lookup_relfilenode(conn, table_name)
    conn.exec("SELECT relfilenode FROM pg_class WHERE relname = $1", [table_name])[0]["relfilenode"]
  end

  def lookup_table_oid(conn, table_name)
    conn.exec("SELECT oid FROM pg_class WHERE relname = $1", [table_name])[0]["oid"]
  end

  def test_db_oid(conn)
    conn.exec("SELECT oid FROM pg_database WHERE datname = current_database()")[0]["oid"]
  end
end

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.include CrossDbHelpers
end

# Shared context providing a running PG instance with the extension loaded.
# Set metadata `extra_config:` on the describe block to add/override postgresql.conf settings.
# Default config includes smgr_chain and smgr_stats.database.
#
# Two connections are provided:
# - conn: connects to TEST_DATABASE where test tables are created
# - stats_conn: connects to postgres where smgr_stats extension/history lives
RSpec.shared_context "pg instance" do
  before(:all) do
    base_config = {
      "smgr_chain" => "smgr_stats,md",
      "smgr_stats.database" => "postgres"
    }
    extra = self.class.metadata[:extra_config] || {}

    @pg = PgInstance.new(
      pg_config: PG_CONFIG,
      shared_preload_libraries: ["pg_smgrstat"],
      extra_config: base_config.merge(extra)
    )
    @pg.start
    @pg.create_database(TEST_DATABASE)
    wait_for_extension
  end

  def wait_for_extension(timeout: 10)
    @pg.connect(dbname: "postgres") do |c|
      deadline = Time.now + timeout
      loop do
        result = c.exec("SELECT 1 FROM pg_extension WHERE extname = 'pg_smgrstat'")
        return if result.ntuples > 0
        raise "Extension not created within #{timeout}s" if Time.now >= deadline
        sleep 0.5
      end
    end
  end

  after(:all) do
    @pg.cleanup
  end

  let(:pg) { @pg }

  # Opens a fresh connection to test database for each test (for table creation/DML)
  let(:conn) { @conn = pg.connect(dbname: TEST_DATABASE) }

  # Opens a fresh connection to postgres database for each test (for stats queries)
  let(:stats_conn) { @stats_conn = pg.connect(dbname: "postgres") }

  after do
    @conn&.close
    @stats_conn&.close
  end
end
