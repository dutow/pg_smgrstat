require_relative "../../spec/support/pg_instance"
require "pg"

# Extended PgInstance for benchmarking with database creation and dump support.
class BenchInstance < PgInstance
  def createdb(name)
    run_cmd(File.join(@pg_bindir, "createdb"), "-h", "127.0.0.1", "-p", @port.to_s, name)
  end

  def wait_for_extension(timeout: 30)
    deadline = Time.now + timeout
    connect do |c|
      loop do
        result = c.exec("SELECT 1 FROM pg_extension WHERE extname = 'pg_smgrstat'")
        return if result.ntuples > 0
        raise "Extension not created within #{timeout}s" if Time.now >= deadline
        sleep 0.5
      end
    end
  end

  def pgbench_bin
    File.join(@pg_bindir, "pgbench")
  end

  def pg_dump_bin
    File.join(@pg_bindir, "pg_dump")
  end

  def dump_stats(output_path, dbname: "postgres")
    # Copy to a non-extension-owned table so pg_dump includes the definition
    connect(dbname: dbname) do |c|
      c.exec("SET client_min_messages = warning")
      c.exec("DROP TABLE IF EXISTS public.smgr_stats_dump")
      c.exec("CREATE TABLE public.smgr_stats_dump AS SELECT * FROM smgr_stats.history")
    end

    run_cmd(pg_dump_bin, "--table=public.smgr_stats_dump",
            "-h", "127.0.0.1", "-p", @port.to_s,
            "--inserts", "--no-owner", "--no-privileges",
            "-f", output_path, dbname)
  end

  # Expose bindir and run_cmd for use by benchmark runners.
  def bindir
    @pg_bindir
  end

  def exec_cmd(*cmd)
    run_cmd(*cmd)
  end

  def server_log
    File.read(@log_file)
  rescue Errno::ENOENT
    ""
  end
end
