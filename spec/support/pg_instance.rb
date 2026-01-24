require "open3"
require "tmpdir"
require "fileutils"
require "pg"

# Manages a temporary PostgreSQL instance for integration testing.
#
# Handles initdb, configuration, start, stop, and cleanup.
# Expects the extension to be installed (`meson install`) before use.
# The instance uses a random port to avoid conflicts.
class PgInstance
  attr_reader :port, :data_dir, :log_file

  def initialize(pg_config:, shared_preload_libraries: [], extra_config: {})
    @pg_bindir = pg_config_value(pg_config, "--bindir")
    @shared_preload_libraries = shared_preload_libraries
    @extra_config = extra_config
    @port = find_available_port
    @data_dir = Dir.mktmpdir("pg_smgrstat_test_")
    @log_file = File.join(@data_dir, "server.log")
    @running = false
  end

  def start
    run_initdb
    write_config
    run_pg_ctl("start", "-l", @log_file, "-w")
    @running = true
  end

  def stop
    return unless @running

    run_pg_ctl("stop", "-m", "fast", "-w")
    @running = false
  end

  def restart
    stop
    run_pg_ctl("start", "-l", @log_file, "-w")
    @running = true
  end

  def cleanup
    stop if @running
    FileUtils.rm_rf(@data_dir)
  end

  def connect(dbname: "postgres", &block)
    conn = PG.connect(host: "127.0.0.1", port: @port, dbname: dbname)
    if block
      begin
        block.call(conn)
      ensure
        conn.close
      end
    else
      conn
    end
  end

  def evict_buffers
    connect do |c|
      c.exec("CREATE EXTENSION IF NOT EXISTS pg_buffercache") unless @buffercache_installed
      @buffercache_installed = true
      c.exec("SELECT pg_buffercache_evict_all()")
    end
  end

  def log_contents
    File.read(@log_file)
  rescue Errno::ENOENT
    ""
  end

  private

  def pg_config_value(pg_config, flag)
    out, status = Open3.capture2(pg_config, flag)
    raise "pg_config #{flag} failed" unless status.success?

    out.strip
  end

  def find_available_port
    server = TCPServer.new("127.0.0.1", 0)
    port = server.addr[1]
    server.close
    port
  end

  def run_initdb
    run_cmd(File.join(@pg_bindir, "initdb"),
            "-D", @data_dir,
            "--no-locale",
            "--encoding=UTF8",
            "-A", "trust")
  end

  def write_config
    conf_path = File.join(@data_dir, "postgresql.conf")
    File.open(conf_path, "a") do |f|
      f.puts "port = #{@port}"
      f.puts "listen_addresses = '127.0.0.1'"
      f.puts "unix_socket_directories = '#{@data_dir}'"
      f.puts "log_destination = 'stderr'"
      f.puts "logging_collector = off"

      unless @shared_preload_libraries.empty?
        f.puts "shared_preload_libraries = '#{@shared_preload_libraries.join(",")}'"
      end

      @extra_config.each do |key, value|
        f.puts "#{key} = '#{value}'"
      end
    end
  end

  def run_pg_ctl(*args)
    run_cmd(File.join(@pg_bindir, "pg_ctl"), "-D", @data_dir, *args)
  end

  def run_cmd(*cmd)
    out, err, status = Open3.capture3(*cmd)
    unless status.success?
      raise "Command failed: #{cmd.join(" ")}\nstdout: #{out}\nstderr: #{err}"
    end

    out
  end
end
