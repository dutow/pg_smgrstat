require "open3"

# Runs sysbench OLTP workloads against a PostgreSQL instance and parses results.
class BenchSysbench
  def initialize(instance, verbose: false)
    @instance = instance
    @verbose = verbose
  end

  def prepare(workload:, table_size:, tables:, dbname:)
    run_sysbench(workload, "prepare",
                 table_size: table_size, tables: tables, dbname: dbname)
  end

  def run(workload:, threads:, duration:, table_size:, tables:, dbname:, warmup: 0)
    if warmup > 0
      run_sysbench(workload, "run",
                   table_size: table_size, tables: tables, dbname: dbname,
                   threads: threads, duration: warmup)
    end
    output = run_sysbench(workload, "run",
                          table_size: table_size, tables: tables, dbname: dbname,
                          threads: threads, duration: duration, report_interval: 5)
    parse_output(output)
  end

  def cleanup(workload:, table_size:, tables:, dbname:)
    run_sysbench(workload, "cleanup",
                 table_size: table_size, tables: tables, dbname: dbname)
  end

  private

  def run_sysbench(workload, command, table_size:, tables:, dbname:,
                   threads: 1, duration: nil, report_interval: nil)
    args = [
      "sysbench", workload, command,
      "--db-driver=pgsql",
      "--pgsql-host=127.0.0.1",
      "--pgsql-port=#{@instance.port}",
      "--pgsql-db=#{dbname}",
      "--pgsql-user=#{ENV["USER"] || "postgres"}",
      "--table-size=#{table_size}",
      "--tables=#{tables}",
      "--threads=#{threads}"
    ]
    args << "--time=#{duration}" if duration
    args << "--report-interval=#{report_interval}" if report_interval

    $stderr.puts "  > #{args.join(" ")}" if @verbose
    out, err, status = Open3.capture3(*args)
    $stderr.puts out if @verbose
    unless status.success?
      raise "sysbench failed: #{args.join(" ")}\nstdout: #{out}\nstderr: #{err}"
    end
    out
  end

  def parse_output(output)
    tps = nil
    qps = nil
    latency_avg = nil
    latency_p95 = nil
    latency_p99 = nil

    output.each_line do |line|
      case line.strip
      when /transactions:\s+\d+\s+\(([\d.]+) per sec/
        tps = $1.to_f
      when /queries:\s+\d+\s+\(([\d.]+) per sec/
        qps = $1.to_f
      when /avg:\s+([\d.]+)/
        latency_avg = $1.to_f
      when /95th percentile:\s+([\d.]+)/
        latency_p95 = $1.to_f
      when /99th percentile:\s+([\d.]+)/
        latency_p99 = $1.to_f
      end
    end

    raise "Failed to parse sysbench TPS from output:\n#{output}" unless tps

    {tps: tps, qps: qps, latency_avg_ms: latency_avg,
     latency_p95_ms: latency_p95, latency_p99_ms: latency_p99}
  end
end
