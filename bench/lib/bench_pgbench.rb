require "open3"

# Runs pgbench workloads and parses results.
class BenchPgbench
  def initialize(instance, verbose: false)
    @instance = instance
    @verbose = verbose
  end

  def init(scale_factor:, dbname:)
    run_pgbench("-i", "-s", scale_factor.to_s,
                "-h", "127.0.0.1", "-p", @instance.port.to_s, dbname)
  end

  def run(clients:, duration:, warmup: 0, dbname:)
    jobs = [clients, 4].min
    args = ["-c", clients.to_s, "-j", jobs.to_s, "-T", duration.to_s, "-P", "5"]
    if warmup > 0
      # pgbench doesn't have a warmup flag; we run a short warmup pass first
      run_pgbench("-c", clients.to_s, "-j", jobs.to_s, "-T", warmup.to_s,
                  "-h", "127.0.0.1", "-p", @instance.port.to_s, dbname)
    end
    output = run_pgbench(*args, "-h", "127.0.0.1", "-p", @instance.port.to_s, dbname)
    parse_output(output)
  end

  private

  def run_pgbench(*args)
    cmd = [@instance.pgbench_bin] + args
    $stderr.puts "  > #{cmd.join(" ")}" if @verbose
    out, err, status = Open3.capture3(*cmd)
    $stderr.puts out if @verbose
    unless status.success?
      raise "pgbench failed: #{cmd.join(" ")}\nstdout: #{out}\nstderr: #{err}"
    end
    out
  end

  def parse_output(output)
    tps = nil
    latency_avg = nil
    latency_stddev = nil

    output.each_line do |line|
      case line
      when /^tps = ([\d.]+) \(without initial connection time\)/
        tps = $1.to_f
      when /^tps = ([\d.]+) \(excluding connections establishing\)/
        tps = $1.to_f
      when /latency average = ([\d.]+) ms/
        latency_avg = $1.to_f
      when /latency stddev = ([\d.]+) ms/
        latency_stddev = $1.to_f
      end
    end

    raise "Failed to parse pgbench TPS from output:\n#{output}" unless tps

    {tps: tps, latency_avg_ms: latency_avg, latency_stddev_ms: latency_stddev}
  end
end
