#!/usr/bin/env ruby
require "yaml"
require "fileutils"
require "optparse"
require_relative "lib/bench_instance"
require_relative "lib/bench_pgbench"
require_relative "lib/bench_sysbench"
require_relative "lib/bench_results"

options = {
  scenario: File.join(__dir__, "scenarios", "default.yml"),
  config: File.join(__dir__, "config.yml"),
  mode: "both",
  runs: nil,
  dump_stats: false,
  skip_build: false,
  verbose: false
}

OptionParser.new do |opts|
  opts.banner = "Usage: ruby bench/bench_runner.rb [OPTIONS]"
  opts.on("--scenario FILE", "Scenario YAML (default: bench/scenarios/default.yml)") { |f| options[:scenario] = f }
  opts.on("--config FILE", "Config YAML (default: bench/config.yml)") { |f| options[:config] = f }
  opts.on("--mode MODE", %w[both baseline extension], "both/baseline/extension (default: both)") { |m| options[:mode] = m }
  opts.on("--runs N", Integer, "Override runs count from scenario") { |n| options[:runs] = n }
  opts.on("--dump-stats", "pg_dump smgr_stats.history after extension runs") { options[:dump_stats] = true }
  opts.on("--skip-build", "Don't rebuild the extension") { options[:skip_build] = true }
  opts.on("--verbose", "Print benchmark tool output in real-time") { options[:verbose] = true }
end.parse!

config = YAML.load_file(options[:config])
scenario = YAML.load_file(options[:scenario])
runs = options[:runs] || scenario["runs"]
extension_source = File.expand_path("..", __dir__)

# Results directory
timestamp = Time.now.strftime("%Y%m%d_%H%M%S")
results_dir = File.join(__dir__, "results", timestamp)
FileUtils.mkdir_p(results_dir)

# Build extension unless skipped
unless options[:skip_build]
  puts "Building extension (#{config["build_type"]})..."
  build_dir = File.join(extension_source, config["build_dir"])
  unless File.exist?(File.join(build_dir, "build.ninja"))
    system("meson", "setup", build_dir, extension_source,
           "-Dpg_config=#{config["pg_config"]}",
           "-Dbuildtype=#{config["build_type"]}",
           exception: true)
  end
  system("meson", "compile", "-C", build_dir, exception: true)
  system("meson", "install", "-C", build_dir, exception: true)
  puts "Build complete."
end

# Expand workloads: array params become individual entries
def expand_workloads(scenario)
  entries = []
  scenario["workloads"].each do |wl|
    case wl["type"]
    when "pgbench"
      clients_list = Array(wl["clients"])
      clients_list.each do |c|
        entries << {
          type: "pgbench",
          name: "pgbench_s#{wl["scale_factor"]}_c#{c}",
          scale_factor: wl["scale_factor"],
          clients: c,
          duration: wl["duration"],
          warmup: wl["warmup"] || 0
        }
      end
    when "sysbench"
      threads_list = Array(wl["threads"])
      threads_list.each do |t|
        entries << {
          type: "sysbench",
          name: "sysbench_#{wl["workload"]}_t#{t}",
          workload: wl["workload"],
          table_size: wl["table_size"],
          tables: wl["tables"],
          threads: t,
          duration: wl["duration"],
          warmup: wl["warmup"] || 0
        }
      end
    end
  end
  entries
end

def build_pg_settings(config, scenario)
  settings = (config["pg_settings"] || {}).dup
  if scenario["pg_settings_override"]
    settings.merge!(scenario["pg_settings_override"])
  end
  settings
end

def run_variant(variant, workloads, config, scenario, runs:, results:,
                results_dir:, dump_stats: false, verbose: false)
  pg_settings = build_pg_settings(config, scenario)
  ext_settings = config["extension_settings"] || {}
  dbname = "benchdb"

  workloads.each do |wl|
    puts "  #{wl[:name]} (#{variant})..."
    runs.times do |run_idx|
      extra_config = pg_settings.transform_keys(&:to_s)
      shared_preload = []

      if variant == "extension"
        ext_settings.each { |k, v| extra_config[k.to_s] = v.to_s }
        shared_preload = ["pg_smgrstat"]
      end

      instance = BenchInstance.new(
        pg_config: config["pg_config"],
        shared_preload_libraries: shared_preload,
        extra_config: extra_config
      )

      begin
        instance.start
        instance.wait_for_extension if variant == "extension"
        instance.createdb(dbname)

        result = case wl[:type]
                 when "pgbench"
                   runner = BenchPgbench.new(instance, verbose: verbose)
                   runner.init(scale_factor: wl[:scale_factor], dbname: dbname)
                   runner.run(clients: wl[:clients], duration: wl[:duration],
                              warmup: wl[:warmup], dbname: dbname)
                 when "sysbench"
                   runner = BenchSysbench.new(instance, verbose: verbose)
                   runner.prepare(workload: wl[:workload], table_size: wl[:table_size],
                                  tables: wl[:tables], dbname: dbname)
                   runner.run(workload: wl[:workload], threads: wl[:threads],
                              duration: wl[:duration], table_size: wl[:table_size],
                              tables: wl[:tables], dbname: dbname, warmup: wl[:warmup])
                 end

        results.add(workload_name: wl[:name], variant: variant, result: result)
        puts "    run #{run_idx + 1}: #{format("%.0f", result[:tps])} tps"

        if dump_stats && variant == "extension"
          stats_db = ext_settings["smgr_stats.database"] || "postgres"

          # Verify stats are in shared memory before stopping
          instance.connect(dbname: stats_db) do |c|
            current = c.exec("SELECT count(*) AS n, coalesce(sum(reads), 0) AS r, coalesce(sum(writes), 0) AS w FROM smgr_stats.current()")
            puts "    current stats: #{current[0]["n"]} entries, #{current[0]["r"]} reads, #{current[0]["w"]} writes"
          end

          # Stop to flush stats, restart to dump
          instance.stop
          instance.restart
          dump_path = File.join(results_dir, "stats_run#{run_idx + 1}.sql")
          instance.dump_stats(dump_path, dbname: stats_db)
          puts "    stats dumped to #{dump_path}"
        end
      ensure
        if verbose && instance
          log = instance.server_log
          worker_lines = log.lines.grep(/smgr_stats|worker|FATAL|ERROR|PANIC/)
          unless worker_lines.empty?
            puts "    --- server log (filtered) ---"
            worker_lines.each { |l| puts "    #{l}" }
          end
        end
        instance&.cleanup
      end
    end
  end
end

# Main execution
scenario_name = File.basename(options[:scenario])
results = BenchResults.new(scenario_name: scenario_name)
workloads = expand_workloads(scenario)

puts "Scenario: #{scenario_name} (#{runs} runs per workload)"
puts "Mode: #{options[:mode]}"
puts ""

variants = case options[:mode]
           when "both" then %w[baseline extension]
           when "baseline" then %w[baseline]
           when "extension" then %w[extension]
           end

variants.each do |variant|
  puts "Running #{variant} variant..."
  run_variant(variant, workloads, config, scenario, runs: runs, results: results,
              results_dir: results_dir, dump_stats: options[:dump_stats],
              verbose: options[:verbose])
end

# Output results
results.print_table
json_path = File.join(results_dir, "summary.json")
results.save_json(json_path)
puts "Results saved to #{json_path}"
