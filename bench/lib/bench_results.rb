require "json"
require "time"

# Collects benchmark results and produces reports.
class BenchResults
  def initialize(scenario_name:)
    @scenario_name = scenario_name
    # {workload_name => {variant => [result_hashes]}}
    @data = {}
  end

  def add(workload_name:, variant:, result:)
    @data[workload_name] ||= {}
    @data[workload_name][variant] ||= []
    @data[workload_name][variant] << result
  end

  def print_table
    puts ""
    puts "pg_smgrstat Benchmark Results"
    puts "Scenario: #{@scenario_name}"
    puts ""

    has_both = @data.values.any? { |v| v.key?("baseline") && v.key?("extension") }

    if has_both
      print_comparison_table
    else
      print_single_table
    end
    puts ""
  end

  def save_json(path)
    output = {
      scenario: @scenario_name,
      timestamp: Time.now.iso8601,
      workloads: {}
    }

    @data.each do |name, variants|
      output[:workloads][name] = {}
      variants.each do |variant, runs|
        stats = compute_stats(runs)
        output[:workloads][name][variant] = {
          runs: runs,
          summary: stats
        }
      end
    end

    File.write(path, JSON.pretty_generate(output) + "\n")
  end

  private

  def print_comparison_table
    header = format("| %-28s | %11s | %11s | %8s |", "Workload", "Base TPS", "Ext TPS", "Overhead")
    separator = "+#{"-" * 30}+#{"-" * 13}+#{"-" * 13}+#{"-" * 10}+"

    puts separator
    puts header
    puts separator

    @data.each do |name, variants|
      base_stats = compute_stats(variants["baseline"] || [])
      ext_stats = compute_stats(variants["extension"] || [])

      base_str = format_tps(base_stats)
      ext_str = format_tps(ext_stats)

      overhead = if base_stats[:tps_mean] && ext_stats[:tps_mean] && base_stats[:tps_mean] > 0
                   pct = ((base_stats[:tps_mean] - ext_stats[:tps_mean]) / base_stats[:tps_mean] * 100)
                   format("%+.1f%%", pct)
                 else
                   "N/A"
                 end

      puts format("| %-28s | %11s | %11s | %8s |", name, base_str, ext_str, overhead)
    end

    puts separator
  end

  def print_single_table
    header = format("| %-28s | %11s | %13s |", "Workload", "TPS", "Latency (ms)")
    separator = "+#{"-" * 30}+#{"-" * 13}+#{"-" * 15}+"

    puts separator
    puts header
    puts separator

    @data.each do |name, variants|
      variants.each do |_variant, runs|
        stats = compute_stats(runs)
        tps_str = format_tps(stats)
        lat_str = stats[:lat_mean] ? format("%.2f", stats[:lat_mean]) : "N/A"
        puts format("| %-28s | %11s | %13s |", name, tps_str, lat_str)
      end
    end

    puts separator
  end

  def compute_stats(runs)
    return {tps_mean: nil, tps_stddev: nil, lat_mean: nil} if runs.empty?

    tps_values = runs.map { |r| r[:tps] }.compact
    lat_values = runs.map { |r| r[:latency_avg_ms] }.compact

    {
      tps_mean: mean(tps_values),
      tps_stddev: stddev(tps_values),
      lat_mean: mean(lat_values),
      lat_stddev: stddev(lat_values)
    }
  end

  def format_tps(stats)
    return "N/A" unless stats[:tps_mean]
    if stats[:tps_stddev] && stats[:tps_stddev] > 0
      format("%.0f\u00b1%.0f", stats[:tps_mean], stats[:tps_stddev])
    else
      format("%.0f", stats[:tps_mean])
    end
  end

  def mean(values)
    return nil if values.empty?
    values.sum / values.size.to_f
  end

  def stddev(values)
    return nil if values.size < 2
    m = mean(values)
    variance = values.sum { |v| (v - m) ** 2 } / (values.size - 1).to_f
    Math.sqrt(variance)
  end
end
