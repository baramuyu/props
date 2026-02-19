#!/usr/bin/env ruby
require "json"

outfile = ARGV[0]
agg_files = ARGV[1..] || []
abort("usage: merge_hiyf_chunk_aggs.rb <out.json> <agg1.json> ...") if outfile.nil? || agg_files.empty?

def choose_snapshot!(base, incoming)
  return incoming if base["snapshot"].nil?
  return base if incoming["snapshot"].nil?

  base_exact = base["snapshot_exact"] ? 1 : 0
  in_exact = incoming["snapshot_exact"] ? 1 : 0
  return incoming if in_exact > base_exact
  return base if in_exact < base_exact
  return incoming if incoming["snapshot_epoch"] < base["snapshot_epoch"]
  base
end

merged = {}

agg_files.each do |f|
  data = JSON.parse(File.read(f))
  data.each do |k, g|
    if !merged.key?(k)
      merged[k] = g
      next
    end

    m = merged[k]
    m["sum"] += g["sum"]
    m["count"] += g["count"]
    m["min"] = [m["min"], g["min"]].compact.min
    m["max"] = [m["max"], g["max"]].compact.max

    if m["first_epoch"].nil? || (!g["first_epoch"].nil? && g["first_epoch"] < m["first_epoch"])
      m["first_epoch"] = g["first_epoch"]
      m["first_occ"] = g["first_occ"]
    end
    if m["last_epoch"].nil? || (!g["last_epoch"].nil? && g["last_epoch"] > m["last_epoch"])
      m["last_epoch"] = g["last_epoch"]
      m["last_occ"] = g["last_occ"]
    end

    chosen = choose_snapshot!(m, g)
    m["snapshot"] = chosen["snapshot"]
    m["snapshot_exact"] = chosen["snapshot_exact"]
    m["snapshot_epoch"] = chosen["snapshot_epoch"]
  end
end

rows = merged.values
  .sort_by { |g| [g["bucket_epoch"], g["sourceelementkey"]] }
  .map do |g|
    avg = g["count"].positive? ? (g["sum"] / g["count"]) : nil
    snap = g["snapshot"] || {}
    delta = if g["first_occ"].nil? || g["last_occ"].nil?
              nil
            else
              g["last_occ"] - g["first_occ"]
            end
    snap.merge(
      "bucketstartdatetime" => g["bucket_start"],
      "minute_stats" => {
        "occupied_avg" => avg&.round(4),
        "occupied_min" => g["min"],
        "occupied_max" => g["max"],
        "occupied_first" => g["first_occ"],
        "occupied_last" => g["last_occ"],
        "occupied_delta" => delta,
        "estimated_enter" => delta.nil? ? nil : [delta, 0].max,
        "estimated_exit" => delta.nil? ? nil : [-delta, 0].max,
        "coverage_samples" => g["count"]
      }
    )
  end

File.write(outfile, JSON.pretty_generate(rows))
