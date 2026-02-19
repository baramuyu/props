#!/usr/bin/env ruby
require "json"
require "time"

infile = ARGV[0]
outfile = ARGV[1]
abort("usage: aggregate_hiyf_ndjson_chunk.rb <in.ndjson> <out.json>") unless infile && outfile

def parse_ts(ts)
  Time.strptime(ts, "%Y-%m-%dT%H:%M:%S.%L").utc
rescue ArgumentError
  nil
end

def bucket_start_time(t)
  Time.at(t.to_i - (t.to_i % 1800)).utc
end

def bucket_start_string(t)
  t.strftime("%Y-%m-%dT%H:%M:%S.000")
end

groups = {}

File.foreach(infile) do |line|
  line = line.strip
  next if line.empty?

  row = JSON.parse(line)
  ts_raw = row["occupancydatetime"]
  key = row["sourceelementkey"]
  occ = row["paidoccupancy"]
  next if ts_raw.nil? || key.nil? || occ.nil?

  ts = parse_ts(ts_raw)
  next if ts.nil?

  occ_i = Integer(occ) rescue nil
  next if occ_i.nil?

  bucket_t = bucket_start_time(ts)
  bucket_s = bucket_start_string(bucket_t)
  gk = "#{key}|#{bucket_s}"

  g = groups[gk]
  unless g
    g = {
      "sourceelementkey" => key,
      "bucket_start" => bucket_s,
      "bucket_epoch" => bucket_t.to_i,
      "snapshot" => nil,
      "snapshot_exact" => false,
      "snapshot_epoch" => nil,
      "sum" => 0.0,
      "count" => 0,
      "min" => nil,
      "max" => nil,
      "first_occ" => nil,
      "last_occ" => nil,
      "first_epoch" => nil,
      "last_epoch" => nil
    }
    groups[gk] = g
  end

  g["sum"] += occ_i
  g["count"] += 1
  g["min"] = g["min"].nil? ? occ_i : [g["min"], occ_i].min
  g["max"] = g["max"].nil? ? occ_i : [g["max"], occ_i].max

  if g["first_epoch"].nil? || ts.to_i < g["first_epoch"]
    g["first_epoch"] = ts.to_i
    g["first_occ"] = occ_i
  end
  if g["last_epoch"].nil? || ts.to_i > g["last_epoch"]
    g["last_epoch"] = ts.to_i
    g["last_occ"] = occ_i
  end

  exact_boundary = (ts.min % 30).zero? && ts.sec.zero?
  if g["snapshot"].nil?
    g["snapshot"] = row
    g["snapshot_exact"] = exact_boundary
    g["snapshot_epoch"] = ts.to_i
  elsif exact_boundary
    if !g["snapshot_exact"] || ts.to_i < g["snapshot_epoch"]
      g["snapshot"] = row
      g["snapshot_exact"] = true
      g["snapshot_epoch"] = ts.to_i
    end
  elsif !g["snapshot_exact"] && ts.to_i < g["snapshot_epoch"]
    g["snapshot"] = row
    g["snapshot_epoch"] = ts.to_i
  end
end

File.write(outfile, JSON.generate(groups))
