# Scripts

Current canonical pipeline for this project:

1. Download 24h source data in API chunks (resumable)

```bash
bash scripts/download_hiyf_24h_chunks.bsh --resume-only
```

Use `--refresh` to start a new 24h window.

2. Build 30-min outputs from chunk files

```bash
bash scripts/build_hiyf_30min_from_chunks.bsh
```

Outputs:
- `data/hiyf-7edq-latest-24h-30min-with-stats.json`
- `data/hiyf-7edq-latest-24h-30min.json`

3. Optional NDJSON conversion (one record per line)

```bash
jq -c '.[]' data/hiyf-7edq-latest-24h-30min-with-stats.json > data/hiyf-7edq-latest-24h-30min-with-stats.ndjson
```

Supporting scripts (used by step 2):
- `scripts/aggregate_hiyf_ndjson_chunk.rb`
- `scripts/merge_hiyf_chunk_aggs.rb`
