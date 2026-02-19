# PROJECT_LOG

Last updated: 2026-02-19

## Current Goal
Build and iterate a Seattle paid parking availability web app using Seattle Open Data dataset `hiyf-7edq`.

## Current App State
- Frontend: static app (`index.html`, `styles.css`, `app.js`) with Leaflet map.
- Time filter: changed from dropdown to range slider.
- Data source currently in app: `/data/hiyf-7edq-latest-24h-30min-with-stats.ndjson`.
- Popup includes occupancy, availability, and 30-min minute-level stats in app logic.
- App loader now parses NDJSON directly and supports precomputed `minute_stats` rows.

## Data Pipeline State
### Downloads
- 24h chunk downloader script exists: `scripts/download_hiyf_24h_chunks.bsh`
- Resume support added:
  - reuses manifest when present
  - skips already-downloaded chunk files
  - `--resume-only` mode bootstraps manifest from existing chunks if needed

### Reduction / Aggregation
- Chunk aggregation scripts added:
  - `scripts/aggregate_hiyf_ndjson_chunk.rb`
  - `scripts/merge_hiyf_chunk_aggs.rb`
- 30-min build script from chunks:
  - `scripts/build_hiyf_30min_from_chunks.bsh`
- Output targets:
  - `data/hiyf-7edq-latest-24h-30min-with-stats.json`
  - `data/hiyf-7edq-latest-24h-30min.json`
  - optional NDJSON: `data/hiyf-7edq-latest-24h-30min-with-stats.ndjson`

### Cleanup Applied
- Removed obsolete scripts from `scripts/` and kept only the canonical pipeline + helpers.
- Added `scripts/README.md` with current run commands.

## Known Open Work
1. Finish/verify full chunk download completeness.
2. Run and verify 30-min reduction from chunks.
3. Optionally switch app source to reduced file for better performance.
4. Clean up docs to match final chosen data source and workflow.
5. Commit/push pending local changes.

## Quick Commands
### Resume chunk download
```bash
bash scripts/download_hiyf_24h_chunks.bsh --resume-only
```

### Fresh chunk download (new window)
```bash
bash scripts/download_hiyf_24h_chunks.bsh --refresh
```

### Build reduced 30-min outputs from chunks
```bash
bash scripts/build_hiyf_30min_from_chunks.bsh
```

## Notes
- If `rg` is unavailable, scripts fallback to `grep` for row counting.
- Current git working tree has multiple modified/untracked files; no cleanup/revert has been performed.
