# PROJECT_LOG

Last updated: 2026-02-20

## Current Goal
Build and iterate a Seattle paid parking availability web app using Seattle Open Data dataset `hiyf-7edq`.

## Current App State
- Frontend: static app (`index.html`, `styles.css`, `app.js`) with Leaflet map.
- Time filter: changed from dropdown to range slider.
- Added day-of-week UI filter (`All Days`, `Sun`-`Sat` based on available rows).
- Added paid parking area UI filter (`All Areas` + dynamic `paid_parking_area` options).
- Changing area selection now re-renders and zooms map to the selected area result set.
- Fixed area zoom bug: map now applies `fitBounds` before low-zoom marker gating, so area selection reliably zooms in.
- Improved area switching: area change now zooms using all rows in that area (independent of current time/day slice), then applies active filters for display.
- Removed Day-of-Week and Minimum Availability controls from UI.
- Added white circle marker state for no-data points (`NO DATA` payment status).
- Data source currently in app: `/data/hiyf-7edq-latest-24h-30min-with-stats.ndjson`.
- Popup includes occupancy, availability, and 30-min minute-level stats in app logic.
- App loader now parses NDJSON directly and supports precomputed `minute_stats` rows.
- Fixed no-data rendering bug: no-data rows now always render as white circles (not free badges).
- Fixed time-bucket alignment bug for NDJSON: app now anchors bucket time from `occupancydatetime` instead of the offset `bucketstartdatetime` value.
- Time slider day window is anchored to Seattle time (`America/Los_Angeles`) and displays a full-day range from `12:00:00 AM` to `11:59:59 PM`.

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
1. Optional: run browser-level regression pass for area switch + time slider behavior.
2. Clean up docs to fully reflect current UI (remove old references in README).
3. Commit/push remaining local non-bugfix changes when ready.

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
