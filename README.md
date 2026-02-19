# Seattle Public Parking Availability Map (MVP)

Web app MVP that visualizes Seattle paid parking availability on a map using a local 60-minute snapshot file.

## MVP Goal

- View parking availability by color on a Seattle map
- Search by address/street text
- Filter by observation time within the 60-minute window
- Click map points to view occupancy and availability details

## Data Used

- Local file: `data/hiyf-7edq-latest-60-min.json`
- Source dataset: Seattle Open Data `hiyf-7edq`

## Features

- Color-coded availability markers:
  - Green: 60-100%
  - Orange: 30-59%
  - Red: 0-29%
- Address/street search (matches blockface and area text)
- Time slider for available observation buckets
- Minimum availability slider
- Popup details on selection:
  - Blockface
  - Observed time
  - Occupied, total, available spaces
  - Occupancy %, availability %
  - Area, subarea, category

## Run Locally

No build step required.

```bash
cd /Users/bensteineman/YukisProject
ruby -run -e httpd . -p 8080
```

Open:

- `http://localhost:8080`

## Optional Low-Memory Mode (24h data)

If you want 24-hour playback without loading the full 24h file into browser memory:

1. Build the 30-minute aggregated file:
```bash
./scripts/build_hiyf_30min_with_stats.bsh
```
2. Split by time bucket and create an index:
```bash
./scripts/build_hiyf_30min_chunks.bsh
```

This creates:
- `data/hiyf-7edq-latest-24h-30min-index.json`
- `data/hiyf-7edq-latest-24h-30min-by-time/*.json`

When the index exists, `app.js` automatically loads chunk mode and fetches only the selected time bucket.

## Files

- `index.html` UI layout and controls
- `styles.css` styling
- `app.js` data loading, filters, map rendering
- `data/hiyf-7edq-latest-60-min.json` local MVP dataset
- `PROJECT_CONTEXT.md` product and architecture context
- `AGENTS.md` project instructions for coding agents
