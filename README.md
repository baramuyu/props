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
- Time selector (`latest per blockface` or exact timestamp)
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

## Files

- `index.html` UI layout and controls
- `styles.css` styling
- `app.js` data loading, filters, map rendering
- `data/hiyf-7edq-latest-60-min.json` local MVP dataset
- `PROJECT_CONTEXT.md` product and architecture context
