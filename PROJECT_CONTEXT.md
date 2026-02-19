# Seattle Public Parking Availability Project Context

## Project Goal
Build a Seattle parking availability mapping system using Seattle public occupancy data, starting with dataset `hiyf-7edq`.

## Current Starting Point
- Local source file: `data/hiyf-7edq-latest-60-min.json`
- Initial mode: read local file, normalize records, render map availability
- Future mode: periodic ingestion from Seattle Open Data API

## Core User Outcomes
- View current estimated parking availability by blockface
- Filter by area, availability range, and parking category
- Understand occupancy trends over time

## Scope

### MVP
- Load and parse local JSON snapshot
- Normalize to canonical observation schema
- Compute occupancy and availability metrics
- Render map with color-coded availability
- Basic filters (min availability, area)

### Phase 2
- Automated data fetch (5-minute / 1-hour windows)
- Incremental upsert and dedup
- Time slider / recent history playback

### Phase 3
- Historical analytics dashboard
- Reliability monitoring and alerting
- Public deployment and scheduled jobs

## Canonical Data Contract
Store app-facing records as `parking_observations`:

- `observation_id` (string)
- `observed_at` (datetime)
- `source_dataset` (string)
- `source_element_key` (string)
- `blockface_name` (string)
- `side_of_street` (string)
- `paid_parking_area` (string)
- `paid_parking_subarea` (string|null)
- `parking_category` (string)
- `time_limit_minutes` (integer)
- `occupied_spaces` (integer)
- `total_spaces` (integer)
- `available_spaces` (integer)
- `occupancy_rate` (float 0..1)
- `availability_rate` (float 0..1)
- `longitude` (float)
- `latitude` (float)
- `ingested_at` (datetime)

## Architecture (Target)
- Ingestion: pull raw rows from Socrata API
- Normalization: map raw row to canonical schema + validation
- Storage: raw snapshots + normalized observations
- API layer: query latest/aggregated availability
- Frontend map: interactive filtering and marker rendering

## Data Rules and Assumptions
- `paidoccupancy` and `parkingspacecount` are string numerics and must be parsed
- `available_spaces = max(total_spaces - occupied_spaces, 0)`
- `occupancy_rate = occupied_spaces / total_spaces` when `total_spaces > 0`
- `availability_rate = 1 - occupancy_rate`
- Coordinates are `[longitude, latitude]`

## Quality and Reliability
- Deduplicate on `observation_id`
- Reject invalid records (missing timestamp, key, or coordinates)
- Track ingest stats: row count, valid count, dropped count, error reason
- Keep ingestion metadata: `window_start`, `window_end`, `downloaded_at`

## Agent Governance
- `AGENTS.md` at repo root is the project instruction file for coding agents.
- Agent work should remain MVP-focused and aligned with `README.md` + this context doc.

## Suggested Repository Structure
- `data/` raw snapshots
- `docs/` project docs, ADRs, runbooks
- `src/ingestion/` fetch + normalize
- `src/core/` metric calculations and schema utilities
- `src/api/` query endpoints (optional)
- `src/ui/` map application

## Milestones
1. Local-file MVP working from `data/hiyf-7edq-latest-60-min.json`
2. Add automated fetcher and append-only raw archive
3. Add normalized store + dedup
4. Add trend analytics and deploy

## Non-Goals (for MVP)
- Citywide traffic prediction
- Complex demand forecasting models
- Payment transaction integration
