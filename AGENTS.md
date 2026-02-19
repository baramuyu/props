# AGENTS.md

This file defines project-specific instructions for coding agents in this repository.

## Project Summary

- Project: Seattle Public Parking Availability Map (MVP)
- Goal: Visualize Seattle paid parking availability from a local 60-minute snapshot
- Current data file: `data/hiyf-7edq-latest-60-min.json`
- Stack: static frontend (`index.html`, `styles.css`, `app.js`) + Leaflet

## Source of Truth

- Product and scope context: `PROJECT_CONTEXT.md`
- User-facing usage/run instructions: `README.md`

If instructions conflict, priority is:
1. Explicit user request in current chat
2. `PROJECT_CONTEXT.md`
3. `README.md`
4. This `AGENTS.md`

## Working Rules

- Keep implementation simple and MVP-focused.
- Do not add new frameworks/build systems unless explicitly requested.
- Preserve existing UI behavior unless a change is requested.
- Prefer minimal, clear code over abstractions.
- Keep files ASCII unless file already requires otherwise.

## Data Rules

- Treat `data/hiyf-7edq-latest-60-min.json` as primary input for MVP.
- Parse numeric strings safely (`paidoccupancy`, `parkingspacecount`, etc.).
- Coordinates are `[longitude, latitude]`.
- Ignore malformed records instead of crashing UI.

## UI and Behavior Rules

- Maintain these MVP capabilities:
  - map rendering
  - street/address text search
  - time filter
  - availability color coding
  - popup details
- Keep map interactions smooth (avoid unnecessary recentering on every input change).
- Keep status/error messaging visible and concise.

## Testing and Validation

- For static checks, verify key assets and references after edits.
- For local smoke tests, use:
  - `ruby -run -e httpd . -p 8080`
- Validate at least:
  - `/`
  - `/app.js`
  - `/data/hiyf-7edq-latest-60-min.json`

## Git and Deployment

- Default branch: `main`
- Hosting target: GitHub Pages (`https://baramuyu.github.io/props/`)
- Do not rewrite git history unless user explicitly asks.
- Keep commit messages short and scoped.

## Out of Scope (unless requested)

- New backend services
- Database migrations
- Authentication
- Large architecture rewrites

