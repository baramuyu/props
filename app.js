const DATA_FILE = "/data/hiyf-7edq-latest-24h-30min-with-stats.ndjson";
const DEFAULT_CENTER = [47.6062, -122.3321];
const DEFAULT_ZOOM = 13;
const BUCKET_MS_SIZE = 30 * 60 * 1000;
const DETAIL_ZOOM_MIN = 14;

const map = L.map("map").setView(DEFAULT_CENTER, DEFAULT_ZOOM);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
}).addTo(map);

const markerLayer = L.layerGroup().addTo(map);

const addressSearch = document.getElementById("addressSearch");
const clearSearchBtn = document.getElementById("clearSearchBtn");
const timeRange = document.getElementById("timeRange");
const timeRangeValue = document.getElementById("timeRangeValue");
const minAvailability = document.getElementById("minAvailability");
const minAvailabilityValue = document.getElementById("minAvailabilityValue");
const refreshBtn = document.getElementById("refreshBtn");
const statusEl = document.getElementById("status");

const statBlockfaces = document.getElementById("statBlockfaces");
const statAvgOcc = document.getElementById("statAvgOcc");
const statAvgAvail = document.getElementById("statAvgAvail");
const statWindow = document.getElementById("statWindow");

let bucketRows = [];
let timeValues = [];
let timeEntries = [];
let hasFitOnce = false;
let useChunkMode = false;
let activeChunkFetch = null;

function setStatus(msg) {
  statusEl.textContent = msg;
}

function toNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function parseTimestamp(value) {
  const ms = new Date(value).getTime();
  return Number.isNaN(ms) ? null : ms;
}

function markerColor(availabilityPct) {
  if (availabilityPct >= 60) {
    return "#2e8b57";
  }
  if (availabilityPct >= 30) {
    return "#d68910";
  }
  return "#c0392b";
}

function formatLocalTime(ms) {
  if (!ms) {
    return "Unknown";
  }
  return new Date(ms).toLocaleString();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeRow(row) {
  const occupied = toNumber(row.paidoccupancy);
  const total = toNumber(row.parkingspacecount);
  const ts = parseTimestamp(row.occupancydatetime);
  const coords = row?.location?.coordinates;

  if (
    occupied === null ||
    total === null ||
    total <= 0 ||
    !ts ||
    !Array.isArray(coords) ||
    coords.length < 2
  ) {
    return null;
  }

  const lon = toNumber(coords[0]);
  const lat = toNumber(coords[1]);
  if (lon === null || lat === null) {
    return null;
  }

  const clampedOccupied = Math.max(0, Math.min(occupied, total));
  const available = Math.max(total - clampedOccupied, 0);
  const occupancyPct = (clampedOccupied / total) * 100;
  const availabilityPct = 100 - occupancyPct;

  return {
    sourceElementKey: row.sourceelementkey || `${lat.toFixed(5)},${lon.toFixed(5)}`,
    blockfaceName: row.blockfacename || "Unknown blockface",
    sideOfStreet: row.sideofstreet || "",
    paidParkingArea: row.paidparkingarea || "Unknown",
    paidParkingSubarea: row.paidparkingsubarea || "",
    parkingCategory: row.parkingcategory || "Unknown",
    observedAtMs: ts,
    observedAtRaw: row.occupancydatetime,
    occupiedSpaces: clampedOccupied,
    totalSpaces: total,
    availableSpaces: available,
    occupancyPct,
    availabilityPct,
    latitude: lat,
    longitude: lon,
  };
}

function normalizeBucketedRow(row) {
  const base = normalizeRow(row);
  if (!base) {
    return null;
  }

  const bucketStartMs =
    parseTimestamp(row.bucketstartdatetime) ||
    Math.floor(base.observedAtMs / BUCKET_MS_SIZE) * BUCKET_MS_SIZE;

  const stats = row.minute_stats || row.minuteStats || {};
  const occupiedAvg = toNumber(stats.occupied_avg);
  const occupiedMin = toNumber(stats.occupied_min);
  const occupiedMax = toNumber(stats.occupied_max);
  const occupiedFirst = toNumber(stats.occupied_first);
  const occupiedLast = toNumber(stats.occupied_last);
  const occupiedDelta = toNumber(stats.occupied_delta);
  const coverageSamples = toNumber(stats.coverage_samples);

  return {
    ...base,
    bucketStartMs,
    minuteStats: {
      occupiedAvg: occupiedAvg ?? base.occupiedSpaces,
      occupiedMin: occupiedMin ?? base.occupiedSpaces,
      occupiedMax: occupiedMax ?? base.occupiedSpaces,
      occupiedFirst: occupiedFirst ?? base.occupiedSpaces,
      occupiedLast: occupiedLast ?? base.occupiedSpaces,
      occupiedDelta: occupiedDelta ?? 0,
      coverageMinutes: coverageSamples ?? 1,
    },
  };
}

function parseRowsFromText(rawText) {
  if (DATA_FILE.endsWith(".ndjson")) {
    return rawText
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  }
  return JSON.parse(rawText);
}

function build30MinRows(rows) {
  const groups = new Map();

  for (const row of rows) {
    const bucketStartMs = Math.floor(row.observedAtMs / BUCKET_MS_SIZE) * BUCKET_MS_SIZE;
    const key = `${row.sourceElementKey}|${bucketStartMs}`;
    const isExactBoundary = row.observedAtMs === bucketStartMs;

    if (!groups.has(key)) {
      groups.set(key, {
        bucketStartMs,
        snapshot: row,
        snapshotExact: isExactBoundary,
        occupiedSum: row.occupiedSpaces,
        occupiedMin: row.occupiedSpaces,
        occupiedMax: row.occupiedSpaces,
        occupiedFirst: row.occupiedSpaces,
        occupiedLast: row.occupiedSpaces,
        firstObservedAtMs: row.observedAtMs,
        lastObservedAtMs: row.observedAtMs,
        sampleCount: 1,
      });
      continue;
    }

    const group = groups.get(key);
    group.occupiedSum += row.occupiedSpaces;
    group.occupiedMin = Math.min(group.occupiedMin, row.occupiedSpaces);
    group.occupiedMax = Math.max(group.occupiedMax, row.occupiedSpaces);
    group.sampleCount += 1;

    if (row.observedAtMs < group.firstObservedAtMs) {
      group.firstObservedAtMs = row.observedAtMs;
      group.occupiedFirst = row.occupiedSpaces;
    }
    if (row.observedAtMs > group.lastObservedAtMs) {
      group.lastObservedAtMs = row.observedAtMs;
      group.occupiedLast = row.occupiedSpaces;
    }

    if (isExactBoundary) {
      if (!group.snapshotExact || row.observedAtMs < group.snapshot.observedAtMs) {
        group.snapshot = row;
        group.snapshotExact = true;
      }
    } else if (!group.snapshotExact && row.observedAtMs < group.snapshot.observedAtMs) {
      group.snapshot = row;
    }
  }

  return [...groups.values()].map((group) => {
    const occupiedAvg = group.sampleCount ? group.occupiedSum / group.sampleCount : 0;

    return {
      ...group.snapshot,
      bucketStartMs: group.bucketStartMs,
      minuteStats: {
        occupiedAvg,
        occupiedMin: group.occupiedMin,
        occupiedMax: group.occupiedMax,
        occupiedFirst: group.occupiedFirst,
        occupiedLast: group.occupiedLast,
        occupiedDelta: group.occupiedLast - group.occupiedFirst,
        coverageMinutes: group.sampleCount,
      },
    };
  });
}

function selectedTimeIndex() {
  if (!timeValues.length) {
    return 0;
  }
  return Math.min(Math.max(Number(timeRange.value) || 0, 0), timeValues.length - 1);
}

function selectedTimeMs() {
  if (!timeValues.length) {
    return null;
  }
  return timeValues[selectedTimeIndex()];
}

function selectedTimeEntry() {
  if (!timeEntries.length) {
    return null;
  }
  return timeEntries[selectedTimeIndex()] || null;
}

function updateTimeRangeLabel() {
  const selected = selectedTimeMs();
  timeRangeValue.textContent = selected ? new Date(selected).toLocaleString() : "-";
}

function initTimeRangeFromValues(values) {
  const unique = [...new Set(values)].filter((v) => Number.isFinite(v)).sort((a, b) => a - b);
  timeValues = unique;

  if (!timeValues.length) {
    timeRange.min = "0";
    timeRange.max = "0";
    timeRange.value = "0";
    timeRange.disabled = true;
    updateTimeRangeLabel();
    return;
  }

  timeRange.min = "0";
  timeRange.max = String(timeValues.length - 1);
  timeRange.value = String(timeValues.length - 1);
  timeRange.disabled = timeValues.length <= 1;
  updateTimeRangeLabel();
}

function filteredPoints() {
  const selectedTime = selectedTimeMs();
  const minAvail = Number(minAvailability.value);
  const query = normalizeText(addressSearch.value);

  let points = useChunkMode
    ? bucketRows
    : bucketRows.filter((row) => row.bucketStartMs === selectedTime);

  points = points.filter((row) => row.availabilityPct >= minAvail);

  if (query) {
    points = points.filter((row) => {
      const haystack = normalizeText(
        `${row.blockfaceName} ${row.paidParkingArea} ${row.paidParkingSubarea}`
      );
      return haystack.includes(query);
    });
  }

  return points;
}

function updateStats(points) {
  statBlockfaces.textContent = points.length.toLocaleString();

  if (!points.length) {
    statAvgOcc.textContent = "-";
    statAvgAvail.textContent = "-";
    return;
  }

  const occAvg = points.reduce((sum, p) => sum + p.occupancyPct, 0) / points.length;
  const availAvg = points.reduce((sum, p) => sum + p.availabilityPct, 0) / points.length;

  statAvgOcc.textContent = `${occAvg.toFixed(1)}%`;
  statAvgAvail.textContent = `${availAvg.toFixed(1)}%`;
}

function render(options = {}) {
  const shouldFitMap = options.fitMap ?? false;
  updateTimeRangeLabel();
  const points = filteredPoints();
  markerLayer.clearLayers();
  updateStats(points);

  if (!points.length) {
    setStatus("No blockfaces match the current filters.");
    return;
  }

  if (map.getZoom() < DETAIL_ZOOM_MIN) {
    setStatus(
      `Zoom in to level ${DETAIL_ZOOM_MIN}+ to view payment station status. ${points.length.toLocaleString()} match current filters.`
    );
    return;
  }

  for (const point of points) {
    const marker = L.circleMarker([point.latitude, point.longitude], {
      radius: 7,
      fillColor: markerColor(point.availabilityPct),
      color: "#ffffff",
      weight: 1,
      fillOpacity: 0.92,
    });

    marker.bindPopup(`
      <div class="popup-title">${escapeHtml(point.blockfaceName)}</div>
      <div>Observed: ${formatLocalTime(point.observedAtMs)}</div>
      <div>30-min Bucket Start: ${formatLocalTime(point.bucketStartMs)}</div>
      <div>Side: ${escapeHtml(point.sideOfStreet || "Unknown")}</div>
      <div>Area: ${escapeHtml(point.paidParkingArea)}${point.paidParkingSubarea ? ` (${escapeHtml(point.paidParkingSubarea)})` : ""}</div>
      <div>Category: ${escapeHtml(point.parkingCategory)}</div>
      <div>Occupied: ${point.occupiedSpaces}</div>
      <div>Total Spaces: ${point.totalSpaces}</div>
      <div>Available Spaces: ${point.availableSpaces}</div>
      <div>Occupancy: ${point.occupancyPct.toFixed(1)}%</div>
      <div>Availability: ${point.availabilityPct.toFixed(1)}%</div>
      <div>Minute Occ Avg: ${point.minuteStats.occupiedAvg.toFixed(1)}</div>
      <div>Minute Occ Min/Max: ${point.minuteStats.occupiedMin} / ${point.minuteStats.occupiedMax}</div>
      <div>Minute Occ First/Last: ${point.minuteStats.occupiedFirst} / ${point.minuteStats.occupiedLast}</div>
      <div>Minute Occ Delta: ${point.minuteStats.occupiedDelta >= 0 ? "+" : ""}${point.minuteStats.occupiedDelta}</div>
      <div>Minute Coverage: ${point.minuteStats.coverageMinutes} samples</div>
    `);

    marker.addTo(markerLayer);
  }

  if (shouldFitMap || !hasFitOnce) {
    const bounds = L.latLngBounds(points.map((p) => [p.latitude, p.longitude]));
    map.fitBounds(bounds.pad(0.1));
    hasFitOnce = true;
  }

  setStatus(`Showing ${points.length.toLocaleString()} blockfaces.`);
}

function updateWindowStat() {
  if (!timeValues.length) {
    statWindow.textContent = "-";
    return;
  }

  const earliest = timeValues[0];
  const latest = timeValues[timeValues.length - 1];
  statWindow.textContent = `${new Date(earliest).toLocaleTimeString()} - ${new Date(latest).toLocaleTimeString()}`;
}

async function loadSelectedChunkTime(options = {}) {
  const shouldFitMap = options.fitMap ?? false;
  const selected = selectedTimeEntry();

  if (!selected || !selected.file) {
    bucketRows = [];
    render({ fitMap: false });
    return;
  }

  if (activeChunkFetch) {
    activeChunkFetch.abort();
  }
  activeChunkFetch = new AbortController();

  setStatus(`Loading ${new Date(selected.bucketStartMs).toLocaleString()}...`);

  try {
    const res = await fetch(selected.file, {
      cache: "no-store",
      signal: activeChunkFetch.signal,
    });
    if (!res.ok) {
      throw new Error(`Could not load ${selected.file} (${res.status}).`);
    }

    const rawRows = await res.json();
    if (!Array.isArray(rawRows)) {
      throw new Error("Chunk file is not a JSON array.");
    }

    bucketRows = rawRows.map(normalizeBucketedRow).filter(Boolean);
    render({ fitMap: shouldFitMap });
  } catch (err) {
    if (err.name === "AbortError") {
      return;
    }
    markerLayer.clearLayers();
    statBlockfaces.textContent = "-";
    statAvgOcc.textContent = "-";
    statAvgAvail.textContent = "-";
    setStatus(`Error: ${err.message}`);
  } finally {
    activeChunkFetch = null;
  }
}

async function loadChunkIndex() {
  return false;
}

async function loadSingleFileData() {
  const res = await fetch(DATA_FILE, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Could not load ${DATA_FILE} (${res.status}). Run a local web server.`);
  }

  const rawText = await res.text();
  const rawRows = parseRowsFromText(rawText);
  if (!Array.isArray(rawRows) || !rawRows.length) {
    throw new Error("Local file is empty or invalid.");
  }

  if (rawRows[0].minute_stats && rawRows[0].bucketstartdatetime) {
    bucketRows = rawRows.map(normalizeBucketedRow).filter(Boolean);
  } else {
    const normalizedRows = rawRows.map(normalizeRow).filter(Boolean);
    if (!normalizedRows.length) {
      throw new Error("No valid rows found after normalization.");
    }
    bucketRows = build30MinRows(normalizedRows);
  }

  if (!bucketRows.length) {
    throw new Error("No valid 30-minute rows were produced.");
  }

  timeEntries = [];
  useChunkMode = false;
  initTimeRangeFromValues(bucketRows.map((row) => row.bucketStartMs));
  updateWindowStat();
  render({ fitMap: true });
}

async function loadLocalData() {
  setStatus("Loading data...");
  refreshBtn.disabled = true;

  try {
    const loadedChunks = await loadChunkIndex();
    if (!loadedChunks) {
      await loadSingleFileData();
    }
  } catch (err) {
    markerLayer.clearLayers();
    statBlockfaces.textContent = "-";
    statAvgOcc.textContent = "-";
    statAvgAvail.textContent = "-";
    statWindow.textContent = "-";
    setStatus(`Error: ${err.message}`);
  } finally {
    refreshBtn.disabled = false;
  }
}

addressSearch.addEventListener("input", () => render({ fitMap: false }));
clearSearchBtn.addEventListener("click", () => {
  addressSearch.value = "";
  render({ fitMap: true });
});
timeRange.addEventListener("input", updateTimeRangeLabel);
timeRange.addEventListener("change", async () => {
  if (useChunkMode) {
    await loadSelectedChunkTime({ fitMap: false });
    return;
  }
  render({ fitMap: false });
});
minAvailability.addEventListener("input", () => {
  minAvailabilityValue.textContent = `${minAvailability.value}%`;
  render({ fitMap: false });
});
map.on("zoomend", () => render({ fitMap: false }));
refreshBtn.addEventListener("click", loadLocalData);

loadLocalData();
