const DATA_FILE = "data/hiyf-7edq-latest-60-min.json";
const DEFAULT_CENTER = [47.6062, -122.3321];
const DEFAULT_ZOOM = 13;

const map = L.map("map").setView(DEFAULT_CENTER, DEFAULT_ZOOM);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
}).addTo(map);

const markerLayer = L.layerGroup().addTo(map);

const addressSearch = document.getElementById("addressSearch");
const clearSearchBtn = document.getElementById("clearSearchBtn");
const timeSelect = document.getElementById("timeSelect");
const minAvailability = document.getElementById("minAvailability");
const minAvailabilityValue = document.getElementById("minAvailabilityValue");
const refreshBtn = document.getElementById("refreshBtn");
const statusEl = document.getElementById("status");

const statBlockfaces = document.getElementById("statBlockfaces");
const statAvgOcc = document.getElementById("statAvgOcc");
const statAvgAvail = document.getElementById("statAvgAvail");
const statWindow = document.getElementById("statWindow");

let allRows = [];
let timeValues = [];
let hasFitOnce = false;

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

function buildTimeOptions(rows) {
  const unique = [...new Set(rows.map((r) => r.observedAtRaw))]
    .filter(Boolean)
    .sort((a, b) => parseTimestamp(b) - parseTimestamp(a));

  timeValues = unique;

  timeSelect.innerHTML = '<option value="latest">Latest per blockface</option>';
  for (const timeValue of unique) {
    const option = document.createElement("option");
    option.value = timeValue;
    option.textContent = new Date(timeValue).toLocaleString();
    timeSelect.appendChild(option);
  }
}

function aggregateByLatest(rows) {
  const byBlockface = new Map();

  for (const row of rows) {
    const key = row.sourceElementKey;
    const existing = byBlockface.get(key);
    if (!existing || row.observedAtMs > existing.observedAtMs) {
      byBlockface.set(key, row);
    }
  }

  return [...byBlockface.values()];
}

function filteredPoints() {
  const selectedTime = timeSelect.value;
  const minAvail = Number(minAvailability.value);
  const query = normalizeText(addressSearch.value);

  let points = [];
  if (selectedTime === "latest") {
    points = aggregateByLatest(allRows);
  } else {
    points = allRows.filter((row) => row.observedAtRaw === selectedTime);
  }

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
  const points = filteredPoints();
  markerLayer.clearLayers();

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
      <div>Side: ${escapeHtml(point.sideOfStreet || "Unknown")}</div>
      <div>Area: ${escapeHtml(point.paidParkingArea)}${point.paidParkingSubarea ? ` (${escapeHtml(point.paidParkingSubarea)})` : ""}</div>
      <div>Category: ${escapeHtml(point.parkingCategory)}</div>
      <div>Occupied: ${point.occupiedSpaces}</div>
      <div>Total Spaces: ${point.totalSpaces}</div>
      <div>Available Spaces: ${point.availableSpaces}</div>
      <div>Occupancy: ${point.occupancyPct.toFixed(1)}%</div>
      <div>Availability: ${point.availabilityPct.toFixed(1)}%</div>
    `);

    marker.addTo(markerLayer);
  }

  updateStats(points);

  if (!points.length) {
    setStatus("No blockfaces match the current filters.");
    return;
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

  const latest = parseTimestamp(timeValues[0]);
  const earliest = parseTimestamp(timeValues[timeValues.length - 1]);
  statWindow.textContent = `${new Date(earliest).toLocaleTimeString()} - ${new Date(latest).toLocaleTimeString()}`;
}

async function loadLocalData() {
  setStatus("Loading local data file...");
  refreshBtn.disabled = true;

  try {
    const res = await fetch(DATA_FILE, { cache: "no-store" });
    if (!res.ok) {
      throw new Error(`Could not load ${DATA_FILE} (${res.status}). Run a local web server.`);
    }

    const rawRows = await res.json();
    if (!Array.isArray(rawRows) || !rawRows.length) {
      throw new Error("Local file is empty or not a JSON array.");
    }

    allRows = rawRows.map(normalizeRow).filter(Boolean);
    if (!allRows.length) {
      throw new Error("No valid rows found after normalization.");
    }

    buildTimeOptions(allRows);
    updateWindowStat();
    render({ fitMap: true });
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

addressSearch.addEventListener("input", render);
clearSearchBtn.addEventListener("click", () => {
  addressSearch.value = "";
  render({ fitMap: true });
});
timeSelect.addEventListener("change", () => render({ fitMap: true }));
minAvailability.addEventListener("input", () => {
  minAvailabilityValue.textContent = `${minAvailability.value}%`;
  render({ fitMap: false });
});
refreshBtn.addEventListener("click", loadLocalData);

loadLocalData();
