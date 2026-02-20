const DATA_FILE = "/data/hiyf-7edq-latest-24h-30min-with-stats.ndjson";
const DEFAULT_CENTER = [47.6062, -122.3321];
const DEFAULT_ZOOM = 13;
const BUCKET_MS_SIZE = 30 * 60 * 1000;
const DETAIL_ZOOM_MIN = 14;
const SEATTLE_TIME_ZONE = "America/Los_Angeles";
const BLOCKFACE_SCHEDULE_FILES = [
  "/data/hiyf-7edq-location-paid-time-metadata.json",
];

const map = L.map("map").setView(DEFAULT_CENTER, DEFAULT_ZOOM);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
}).addTo(map);

const markerLayer = L.layerGroup().addTo(map);

const addressSearch = document.getElementById("addressSearch");
const clearSearchBtn = document.getElementById("clearSearchBtn");
const timeRange = document.getElementById("timeRange");
const timeRangeValue = document.getElementById("timeRangeValue");
const paidAreaSelect = document.getElementById("paidAreaSelect");
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
let sliderDayStartMs = null;
let blockfaceScheduleByKey = new Map();
let blockfaceScheduleSource = null;

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

const freeIcon = L.divIcon({
  className: "free-marker-wrapper",
  html: '<span class="free-marker-badge">Free</span>',
  iconSize: [38, 18],
  iconAnchor: [19, 9],
  popupAnchor: [0, -8],
});

function hasActivePaymentTransaction(point) {
  return point.occupiedSpaces > 0;
}

function isNoDataPoint(point) {
  if (!point) {
    return true;
  }
  if (!Number.isFinite(point.occupiedSpaces) || !Number.isFinite(point.totalSpaces)) {
    return true;
  }
  if (point.minuteStats && Number.isFinite(point.minuteStats.coverageMinutes)) {
    return point.minuteStats.coverageMinutes <= 0;
  }
  return false;
}

function toBoolean(value) {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "1", "yes", "y"].includes(normalized)) {
      return true;
    }
    if (["false", "0", "no", "n"].includes(normalized)) {
      return false;
    }
  }
  return null;
}

function isExplicitFreeRow(row) {
  const directFlags = [
    row.isfree,
    row.is_free,
    row.free,
    row.freeparking,
    row.isfreetime,
    row.is_free_time,
  ];
  if (directFlags.some((value) => toBoolean(value) === true)) {
    return true;
  }

  const category = String(row.parkingcategory || "").toLowerCase();
  const paymentStatus = String(row.paymentstatus || row.payment_status || "").toLowerCase();
  return category.includes("free") || paymentStatus === "free";
}

function markerColorForPoint(point) {
  return markerColor(point.availabilityPct);
}

function normalizeKeyName(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function parseCsvText(text) {
  const rows = [];
  let current = "";
  let row = [];
  let inQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    const next = text[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      row.push(current);
      current = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && next === "\n") {
        i += 1;
      }
      row.push(current);
      current = "";
      if (row.some((cell) => cell.trim() !== "")) {
        rows.push(row);
      }
      row = [];
      continue;
    }

    current += char;
  }

  if (current.length || row.length) {
    row.push(current);
    if (row.some((cell) => cell.trim() !== "")) {
      rows.push(row);
    }
  }

  if (!rows.length) {
    return [];
  }

  const headers = rows[0].map((h) => h.trim());
  return rows.slice(1).map((cells) => {
    const obj = {};
    for (let i = 0; i < headers.length; i += 1) {
      obj[headers[i]] = cells[i] ?? "";
    }
    return obj;
  });
}

function parseServiceDate(value) {
  if (value === null || value === undefined) {
    return null;
  }

  const raw = String(value).trim();
  if (!raw) {
    return null;
  }

  const isoMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) {
    return Number(`${isoMatch[1]}${isoMatch[2]}${isoMatch[3]}`);
  }

  const abbrMatch = raw.match(/^(\d{1,2})-([A-Za-z]{3})-(\d{2}|\d{4})$/);
  if (!abbrMatch) {
    return null;
  }

  const monthMap = {
    jan: 1,
    feb: 2,
    mar: 3,
    apr: 4,
    may: 5,
    jun: 6,
    jul: 7,
    aug: 8,
    sep: 9,
    oct: 10,
    nov: 11,
    dec: 12,
  };

  const day = Number(abbrMatch[1]);
  const month = monthMap[abbrMatch[2].toLowerCase()];
  let year = Number(abbrMatch[3]);
  if (!month || !Number.isFinite(day) || !Number.isFinite(year)) {
    return null;
  }
  if (year < 100) {
    year += year >= 70 ? 1900 : 2000;
  }

  return year * 10000 + month * 100 + day;
}

function parseTimeToMinutes(value) {
  if (value === null || value === undefined) {
    return null;
  }

  const numeric = toNumber(value);
  if (numeric !== null) {
    const intVal = Math.round(numeric);
    if (intVal >= 0 && intVal <= 24 * 60) {
      return intVal;
    }
    if (intVal >= 0 && intVal <= 2359) {
      const hours = Math.floor(intVal / 100);
      const mins = intVal % 100;
      if (hours <= 24 && mins < 60) {
        return hours * 60 + mins;
      }
    }
  }

  const raw = String(value).trim().toUpperCase();
  if (!raw) {
    return null;
  }

  const ampmMatch = raw.match(/^(\d{1,2})(?::(\d{2}))?\s*([AP]M)$/);
  if (ampmMatch) {
    let hour = Number(ampmMatch[1]) % 12;
    const minute = Number(ampmMatch[2] || 0);
    if (!Number.isFinite(hour) || !Number.isFinite(minute) || minute >= 60) {
      return null;
    }
    if (ampmMatch[3] === "PM") {
      hour += 12;
    }
    return hour * 60 + minute;
  }

  const hmsMatch = raw.match(/^(\d{1,2}):(\d{2})(?::\d{2})?$/);
  if (hmsMatch) {
    const hour = Number(hmsMatch[1]);
    const minute = Number(hmsMatch[2]);
    if (!Number.isFinite(hour) || !Number.isFinite(minute) || hour > 24 || minute >= 60) {
      return null;
    }
    return hour * 60 + minute;
  }

  return null;
}

function seattleTimeParts(ms) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: SEATTLE_TIME_ZONE,
    weekday: "short",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(new Date(ms));

  const lookup = {};
  for (const part of parts) {
    if (part.type !== "literal") {
      lookup[part.type] = part.value;
    }
  }

  const year = Number(lookup.year);
  const month = Number(lookup.month);
  const day = Number(lookup.day);
  const hour = Number(lookup.hour);
  const minute = Number(lookup.minute);

  return {
    weekday: lookup.weekday || "",
    minuteOfDay: (Number.isFinite(hour) ? hour : 0) * 60 + (Number.isFinite(minute) ? minute : 0),
    yyyymmdd:
      (Number.isFinite(year) ? year : 0) * 10000 +
      (Number.isFinite(month) ? month : 0) * 100 +
      (Number.isFinite(day) ? day : 0),
  };
}

function minuteInWindow(minuteOfDay, startMinute, endMinute) {
  if (
    !Number.isFinite(minuteOfDay) ||
    !Number.isFinite(startMinute) ||
    !Number.isFinite(endMinute)
  ) {
    return false;
  }
  if (startMinute === endMinute) {
    return true;
  }
  if (startMinute < endMinute) {
    return minuteOfDay >= startMinute && minuteOfDay < endMinute;
  }
  return minuteOfDay >= startMinute || minuteOfDay < endMinute;
}

function normalizeScheduleRow(rawRow) {
  const normalized = {};
  for (const [key, value] of Object.entries(rawRow || {})) {
    normalized[normalizeKeyName(key)] = value;
  }

  const get = (...names) => {
    for (const name of names) {
      const value = normalized[normalizeKeyName(name)];
      if (value !== null && value !== undefined && String(value).trim() !== "") {
        return value;
      }
    }
    return null;
  };

  const sourceElementKey = String(
    get("sourceelementkey", "source_element_key", "elementkey", "element_key") || ""
  ).trim();
  if (!sourceElementKey) {
    return null;
  }

  const dayWindows = (dayName) => {
    const day = dayName.toLowerCase();
    const windows = [];
    const directStart = parseTimeToMinutes(get(`starttime${day}`));
    const directEnd = parseTimeToMinutes(get(`endtime${day}`));
    if (directStart !== null && directEnd !== null) {
      windows.push({ startMinute: directStart, endMinute: directEnd });
    }

    for (let i = 1; i <= 4; i += 1) {
      const startMinute = parseTimeToMinutes(
        get(`${day}start${i}`, `${day}starttime${i}`)
      );
      const endMinute = parseTimeToMinutes(get(`${day}end${i}`, `${day}endtime${i}`));
      if (startMinute !== null && endMinute !== null) {
        windows.push({ startMinute, endMinute });
      }
    }

    const genericStart = parseTimeToMinutes(get(`${day}start`));
    const genericEnd = parseTimeToMinutes(get(`${day}end`));
    if (genericStart !== null && genericEnd !== null) {
      windows.push({ startMinute: genericStart, endMinute: genericEnd });
    }

    return windows;
  };

  return {
    sourceElementKey,
    effectiveStartDate: parseServiceDate(
      get("effectivestartdate", "startdate", "firsteffectivedate")
    ),
    effectiveEndDate: parseServiceDate(
      get("effectiveenddate", "enddate", "lasteffectivedate")
    ),
    weekdayWindows: dayWindows("weekday"),
    saturdayWindows: dayWindows("saturday"),
    sundayWindows: dayWindows("sunday"),
  };
}

function buildBlockfaceScheduleIndex(rawRows) {
  const index = new Map();

  for (const rawRow of rawRows) {
    const row = normalizeScheduleRow(rawRow);
    if (!row) {
      continue;
    }

    const key = String(row.sourceElementKey).trim();
    if (!index.has(key)) {
      index.set(key, []);
    }
    index.get(key).push(row);
  }

  for (const records of index.values()) {
    records.sort((a, b) => {
      const aStart = a.effectiveStartDate ?? -Infinity;
      const bStart = b.effectiveStartDate ?? -Infinity;
      return bStart - aStart;
    });
  }

  return index;
}

function parseScheduleRows(rawText, filePath) {
  if (filePath.endsWith(".json")) {
    const parsed = JSON.parse(rawText);
    if (Array.isArray(parsed)) {
      return parsed;
    }
    if (Array.isArray(parsed.rows)) {
      return parsed.rows;
    }
    if (Array.isArray(parsed.data)) {
      return parsed.data;
    }
    return [];
  }
  return parseCsvText(rawText);
}

async function loadBlockfaceScheduleData() {
  blockfaceScheduleByKey = new Map();
  blockfaceScheduleSource = null;

  for (const filePath of BLOCKFACE_SCHEDULE_FILES) {
    try {
      const res = await fetch(filePath, { cache: "no-store" });
      if (!res.ok) {
        continue;
      }
      const rawText = await res.text();
      if (!rawText.trim()) {
        continue;
      }

      const rawRows = parseScheduleRows(rawText, filePath);
      const scheduleIndex = buildBlockfaceScheduleIndex(rawRows);
      if (!scheduleIndex.size) {
        continue;
      }

      blockfaceScheduleByKey = scheduleIndex;
      blockfaceScheduleSource = filePath;
      return;
    } catch (_err) {
      // Keep trying fallback files.
    }
  }
}

function selectScheduleRecordForDate(records, yyyymmdd) {
  if (!records.length) {
    return null;
  }

  const matched = records.find((record) => {
    const start = record.effectiveStartDate;
    const end = record.effectiveEndDate;
    if (start !== null && yyyymmdd < start) {
      return false;
    }
    if (end !== null && yyyymmdd > end) {
      return false;
    }
    return true;
  });

  return matched || records[0];
}

function evaluateScheduleFreeStatus(sourceElementKey, observedAtMs) {
  const key = String(sourceElementKey || "").trim();
  const records = key ? blockfaceScheduleByKey.get(key) : null;
  if (!records || !records.length) {
    return {
      known: false,
      isFree: false,
      reason: "Schedule unavailable for this blockface",
    };
  }

  const time = seattleTimeParts(observedAtMs);
  const record = selectScheduleRecordForDate(records, time.yyyymmdd);
  if (!record) {
    return {
      known: false,
      isFree: false,
      reason: "No valid schedule record",
    };
  }

  let windows = record.weekdayWindows;
  if (time.weekday === "Sat") {
    windows = record.saturdayWindows;
  } else if (time.weekday === "Sun") {
    windows = record.sundayWindows;
  }

  if (!windows.length) {
    return {
      known: true,
      isFree: true,
      reason: `Outside paid schedule (${time.weekday})`,
    };
  }

  const inPaidWindow = windows.some((window) =>
    minuteInWindow(time.minuteOfDay, window.startMinute, window.endMinute)
  );

  return {
    known: true,
    isFree: !inPaidWindow,
    reason: inPaidWindow
      ? `Inside paid schedule (${time.weekday})`
      : `Outside paid schedule (${time.weekday})`,
  };
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
  const sourceElementKey = row.sourceelementkey || `${lat.toFixed(5)},${lon.toFixed(5)}`;
  const scheduleStatus = evaluateScheduleFreeStatus(sourceElementKey, ts);
  const explicitFree = isExplicitFreeRow(row);

  return {
    sourceElementKey,
    blockfaceName: row.blockfacename || "Unknown blockface",
    sideOfStreet: row.sideofstreet || "",
    paidParkingArea: row.paidparkingarea || "Unknown",
    paidParkingSubarea: row.paidparkingsubarea || "",
    parkingCategory: row.parkingcategory || "Unknown",
    isFree: explicitFree || scheduleStatus.isFree,
    freeReason: explicitFree ? "Marked free in source data" : scheduleStatus.reason,
    scheduleKnown: scheduleStatus.known,
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

  const bucketStartMs = Math.floor(base.observedAtMs / BUCKET_MS_SIZE) * BUCKET_MS_SIZE;

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
  if (!selected) {
    timeRangeValue.textContent = "-";
    return;
  }
  timeRangeValue.textContent = new Date(selected).toLocaleTimeString();
}

function initializePaidAreaFilter(rows) {
  const previousValue = paidAreaSelect.value || "all";
  const uniqueAreas = [...new Set(rows.map((row) => String(row.paidParkingArea || "").trim()))]
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));

  paidAreaSelect.innerHTML = '<option value="all">All Areas</option>';
  for (const area of uniqueAreas) {
    const option = document.createElement("option");
    option.value = area;
    option.textContent = area;
    paidAreaSelect.appendChild(option);
  }

  if (previousValue !== "all" && uniqueAreas.includes(previousValue)) {
    paidAreaSelect.value = previousValue;
  } else {
    paidAreaSelect.value = "all";
  }
  paidAreaSelect.disabled = uniqueAreas.length <= 1;
}

function initTimeRangeFromValues(values) {
  const unique = [...new Set(values)].filter((v) => Number.isFinite(v)).sort((a, b) => a - b);
  if (!unique.length) {
    timeValues = [];
    sliderDayStartMs = null;
    timeRange.min = "0";
    timeRange.max = "0";
    timeRange.value = "0";
    timeRange.disabled = true;
    updateTimeRangeLabel();
    return;
  }

  // Force a full Seattle-day slider: 12:00:00 AM to 11:59:59 PM (30-min slots).
  const latest = unique[unique.length - 1];
  const latestSeattle = seattleTimeParts(latest);
  sliderDayStartMs = latest - latestSeattle.minuteOfDay * 60 * 1000;
  timeValues = Array.from({ length: 48 }, (_, i) => sliderDayStartMs + i * BUCKET_MS_SIZE);

  timeRange.min = "0";
  timeRange.max = "47";
  // Default to latest available bucket within the day.
  const latestIdx = Math.min(
    47,
    Math.max(0, Math.floor((latest - sliderDayStartMs) / BUCKET_MS_SIZE))
  );
  timeRange.value = String(latestIdx);
  timeRange.disabled = timeValues.length <= 1;
  updateTimeRangeLabel();
}

function filteredPoints() {
  const selectedTime = selectedTimeMs();
  const selectedArea = paidAreaSelect.value;
  const query = normalizeText(addressSearch.value);

  let points = useChunkMode
    ? bucketRows
    : bucketRows.filter((row) => row.bucketStartMs === selectedTime);

  if (selectedArea && selectedArea !== "all") {
    points = points.filter((row) => String(row.paidParkingArea || "").trim() === selectedArea);
  }

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

function zoomToArea(areaValue) {
  const targetArea = String(areaValue || "").trim();
  if (!targetArea || targetArea === "all") {
    return false;
  }

  const areaPoints = bucketRows.filter(
    (row) => String(row.paidParkingArea || "").trim() === targetArea
  );
  if (!areaPoints.length) {
    return false;
  }

  const bounds = L.latLngBounds(areaPoints.map((p) => [p.latitude, p.longitude]));
  map.fitBounds(bounds.pad(0.1));
  hasFitOnce = true;
  return true;
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

  if (shouldFitMap || !hasFitOnce) {
    const bounds = L.latLngBounds(points.map((p) => [p.latitude, p.longitude]));
    map.fitBounds(bounds.pad(0.1));
    hasFitOnce = true;
  }

  if (map.getZoom() < DETAIL_ZOOM_MIN) {
    setStatus(
      `Zoom in to level ${DETAIL_ZOOM_MIN}+ to view payment station status. ${points.length.toLocaleString()} match current filters.`
    );
    return;
  }

  for (const point of points) {
    const noData = isNoDataPoint(point);
    const paymentStatus = noData
      ? "NO DATA"
      : point.isFree
        ? "Free (outside paid time)"
        : hasActivePaymentTransaction(point)
          ? "PAID (active payment transaction)"
          : "NO PAID TRANSACTION";

    const marker = noData
      ? L.circleMarker([point.latitude, point.longitude], {
          radius: 7,
          fillColor: "#ffffff",
          color: "#5f6a6a",
          weight: 1,
          fillOpacity: 1,
        })
      : point.isFree
        ? L.marker([point.latitude, point.longitude], { icon: freeIcon })
        : L.circleMarker([point.latitude, point.longitude], {
            radius: 7,
            fillColor: markerColorForPoint(point),
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
      <div>Payment Status: ${paymentStatus}</div>
      <div>Paid-Time Rule: ${escapeHtml(point.freeReason || "Unknown")}</div>
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

  const scheduleNote = blockfaceScheduleSource
    ? ` Paid-time rules loaded from ${blockfaceScheduleSource}.`
    : " Paid-time rules unavailable (using transaction/availability only).";
  setStatus(`Showing ${points.length.toLocaleString()} blockfaces.${scheduleNote}`);
}

function updateWindowStat() {
  if (!timeValues.length || sliderDayStartMs === null) {
    statWindow.textContent = "-";
    return;
  }

  const dayEnd = sliderDayStartMs + (24 * 60 * 60 * 1000) - 1000;
  statWindow.textContent = `${new Date(sliderDayStartMs).toLocaleTimeString()} - ${new Date(dayEnd).toLocaleTimeString()}`;
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
    await loadBlockfaceScheduleData();
    const loadedChunks = await loadChunkIndex();
    if (!loadedChunks) {
      await loadSingleFileData();
    }
    initializePaidAreaFilter(bucketRows);
    render({ fitMap: false });
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
paidAreaSelect.addEventListener("change", () => {
  const selectedArea = paidAreaSelect.value;
  const didZoom = zoomToArea(selectedArea);
  render({ fitMap: !didZoom });
});
map.on("zoomend", () => render({ fitMap: false }));
refreshBtn.addEventListener("click", loadLocalData);

loadLocalData();
