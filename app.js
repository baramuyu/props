const DATA_FILES = [
  "/data/hiyf-7edq-latest-24h-30min-with-stats.ndjson",
];
const DEFAULT_CENTER = [47.6062, -122.3321];
const DEFAULT_ZOOM = 15;
const BUCKET_MS_SIZE = 30 * 60 * 1000;
const DEFAULT_TIME_MINUTE_OF_DAY = 8 * 60;
const DETAIL_ZOOM_MIN = 14;
const SEATTLE_TIME_ZONE = "America/Los_Angeles";
const BLOCKFACE_SCHEDULE_FILES = [
  "/data/hiyf-7edq-location-paid-time-metadata.json",
];
const CHUNK_INDEX_FILES = ["/data/hiyf-7edq-latest-24h-30min-index.json"];

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
const statusEl = document.getElementById("status");
const metaVisibleCount = document.getElementById("metaVisibleCount");
const metaDataSource = document.getElementById("metaDataSource");

let bucketRows = [];
let timeValues = [];
let timeEntries = [];
let hasFitOnce = true;
let useChunkMode = false;
let activeChunkFetch = null;
let sliderDayStartMs = null;
let blockfaceScheduleByKey = new Map();
let blockfaceScheduleSource = null;
let activeDataFile = DATA_FILES[0];

function setStatus(msg) {
  statusEl.textContent = msg;
}

function setVisibleBlockfaceCount(value) {
  if (!metaVisibleCount) {
    return;
  }
  if (Number.isFinite(value)) {
    metaVisibleCount.textContent = Number(value).toLocaleString();
    return;
  }
  metaVisibleCount.textContent = "-";
}

function setDataSourceLabel(value) {
  if (!metaDataSource) {
    return;
  }
  metaDataSource.textContent = value || "-";
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

function isExplicitNoDataRow(row) {
  const directFlags = [row.isnodata, row.is_no_data, row.nodata, row.no_data];
  if (directFlags.some((value) => toBoolean(value) === true)) {
    return true;
  }

  const paymentStatus = String(row.paymentstatus || row.payment_status || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();

  return paymentStatus === "no data" || paymentStatus === "nodata";
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
      if (hours < 24 && mins < 60) {
        return hours * 60 + mins;
      }
      if (hours === 24 && mins === 0) {
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
    if (
      !Number.isFinite(hour) ||
      !Number.isFinite(minute) ||
      hour > 24 ||
      minute > 59 ||
      (hour === 24 && minute !== 0)
    ) {
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

function formatMinuteLabel(minuteOfDay) {
  const clamped = Math.max(0, Math.min(24 * 60, Math.round(minuteOfDay)));
  const hour24 = Math.floor(clamped / 60) % 24;
  const minute = clamped % 60;
  const suffix = hour24 >= 12 ? "PM" : "AM";
  const hour12 = hour24 % 12 === 0 ? 12 : hour24 % 12;
  if (minute === 0) {
    return `${hour12}${suffix}`;
  }
  return `${hour12}:${String(minute).padStart(2, "0")}${suffix}`;
}

function formatChargingPeriod(windows) {
  if (!Array.isArray(windows) || !windows.length) {
    return "None (Free all day)";
  }

  return windows
    .map((window) => {
      if (
        !window ||
        !Number.isFinite(window.startMinute) ||
        !Number.isFinite(window.endMinute)
      ) {
        return null;
      }
      if (window.startMinute === window.endMinute) {
        return "24 hours";
      }
      return `${formatMinuteLabel(window.startMinute)} to ${formatMinuteLabel(window.endMinute)}`;
    })
    .filter(Boolean)
    .join(", ");
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

  const paidTimeStartMinute = toNumber(
    get("paid_time_start_minute", "paidtimestartminute")
  );
  const paidTimeEndMinute = toNumber(get("paid_time_end_minute", "paidtimeendminute"));
  const is24hPaid = toBoolean(get("is_24h", "is24h", "is_24_hour", "is24hour"));

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

    if (!windows.length && paidTimeStartMinute !== null && paidTimeEndMinute !== null) {
      const startMinute = Math.max(0, Math.min(24 * 60, Math.round(paidTimeStartMinute)));
      const endMinute = Math.max(0, Math.min(24 * 60, Math.round(paidTimeEndMinute)));

      // Equal minute bounds should only be treated as 24h paid when explicitly flagged.
      if (startMinute !== endMinute || is24hPaid === true) {
        windows.push({ startMinute, endMinute });
      }
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
    if (parsed && parsed.locations && typeof parsed.locations === "object") {
      return Object.values(parsed.locations);
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
      chargingPeriod: "Unknown",
    };
  }

  const time = seattleTimeParts(observedAtMs);
  const record = selectScheduleRecordForDate(records, time.yyyymmdd);
  if (!record) {
    return {
      known: false,
      isFree: false,
      reason: "No valid schedule record",
      chargingPeriod: "Unknown",
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
      chargingPeriod: formatChargingPeriod(windows),
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
    chargingPeriod: formatChargingPeriod(windows),
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

function readField(row, keys) {
  for (const key of keys) {
    if (!Object.prototype.hasOwnProperty.call(row, key)) {
      continue;
    }
    const value = row[key];
    if (value === null || value === undefined) {
      continue;
    }
    if (typeof value === "string" && !value.trim()) {
      continue;
    }
    return value;
  }
  return null;
}

function normalizeRow(row) {
  const paidOccupancyRaw = readField(row, ["paidoccupancy", "occupied_spaces", "occupiedspaces"]);
  const parkingSpaceCountRaw = readField(row, [
    "parkingspacecount",
    "total_spaces",
    "totalspaces",
    "spot_count",
  ]);
  const occupied = toNumber(paidOccupancyRaw);
  const total = toNumber(parkingSpaceCountRaw);
  const observedAtRaw = readField(row, ["occupancydatetime", "observed_at", "bucketstartdatetime"]);
  const bucketStartRaw = readField(row, ["bucketstartdatetime", "bucket_start_datetime"]);
  const ts = parseTimestamp(observedAtRaw);
  const coords = row?.location?.coordinates;
  const lon = toNumber(coords?.[0] ?? readField(row, ["longitude", "lon"]));
  const lat = toNumber(coords?.[1] ?? readField(row, ["latitude", "lat"]));

  if (
    occupied === null ||
    total === null ||
    total <= 0 ||
    !ts ||
    lon === null ||
    lat === null ||
    lat < -90 ||
    lat > 90 ||
    lon < -180 ||
    lon > 180
  ) {
    return null;
  }

  const clampedOccupied = Math.max(0, Math.min(occupied, total));
  const available = Math.max(total - clampedOccupied, 0);
  const occupancyPct = (clampedOccupied / total) * 100;
  const availabilityPct = 100 - occupancyPct;
  const sourceElementKey =
    readField(row, ["sourceelementkey", "source_element_key", "element_key"]) ||
    `${lat.toFixed(5)},${lon.toFixed(5)}`;
  const parkingCategory = readField(row, ["parkingcategory", "parking_category"]) || "Unknown";
  const scheduleStatus = evaluateScheduleFreeStatus(sourceElementKey, ts);
  const explicitFree = isExplicitFreeRow(row);
  const explicitNoData = isExplicitNoDataRow(row);

  return {
    sourceElementKey,
    blockfaceName: readField(row, ["blockfacename", "blockface_name"]) || "Unknown blockface",
    sideOfStreet: readField(row, ["sideofstreet", "side_of_street"]) || "",
    paidParkingArea:
      readField(row, ["paidparkingarea", "paid_parking_area", "area_name"]) || "Unknown",
    paidParkingSubarea:
      readField(row, ["paidparkingsubarea", "paid_parking_subarea", "subarea_name"]) || "",
    parkingCategory,
    isFree: explicitFree || scheduleStatus.isFree,
    freeReason: explicitFree
      ? "Marked free in source data"
      : scheduleStatus.reason,
    chargingPeriod: scheduleStatus.chargingPeriod || "Unknown",
    isExplicitNoData: explicitNoData,
    scheduleKnown: scheduleStatus.known,
    observedAtMs: ts,
    observedAtRaw,
    bucketStartRaw,
    parkingTimeLimitCategory:
      readField(row, ["parkingtimelimitcategory", "parking_time_limit_category"]) || "",
    paidOccupancyRaw,
    parkingSpaceCountRaw,
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

function parseRowsFromText(rawText, filePath) {
  if (filePath.endsWith(".ndjson")) {
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
  const defaultIdx = Math.min(
    47,
    Math.max(0, Math.floor(DEFAULT_TIME_MINUTE_OF_DAY / (BUCKET_MS_SIZE / (60 * 1000))))
  );
  timeRange.value = String(defaultIdx);
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
  setVisibleBlockfaceCount(points.length);

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
    const marker = L.circleMarker([point.latitude, point.longitude], {
      radius: 7,
      fillColor: markerColorForPoint(point),
      color: "#ffffff",
      weight: 1,
      fillOpacity: 0.92,
    });

    const bucketStartLabel = point.bucketStartRaw || formatLocalTime(point.bucketStartMs);

    marker.bindPopup(`
      <div class="popup-title">${escapeHtml(point.blockfaceName)}</div>
      <div>Source Element Key: ${escapeHtml(point.sourceElementKey || "Unknown")}</div>
      <div>Occupancy Datetime: ${escapeHtml(point.observedAtRaw || "Unknown")}</div>
      <div>Bucket Start Datetime: ${escapeHtml(bucketStartLabel || "Unknown")}</div>
      <div>Side of Street: ${escapeHtml(point.sideOfStreet || "Unknown")}</div>
      <div>Paid Parking Area: ${escapeHtml(point.paidParkingArea || "Unknown")}</div>
      <div>Paid Parking Subarea: ${escapeHtml(point.paidParkingSubarea || "Unknown")}</div>
      <div>Parking Category: ${escapeHtml(point.parkingCategory || "Unknown")}</div>
      <div>Parking Time Limit Category: ${escapeHtml(point.parkingTimeLimitCategory || "Unknown")}</div>
      <div>Paid Occupancy: ${escapeHtml(point.paidOccupancyRaw || "Unknown")}</div>
      <div>Parking Space Count: ${escapeHtml(point.parkingSpaceCountRaw || "Unknown")}</div>
      <div>Coordinates: [${point.longitude.toFixed(8)}, ${point.latitude.toFixed(8)}]</div>
    `);

    marker.addTo(markerLayer);
  }

  const scheduleNote = blockfaceScheduleSource
    ? ` Paid-time rules loaded from ${blockfaceScheduleSource}.`
    : " Paid-time rules unavailable (using transaction/availability only).";
  setStatus(`Showing ${points.length.toLocaleString()} blockfaces.${scheduleNote}`);
}

async function loadSelectedChunkTime(options = {}) {
  const shouldFitMap = options.fitMap ?? false;
  const selected = selectedTimeEntry();

  if (!selected || !selected.file) {
    bucketRows = [];
    setDataSourceLabel("-");
    render({ fitMap: false });
    return;
  }

  if (activeChunkFetch) {
    activeChunkFetch.abort();
  }
  activeChunkFetch = new AbortController();
  activeDataFile = selected.file;
  setDataSourceLabel(activeDataFile);

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
    setVisibleBlockfaceCount(null);
    setStatus(`Error: ${err.message}`);
  } finally {
    activeChunkFetch = null;
  }
}

async function loadChunkIndex() {
  for (const filePath of CHUNK_INDEX_FILES) {
    try {
      const res = await fetch(filePath, { cache: "no-store" });
      if (!res.ok) {
        continue;
      }

      const indexJson = await res.json();
      const rawEntries = Array.isArray(indexJson)
        ? indexJson
        : indexJson?.entries || indexJson?.buckets || indexJson?.times || [];
      if (!Array.isArray(rawEntries) || !rawEntries.length) {
        continue;
      }

      const normalizedEntries = rawEntries
        .map((entry) => {
          const bucketRaw =
            entry?.bucketStartMs ??
            entry?.bucket_start_ms ??
            entry?.bucketstartms ??
            entry?.bucketStart ??
            entry?.bucket_start ??
            entry?.bucketstartdatetime ??
            entry?.time ??
            entry?.timestamp;
          const bucketStartMs =
            typeof bucketRaw === "number" ? bucketRaw : parseTimestamp(bucketRaw);

          const file =
            entry?.file || entry?.path || entry?.url || entry?.chunkFile || entry?.chunk_file;
          if (!Number.isFinite(bucketStartMs) || !file) {
            return null;
          }

          return {
            bucketStartMs,
            file: String(file),
          };
        })
        .filter(Boolean)
        .sort((a, b) => a.bucketStartMs - b.bucketStartMs);

      if (!normalizedEntries.length) {
        continue;
      }

      const dayStartRaw =
        indexJson?.sliderDayStartMs || indexJson?.slider_day_start_ms || indexJson?.dayStartMs;
      const parsedDayStart =
        typeof dayStartRaw === "number" ? dayStartRaw : parseTimestamp(dayStartRaw);
      sliderDayStartMs = Number.isFinite(parsedDayStart) ? parsedDayStart : null;

      timeEntries = normalizedEntries;
      useChunkMode = true;
      activeDataFile = filePath;

      initTimeRangeFromValues(normalizedEntries.map((entry) => entry.bucketStartMs));
      await loadSelectedChunkTime({ fitMap: false });
      return true;
    } catch (_err) {
      // Try next index file.
    }
  }

  timeEntries = [];
  useChunkMode = false;
  return false;
}

async function loadSingleFileData() {
  let rawRows = null;

  for (const filePath of DATA_FILES) {
    try {
      const res = await fetch(filePath, { cache: "no-store" });
      if (!res.ok) {
        continue;
      }

      const rawText = await res.text();
      const parsedRows = parseRowsFromText(rawText, filePath);
      if (!Array.isArray(parsedRows) || !parsedRows.length) {
        continue;
      }

      rawRows = parsedRows;
      activeDataFile = filePath;
      break;
    } catch (_err) {
      // Try the next candidate file.
    }
  }

  if (!rawRows) {
    throw new Error(
      `Could not load a local dataset. Tried: ${DATA_FILES.join(", ")}. Run a local web server.`
    );
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

  setDataSourceLabel(activeDataFile);

  timeEntries = [];
  useChunkMode = false;
  initTimeRangeFromValues(bucketRows.map((row) => row.bucketStartMs));
  render({ fitMap: false });
}

async function loadLocalData() {
  setStatus("Loading data...");
  setVisibleBlockfaceCount(null);
  setDataSourceLabel(activeDataFile);

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
    setVisibleBlockfaceCount(null);
    setStatus(`Error: ${err.message}`);
  }
}

addressSearch.addEventListener("input", () => render({ fitMap: false }));
addressSearch.addEventListener("keydown", (event) => {
  if (event.key !== "Enter") {
    return;
  }
  event.preventDefault();
  render({ fitMap: true });
});
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

loadLocalData();
