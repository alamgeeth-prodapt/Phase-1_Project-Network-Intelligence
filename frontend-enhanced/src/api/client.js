import axios from 'axios';

const BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api';

export const client = axios.create({
  baseURL: BASE_URL,
});

// Attach the JWT to every request once the user is logged in.
client.interceptors.request.use((config) => {
  const token = localStorage.getItem('gnc_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// A 401 anywhere means the token is gone or expired — clear it and let the
// app-level auth context react by redirecting to /login.
client.interceptors.response.use(
  (res) => res,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('gnc_token');
      window.dispatchEvent(new Event('gnc-auth-expired'));
    }
    return Promise.reject(error);
  }
);

/* ---- Auth ---- */

// POST /login expects OAuth2 form data, not JSON.
export async function login(username, password) {
  const form = new URLSearchParams();
  form.append('username', username);
  form.append('password', password);
  const { data } = await client.post('/login', form, {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  });
  return data; // { access_token, token_type }
}

/* ---- Network analytics ---- */

export async function getNetworkSummary() {
  const { data } = await client.get('/network/summary');
  return data;
}

// All grid centroids/polygons for the map. If your backend doesn't have this
// bulk route yet, add it — plotting 10k grids one request at a time isn't viable.
export async function getGrids() {
  const { data } = await client.get('/network/grids');
  return data;
}

// Per-day activity for the heat map. date must be 'YYYY-MM-DD'.
export async function getGridsActivityForDate(date) {
  const { data } = await client.get('/network/grids', { params: { date } });
  return data;
}

export async function getGridTimeseries(gridId, { asOf } = {}) {
  const { data } = await client.get(`/network/grid/${gridId}`, {
    params: asOf ? { as_of: asOf } : {},
  });
  return data;
}

export async function getGridFeatures(gridId, { asOf } = {}) {
  const { data } = await client.get(`/network/grid/${gridId}/features`, {
    params: asOf ? { as_of: asOf } : {},
  });
  return data;
}

// Despite the URL (a pre-existing backend route, left as-is here), this
// predicts an activity drop specifically — the response's own
// `drop_probability` field confirms that's what the model is scoring, so
// the frontend calls it that everywhere it's user-facing.
export async function predictActivityDrop(gridId, { asOf } = {}) {
  const { data } = await client.get(`/network/ml/grid/${gridId}/predict-anomaly`, {
    params: asOf ? { as_of: asOf } : {},
  });
  return data;
}

/* ---- Rule-based anomalies ---- */

// Backend wraps the alert/hotspot list in an envelope: { as_of, data: [...] }
// rather than returning a bare array — unwrap defensively so callers always
// get a plain array regardless of which shape actually comes back.
function unwrapList(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.data)) return payload.data;
  return [];
}

export async function getAlerts(params = {}) {
  const { data } = await client.get('/network/rule/alerts', { params });
  return unwrapList(data);
}

export async function getHotspots(params = {}) {
  const { data } = await client.get('/network/rule/hotspots', { params });
  return unwrapList(data);
}

// Per-day alert counts by severity, for a trend chart. Not yet on the
// backend — see routes list.
export async function getAlertsTimeline({ days = 14 } = {}) {
  const { data } = await client.get('/network/rule/alerts/timeline', { params: { days } });
  return unwrapList(data);
}

/* ---- Aggregate profiles (for dashboard charts) ---- */

// Average city-wide activity per hour of day (0-23). Pass `date` to scope
// to a single day instead of the all-time average. Not yet on the backend.
export async function getHourlyProfile({ date } = {}) {
  const { data } = await client.get('/network/summary/hourly-profile', { params: { date } });
  return data;
}

// Average activity per day of week (Mon-Sun), all-time. Not yet on the backend.
export async function getDayOfWeekProfile() {
  const { data } = await client.get('/network/summary/day-of-week-profile');
  return data;
}

// Trailing N days of voice/sms/internet activity split, for a traffic
// composition chart. Not yet on the backend.
export async function getTrafficMix({ days = 14 } = {}) {
  const { data } = await client.get('/network/summary/traffic-mix', { params: { days } });
  return data;
}

// Busiest grids by activity (optionally for one day), for a leaderboard
// chart. Not yet on the backend.
export async function getTopGrids({ date, limit = 10 } = {}) {
  const { data } = await client.get('/network/grids/top', { params: { date, limit } });
  return data;
}

// Network-wide min/avg/max for each ML feature, used as a benchmark line
// against a single grid's features. Not yet on the backend.
export async function getFeaturesSummary() {
  const { data } = await client.get('/network/features/summary');
  return data;
}

/* ---- AI NOC Assistant ---- */

export async function sendAiChatMessage(message, history = [], enabledPlugins = null) {
  const payload = { message, history };
  if (enabledPlugins) {
    payload.enabled_plugins = enabledPlugins;
  }
  const { data } = await client.post('/ai/chat', payload);
  return data; // { response, traces, duration_ms, active_plugins, compliance_passed }
}

export async function getAiPlugins() {
  const { data } = await client.get('/ai/plugins');
  return data;
}