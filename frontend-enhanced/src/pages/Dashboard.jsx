import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  BarChart,
  Bar,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from 'recharts';
import {
  getNetworkSummary,
  getHourlyProfile,
  getTopGrids,
  getTrafficMix,
  getDayOfWeekProfile,
} from '../api/client';
import './dashboard.css';

function formatDate(value) {
  const d = new Date(value);
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

function formatHour(h) {
  return `${String(h).padStart(2, '0')}:00`;
}

// Friendly labels for the traffic-mix series, so the tooltip doesn't leak
// raw API field names (voice_activity) straight into the UI.
const TRAFFIC_SERIES_LABEL = {
  voice_activity: 'Voice',
  sms_activity: 'SMS',
  internet_activity: 'Internet',
};

function TrafficMixTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-title data">{formatDate(label)}</div>
      <div className="chart-tooltip-rows">
        {payload.map((p) => (
          <div key={p.dataKey}>
            <dt style={{ color: p.stroke }}>{TRAFFIC_SERIES_LABEL[p.dataKey] ?? p.dataKey}</dt>
            <dd className="data">{Math.round(p.value).toLocaleString()}</dd>
          </div>
        ))}
      </div>
    </div>
  );
}

// Compact axis labels (100K, 1.2M) instead of raw digits — activity totals
// run to 6+ figures, which doesn't fit the narrow axis width as plain
// numbers. Tooltips still show exact values via Math.round elsewhere.
const compactNumber = new Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 });
function formatCompact(value) {
  return compactNumber.format(value);
}

// Custom tooltip: shows the full daily summary for whichever point the
// user is hovering, not just the plotted activity value.
function SummaryTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const day = payload[0].payload;
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-title data">{formatDate(day.summary_date)}</div>
      <dl className="chart-tooltip-rows">
        <div>
          <dt>Total activity</dt>
          <dd className="data">{day.total_activity}</dd>
        </div>
        <div>
          <dt>Active grids</dt>
          <dd className="data">{day.active_grids}</dd>
        </div>
        <div>
          <dt>Peak hour</dt>
          <dd className="data">{day.peak_hour}:00</dd>
        </div>
        <div>
          <dt>Top grid</dt>
          <dd className="data">{day.top_grid}</dd>
        </div>
      </dl>
    </div>
  );
}

function HourTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const point = payload[0].payload;
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-title data">{formatHour(point.hour)}</div>
      <div className="chart-tooltip-rows">
        <div>
          <dt>Avg activity</dt>
          <dd className="data">{Math.round(point.avg_activity)}</dd>
        </div>
      </div>
    </div>
  );
}

function GridTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const point = payload[0].payload;
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-title data">Grid {point.grid_id}</div>
      <div className="chart-tooltip-rows">
        <div>
          <dt>Activity</dt>
          <dd className="data">{Math.round(point.activity)}</dd>
        </div>
      </div>
    </div>
  );
}

// Small trend arrow comparing the latest value to the prior day — derived
// from data already on hand, no extra request needed.
function Delta({ current, previous }) {
  if (current == null || previous == null || previous === 0) return null;
  const pct = ((current - previous) / previous) * 100;
  const dir = pct > 0.5 ? 'up' : pct < -0.5 ? 'down' : 'flat';
  const sign = pct > 0 ? '+' : '';
  return (
    <span className={`stat-delta stat-delta-${dir}`}>
      {dir === 'up' && (
        <svg width="9" height="9" viewBox="0 0 10 10"><path d="M1 9 L9 1 M9 1 L4 1 M9 1 L9 6" stroke="currentColor" strokeWidth="1.4" fill="none" strokeLinecap="round" strokeLinejoin="round" /></svg>
      )}
      {dir === 'down' && (
        <svg width="9" height="9" viewBox="0 0 10 10"><path d="M1 1 L9 9 M9 9 L4 9 M9 9 L9 4" stroke="currentColor" strokeWidth="1.4" fill="none" strokeLinecap="round" strokeLinejoin="round" /></svg>
      )}
      {dir === 'flat' && (
        <svg width="9" height="9" viewBox="0 0 10 10"><path d="M1 5 L9 5" stroke="currentColor" strokeWidth="1.4" fill="none" strokeLinecap="round" /></svg>
      )}
      <span className="data">{sign}{pct.toFixed(1)}%</span>
    </span>
  );
}

export default function Dashboard() {
  const [summaries, setSummaries] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  const [hourlyProfile, setHourlyProfile] = useState(null);
  const [hourlyError, setHourlyError] = useState(null);

  const [topGrids, setTopGrids] = useState(null);
  const [topGridsError, setTopGridsError] = useState(null);

  const [trafficMix, setTrafficMix] = useState(null);
  const [trafficMixError, setTrafficMixError] = useState(null);

  const [dayOfWeek, setDayOfWeek] = useState(null);
  const [dayOfWeekError, setDayOfWeekError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const summaryData = await getNetworkSummary();
        if (cancelled) return;
        // Backend returns daily summaries ordered by date ascending.
        setSummaries(Array.isArray(summaryData) ? summaryData : []);
      } catch {
        if (!cancelled) setError('Could not load network summary.');
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    getHourlyProfile({})
      .then((data) => !cancelled && setHourlyProfile(Array.isArray(data) ? data : []))
      .catch(() => !cancelled && setHourlyError('Hourly profile unavailable.'));
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    getTopGrids({ limit: 8 })
      .then((data) => !cancelled && setTopGrids(Array.isArray(data) ? data : []))
      .catch(() => !cancelled && setTopGridsError('Grid leaderboard unavailable.'));
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    getTrafficMix({ days: 21 })
      .then((data) => !cancelled && setTrafficMix(Array.isArray(data) ? data : []))
      .catch(() => !cancelled && setTrafficMixError('Traffic mix unavailable.'));
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    getDayOfWeekProfile()
      .then((data) => !cancelled && setDayOfWeek(Array.isArray(data) ? data : []))
      .catch(() => !cancelled && setDayOfWeekError('Day-of-week profile unavailable.'));
    return () => {
      cancelled = true;
    };
  }, []);

  if (loading) return <div className="page-state">Loading summary…</div>;
  if (error) return <div className="page-state page-state-error">{error}</div>;

  const latest = summaries.length ? summaries[summaries.length - 1] : null;
  const prior = summaries.length > 1 ? summaries[summaries.length - 2] : null;

  const stats = latest
    ? [
        { label: 'Total activity', value: latest.total_activity, prior: prior?.total_activity },
        { label: 'Active grids', value: latest.active_grids, prior: prior?.active_grids },
        { label: 'Peak hour', value: `${latest.peak_hour}:00`, prior: null },
        { label: 'Top grid', value: latest.top_grid, prior: null },
      ]
    : [];

  const peakHour = hourlyProfile?.length
    ? hourlyProfile.reduce((max, p) => (p.avg_activity > max.avg_activity ? p : max), hourlyProfile[0]).hour
    : null;

  return (
    <div className="dashboard">
      <header className="page-header">
        <h1>Network summary</h1>
        {latest && <span className="page-header-meta data">as of {formatDate(latest.summary_date)}</span>}
      </header>

      {latest && (
        <div className="stat-row">
          {stats.map((stat) => (
            <div className="stat-card" key={stat.label}>
              <span className="stat-label">{stat.label}</span>
              <div className="stat-value-row">
                <span className="stat-value data">{stat.value ?? '—'}</span>
                <Delta current={stat.prior != null ? stat.value : null} previous={stat.prior} />
              </div>
            </div>
          ))}
        </div>
      )}

      <div className="chart-grid-2">
        <section className="panel">
          <div className="panel-header">
            <h2>Activity by day</h2>
          </div>
          {summaries.length === 0 ? (
            <p className="panel-empty">No summary data yet.</p>
          ) : (
            <div className="chart-frame">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={summaries} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                  <defs>
                    <linearGradient id="activityFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="var(--beacon)" stopOpacity={0.28} />
                      <stop offset="100%" stopColor="var(--beacon)" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid stroke="var(--border)" vertical={false} />
                  <XAxis
                    dataKey="summary_date"
                    tickFormatter={formatDate}
                    stroke="var(--text-faint)"
                    tick={{ fontSize: 11.5, fontFamily: 'var(--font-data)' }}
                  />
                  <YAxis
                    stroke="var(--text-faint)"
                    tick={{ fontSize: 11.5, fontFamily: 'var(--font-data)' }}
                    tickFormatter={formatCompact}
                    width={44}
                  />
                  <Tooltip content={<SummaryTooltip />} />
                  <Area
                    type="monotone"
                    dataKey="total_activity"
                    stroke="var(--beacon)"
                    strokeWidth={2}
                    fill="url(#activityFill)"
                    dot={{ r: 3, stroke: 'var(--beacon)', fill: 'var(--panel)' }}
                    activeDot={{ r: 5 }}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          )}
        </section>

        <section className="panel">
          <div className="panel-header">
            <h2>Activity by hour</h2>
            {peakHour != null && <span className="panel-header-note data">peak {formatHour(peakHour)}</span>}
          </div>
          {hourlyError ? (
            <p className="panel-empty">{hourlyError}</p>
          ) : !hourlyProfile ? (
            <p className="panel-empty">Loading…</p>
          ) : hourlyProfile.length === 0 ? (
            <p className="panel-empty">No hourly data yet.</p>
          ) : (
            <div className="chart-frame">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={hourlyProfile} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                  <CartesianGrid stroke="var(--border)" vertical={false} />
                  <XAxis
                    dataKey="hour"
                    tickFormatter={(h) => (h % 3 === 0 ? h : '')}
                    stroke="var(--text-faint)"
                    tick={{ fontSize: 11.5, fontFamily: 'var(--font-data)' }}
                  />
                  <YAxis
                    stroke="var(--text-faint)"
                    tick={{ fontSize: 11.5, fontFamily: 'var(--font-data)' }}
                    tickFormatter={formatCompact}
                    width={44}
                  />
                  <Tooltip content={<HourTooltip />} cursor={{ fill: 'var(--border)', opacity: 0.4 }} />
                  <Bar dataKey="avg_activity" radius={[2, 2, 0, 0]}>
                    {hourlyProfile.map((p) => (
                      <Cell key={p.hour} fill={p.hour === peakHour ? 'var(--beacon)' : 'var(--heat-low)'} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}
        </section>
      </div>

      <div className="chart-grid-2">
        <section className="panel">
          <div className="panel-header">
            <h2>Traffic composition</h2>
          </div>
          {trafficMixError ? (
            <p className="panel-empty">{trafficMixError}</p>
          ) : !trafficMix ? (
            <p className="panel-empty">Loading…</p>
          ) : trafficMix.length === 0 ? (
            <p className="panel-empty">No traffic data yet.</p>
          ) : (
            <div className="chart-frame">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={trafficMix} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                  <CartesianGrid stroke="var(--border)" vertical={false} />
                  <XAxis
                    dataKey="summary_date"
                    tickFormatter={formatDate}
                    stroke="var(--text-faint)"
                    tick={{ fontSize: 11.5, fontFamily: 'var(--font-data)' }}
                  />
                  <YAxis stroke="var(--text-faint)" tick={{ fontSize: 11.5, fontFamily: 'var(--font-data)' }} tickFormatter={formatCompact} width={44} />
                  <Tooltip content={<TrafficMixTooltip />} />
                  <Legend
                    wrapperStyle={{ fontSize: 12 }}
                    formatter={(value) => <span className="legend-label">{value.replace('_activity', '')}</span>}
                  />
                  <Area type="monotone" dataKey="voice_activity" stackId="mix" stroke="var(--series-voice)" fill="var(--series-voice)" fillOpacity={0.55} />
                  <Area type="monotone" dataKey="sms_activity" stackId="mix" stroke="var(--series-sms)" fill="var(--series-sms)" fillOpacity={0.55} />
                  <Area type="monotone" dataKey="internet_activity" stackId="mix" stroke="var(--series-internet)" fill="var(--series-internet)" fillOpacity={0.55} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          )}
        </section>

        <section className="panel">
          <div className="panel-header">
            <h2>Weekly rhythm</h2>
          </div>
          {dayOfWeekError ? (
            <p className="panel-empty">{dayOfWeekError}</p>
          ) : !dayOfWeek ? (
            <p className="panel-empty">Loading…</p>
          ) : dayOfWeek.length === 0 ? (
            <p className="panel-empty">No data yet.</p>
          ) : (
            <div className="chart-frame">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={dayOfWeek} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                  <CartesianGrid stroke="var(--border)" vertical={false} />
                  <XAxis dataKey="label" stroke="var(--text-faint)" tick={{ fontSize: 11.5, fontFamily: 'var(--font-data)' }} />
                  <YAxis stroke="var(--text-faint)" tick={{ fontSize: 11.5, fontFamily: 'var(--font-data)' }} tickFormatter={formatCompact} width={44} />
                  <Tooltip
                    content={({ active, payload, label }) => {
                      if (!active || !payload?.length) return null;
                      return (
                        <div className="chart-tooltip">
                          <div className="chart-tooltip-title data">{label}</div>
                          <div className="chart-tooltip-rows">
                            <div><dt>Avg activity</dt><dd className="data">{Math.round(payload[0].value)}</dd></div>
                          </div>
                        </div>
                      );
                    }}
                    cursor={{ fill: 'var(--border)', opacity: 0.4 }}
                  />
                  <Bar dataKey="avg_activity" radius={[2, 2, 0, 0]}>
                    {dayOfWeek.map((d) => (
                      <Cell key={d.day_of_week} fill={d.day_of_week >= 5 ? 'var(--heat-mid)' : 'var(--heat-low)'} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}
        </section>
      </div>

      <section className="panel">
        <div className="panel-header">
          <h2>Busiest grids</h2>
          <Link to="/map" className="panel-link">Open map</Link>
        </div>
        {topGridsError ? (
          <p className="panel-empty">{topGridsError}</p>
        ) : !topGrids ? (
          <p className="panel-empty">Loading…</p>
        ) : topGrids.length === 0 ? (
          <p className="panel-empty">No grid activity yet.</p>
        ) : (
          <div className="chart-frame chart-frame-wide">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={[...topGrids].sort((a, b) => a.activity - b.activity)}
                layout="vertical"
                margin={{ top: 4, right: 16, bottom: 0, left: 0 }}
              >
                <CartesianGrid stroke="var(--border)" horizontal={false} />
                <XAxis type="number" hide />
                <YAxis
                  type="category"
                  dataKey="grid_id"
                  stroke="var(--text-faint)"
                  tick={{ fontSize: 11.5, fontFamily: 'var(--font-data)' }}
                  width={48}
                />
                <Tooltip content={<GridTooltip />} cursor={{ fill: 'var(--border)', opacity: 0.4 }} />
                <Bar dataKey="activity" fill="var(--heat-mid)" radius={[0, 2, 2, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </section>
    </div>
  );
}