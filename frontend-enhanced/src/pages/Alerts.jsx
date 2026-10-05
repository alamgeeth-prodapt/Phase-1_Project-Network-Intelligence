import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from 'recharts';
import { getAlerts, getHotspots, getAlertsTimeline } from '../api/client';
import StatusBadge from '../components/StatusBadge';
import './dashboard.css';

// Conservative default — some backends cap `limit` server-side and will
// 422 on anything above it. This 50 is the value the app used before we
// tried to raise it; bump it once you've confirmed the backend's actual
// ceiling (check the console log in the catch below for the real detail).
const ALL_ALERTS_LIMIT = 50;
// Same caution as above — raise once the backend's real ceiling for this
// route is confirmed (see console log in the catch below).
const ALL_HOTSPOTS_LIMIT = 50;
const HISTORY_RANGES = [7, 14, 30, 60];

// The dataset is historical (2013 Milan data) — same default used on the
// grid map page, so hotspots are evaluated against a point in time that
// actually has data instead of real-world "now".
const DEFAULT_HOTSPOTS_AS_OF = '2013-11-02T00:00';

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

function formatDate(value) {
  const d = new Date(value);
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

export default function Alerts() {
  const [alerts, setAlerts] = useState([]);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  const [hotspots, setHotspots] = useState(null);
  const [hotspotsError, setHotspotsError] = useState(null);
  const [hotspotsAsOf, setHotspotsAsOf] = useState(DEFAULT_HOTSPOTS_AS_OF);

  const [historyDays, setHistoryDays] = useState(14);
  const [timeline, setTimeline] = useState(null);
  const [timelineError, setTimelineError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    getAlerts({ limit: ALL_ALERTS_LIMIT })
      .then((data) => !cancelled && setAlerts(Array.isArray(data) ? data : []))
      .catch((err) => {
        if (cancelled) return;
        // Log the real cause instead of hiding it behind a generic message —
        // check the browser console for the status/body next time this fires.
        console.error('getAlerts failed:', err.response?.status, err.response?.data || err.message);
        const status = err.response?.status;
        if (status === 401) {
          setError('Session expired — please sign in again.');
        } else if (status === 422) {
          setError('Could not load alerts (the server rejected the request parameters).');
        } else {
          setError('Could not load alerts.');
        }
      })
      .finally(() => !cancelled && setLoading(false));
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    setHotspots(null);
    setHotspotsError(null);
    getHotspots({ limit: ALL_HOTSPOTS_LIMIT, as_of: hotspotsAsOf })
      .then((data) => !cancelled && setHotspots(Array.isArray(data) ? data : []))
      .catch((err) => {
        if (cancelled) return;
        console.error('getHotspots failed:', err.response?.status, err.response?.data || err.message);
        setHotspotsError('Could not load hotspots.');
      });
    return () => {
      cancelled = true;
    };
  }, [hotspotsAsOf]);

  useEffect(() => {
    let cancelled = false;
    setTimeline(null);
    setTimelineError(null);
    getAlertsTimeline({ days: historyDays })
      .then((data) => !cancelled && setTimeline(Array.isArray(data) ? data : []))
      .catch(() => !cancelled && setTimelineError('Alert history unavailable.'));
    return () => {
      cancelled = true;
    };
  }, [historyDays]);

  if (loading) return <div className="page-state">Loading alerts…</div>;
  if (error) return <div className="page-state page-state-error">{error}</div>;

  return (
    <div className="dashboard">
      <header className="page-header">
        <h1>Rule-based alerts</h1>
      </header>

      <div className="chart-grid-2">
        <section className="panel">
          <div className="panel-header">
            <h2>Overloaded grids</h2>
            <div className="hotspots-header-controls">
              {hotspots && <span className="panel-header-note data">{hotspots.length} shown</span>}
              <label className="date-field">
                <span>As of</span>
                <input
                  type="datetime-local"
                  value={hotspotsAsOf}
                  max={`${todayIso()}T23:59`}
                  onChange={(e) => setHotspotsAsOf(e.target.value)}
                />
              </label>
            </div>
          </div>
          {hotspotsError ? (
            <p className="panel-empty">{hotspotsError}</p>
          ) : !hotspots ? (
            <p className="panel-empty">Loading…</p>
          ) : hotspots.length === 0 ? (
            <p className="panel-empty">No high-activity grids right now.</p>
          ) : (
            <table className="data-table">
              <thead>
                <tr>
                  <th>Grid</th>
                  <th>Current</th>
                  <th>Baseline</th>
                  <th>Reason</th>
                </tr>
              </thead>
              <tbody>
                {hotspots.map((h) => (
                  <tr key={`${h.grid_id}-${h.timestamp}`}>
                    <td className="data">
                      <Link to={`/map?grid=${h.grid_id}`}>{h.grid_id}</Link>
                    </td>
                    <td className="data">{h.current_activity}</td>
                    <td className="data">{h.baseline_activity}</td>
                    <td className="hotspot-reason">
                      <StatusBadge severity="warn">{h.reason}</StatusBadge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>

        <section className="panel">
          <div className="panel-header">
            <h2>Alert history</h2>
            <div className="range-select" role="group" aria-label="History range">
              {HISTORY_RANGES.map((days) => (
                <button
                  key={days}
                  type="button"
                  className={`range-select-option${days === historyDays ? ' is-active' : ''}`}
                  onClick={() => setHistoryDays(days)}
                >
                  {days}d
                </button>
              ))}
            </div>
          </div>
          {timelineError ? (
            <p className="panel-empty">{timelineError}</p>
          ) : !timeline ? (
            <p className="panel-empty">Loading…</p>
          ) : timeline.length === 0 ? (
            <p className="panel-empty">No alert history yet.</p>
          ) : (
            <div className="chart-frame">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={timeline} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                  <CartesianGrid stroke="var(--border)" vertical={false} />
                  <XAxis
                    dataKey="date"
                    tickFormatter={formatDate}
                    stroke="var(--text-faint)"
                    tick={{ fontSize: 11.5, fontFamily: 'var(--font-data)' }}
                  />
                  <YAxis stroke="var(--text-faint)" tick={{ fontSize: 11.5, fontFamily: 'var(--font-data)' }} width={32} />
                  <Tooltip
                    content={({ active, payload, label }) => {
                      if (!active || !payload?.length) return null;
                      return (
                        <div className="chart-tooltip">
                          <div className="chart-tooltip-title data">{formatDate(label)}</div>
                          <div className="chart-tooltip-rows">
                            {payload.map((p) => (
                              <div key={p.dataKey}>
                                <dt style={{ color: p.fill }}>{p.dataKey}</dt>
                                <dd className="data">{p.value}</dd>
                              </div>
                            ))}
                          </div>
                        </div>
                      );
                    }}
                    cursor={{ fill: 'var(--border)', opacity: 0.4 }}
                  />
                  <Bar dataKey="critical" stackId="a" fill="var(--critical)" radius={[0, 0, 0, 0]} />
                  <Bar dataKey="warn" stackId="a" fill="var(--warn)" radius={[2, 2, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}
        </section>
      </div>

      <section className="panel">
        <div className="panel-header">
          <h2>All alerts</h2>
          <span className="panel-header-note data">{alerts.length} shown</span>
        </div>
        {alerts.length === 0 ? (
          <p className="panel-empty">No active alerts.</p>
        ) : (
          <table className="data-table">
            <thead>
              <tr>
                <th>Severity</th>
                <th>Grid</th>
                <th>Timestamp</th>
                <th>Current</th>
                <th>Baseline</th>
                <th>Reason</th>
              </tr>
            </thead>
            <tbody>
              {alerts.map((a) => (
                <tr key={`${a.grid_id}-${a.timestamp}`}>
                  <td>
                    <StatusBadge severity={a.severity}>{a.severity}</StatusBadge>
                  </td>
                  <td className="data">
                    <Link to={`/map?grid=${a.grid_id}`}>{a.grid_id}</Link>
                  </td>
                  <td className="data">{a.timestamp}</td>
                  <td className="data">{a.current_activity}</td>
                  <td className="data">{a.baseline_activity}</td>
                  <td>{a.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}
