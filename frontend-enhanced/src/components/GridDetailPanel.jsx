import { useEffect, useMemo, useState } from 'react';
import {
  ResponsiveContainer,
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  RadialBarChart,
  RadialBar,
  PolarAngleAxis,
} from 'recharts';
import { getGridFeatures, getGridTimeseries, getFeaturesSummary, predictActivityDrop } from '../api/client';
import StatusBadge from './StatusBadge';
import './grid-detail-panel.css';

const FEATURE_ROWS = [
  { key: 'avg_activity', label: 'Avg activity' },
  { key: 'activity_growth', label: 'Activity growth' },
  { key: 'active_hours', label: 'Active hours' },
  { key: 'peak_ratio', label: 'Peak ratio' },
  { key: 'variability', label: 'Variability' },
  { key: 'internet_share_at_t', label: 'Internet share' },
];

// datetime-local gives "YYYY-MM-DDTHH:mm" (no seconds) — normalize to a
// full ISO string for the backend, and split into date/hour for the
// timeseries endpoint, which takes those separately.
function parseAsOf(asOf) {
  if (!asOf) return { iso: null, date: null, hour: null };
  const [datePart, timePart = '00:00'] = asOf.split('T');
  const hour = Number(timePart.split(':')[0]);
  return { iso: `${asOf}:00`, date: datePart, hour: Number.isNaN(hour) ? null : hour };
}

function formatAsOf(asOf) {
  if (!asOf) return null;
  const d = new Date(asOf);
  if (Number.isNaN(d.getTime())) return asOf;
  return d.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export default function GridDetailPanel({ gridId, asOf, onClose }) {
  const [features, setFeatures] = useState(null);
  const [prediction, setPrediction] = useState(null);
  const [predicting, setPredicting] = useState(false);
  const [error, setError] = useState(null);
  const [actuals, setActuals] = useState({ current: 'Loading...', next: 'Loading...' });
  const [series, setSeries] = useState(null);
  const [networkAvg, setNetworkAvg] = useState(null);


  useEffect(() => {
    if (!gridId || !asOf) return;

    async function fetchActuals() {
      try {
        // 1. Fetch data up to the currently selected 'asOf' time
        const currentReq = await getGridTimeseries(gridId, { asOf });
        const currentData = currentReq.data;
        
        // The last item in the array is the current hour
        const currentActivity = currentData.length > 0 
          ? currentData[currentData.length - 1].total_activity 
          : 'N/A';

        // 2. Calculate the timestamp for exactly 1 hour into the future
        const [datePart, timePart] = asOf.split('T');
        const [yyyy, mm, dd] = datePart.split('-');
        const [hh, min] = timePart.split(':');
        
        const targetDate = new Date(yyyy, mm - 1, dd, hh, min);
        targetDate.setHours(targetDate.getHours() + 1);
        
        const pad = (n) => String(n).padStart(2, '0');
        const nextHourAsOf = `${targetDate.getFullYear()}-${pad(targetDate.getMonth() + 1)}-${pad(targetDate.getDate())}T${pad(targetDate.getHours())}:${pad(targetDate.getMinutes())}`;

        // 3. Fetch data up to the NEXT hour to get the model's target
        const nextReq = await getGridTimeseries(gridId, { asOf: nextHourAsOf });
        const nextData = nextReq.data;
        const nextActivity = nextData.length > 0 
          ? nextData[nextData.length - 1].total_activity 
          : 'N/A';

        setActuals({ current: currentActivity, next: nextActivity });
      } catch (error) {
        console.error("Failed to fetch actual activities", error);
        setActuals({ current: 'Error', next: 'Error' });
      }
    }

    fetchActuals();
  }, [gridId, asOf]);
  // Network-wide feature benchmark — same for every grid, so fetch once.
  useEffect(() => {
    let cancelled = false;
    getFeaturesSummary()
      .then((data) => !cancelled && setNetworkAvg(data))
      .catch(() => {}); // benchmark is a nice-to-have; missing it shouldn't block the panel
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!gridId) return;
    let cancelled = false;
    const { iso } = parseAsOf(asOf);
    setFeatures(null);
    setPrediction(null);
    setError(null);
    setSeries(null);

    getGridFeatures(gridId, { asOf: iso })
      .then((data) => !cancelled && setFeatures(data))
      .catch(() => !cancelled && setError('Could not load features for this grid.'));

    // Trailing window ending at the chosen moment, so the sparkline shows
    // exactly the 24h the model's activity_lag_24 feature is drawn from.
    getGridTimeseries(gridId, { asOf: iso })
      .then((data) => {
        if (cancelled) return;
        const points = data?.series || data?.timeseries || (Array.isArray(data) ? data : []);
        setSeries(points);
      })
      .catch(() => {}); // sparkline is supplementary — feature load errors already surface above

    return () => {
      cancelled = true;
    };
  }, [gridId, asOf]);

  async function handlePredict() {
    setPredicting(true);
    setError(null);
    try {
      const { iso } = parseAsOf(asOf);
      const result = await predictActivityDrop(gridId, { asOf: iso });
      setPrediction(result);
    } catch {
      setError('Prediction failed — model or feature data may be unavailable.');
    } finally {
      setPredicting(false);
    }
  }

  const comparisonData = useMemo(() => {
    if (!features?.features) return [];
    const f = features.features;
    return FEATURE_ROWS.map((row) => ({
      label: row.label,
      grid: typeof f[row.key] === 'number' ? f[row.key] : 0,
      network: typeof networkAvg?.[row.key]?.avg === 'number' ? networkAvg[row.key].avg : null,
    }));
  }, [features, networkAvg]);

  if (!gridId) return null;

  const f = features?.features;
  const isStale = features?.feature_freshness?.status !== 'FRESH';
  const gaugeValue = prediction ? Math.round((prediction.drop_probability ?? 0) * 100) : 0;

  return (
    <aside className="grid-panel">
      <div className="grid-panel-header">
        <h2 className="data">Grid {gridId}</h2>
        <button className="grid-panel-close" onClick={onClose} aria-label="Close panel">
          ×
        </button>
      </div>

      {asOf && <div className="grid-panel-asof">as of <span className="data">{formatAsOf(asOf)}</span></div>}

      <div className="grid-panel-ai-action">
        <button
          type="button"
          className="grid-panel-ai-btn"
          onClick={() => {
            window.dispatchEvent(
              new CustomEvent('gnc-ask-ai', {
                detail: {
                  prompt: `Investigate grid ${gridId}${asOf ? ` (as of ${asOf})` : ''}: analyze features, rule alerts, and ML anomaly drop risk.`,
                  autoSend: true,
                },
              })
            );
          }}
          title="Ask AI NOC Copilot to analyze this grid"
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3Z" />
            <path d="M19 10v2a7 7 0 0 1-14 0v-2" />
            <line x1="12" x2="12" y1="19" y2="22" />
            <line x1="8" x2="16" y1="22" y2="22" />
          </svg>
          Ask AI NOC Copilot
        </button>
      </div>

      {error && <p className="grid-panel-error">{error}</p>}

      {!features && !error && <p className="grid-panel-loading">Loading features…</p>}
      
      {series?.length > 1 && (
        <div className="grid-panel-section">
          <div className="grid-panel-section-title">Last 24h</div>
          <div className="grid-panel-sparkline">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={series} margin={{ top: 4, right: 4, bottom: 0, left: 4 }}>
                <Line
                  type="monotone"
                  dataKey="activity"
                  stroke="var(--beacon)"
                  strokeWidth={1.75}
                  dot={false}
                  isAnimationActive={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {features && (
        <>
          <div className="grid-panel-section">
            <div className="grid-panel-section-title">ML features</div>
            {isStale && (
              <div className="grid-panel-stale">Feature data is stale — prediction may be unreliable.</div>
            )}
            
            <dl className="grid-panel-features">
              {/* --- NEW ACTUAL ACTIVITY ROWS --- */}
              <FeatureRow 
                label="Current actual activity" 
                value={typeof actuals?.current === 'number' ? actuals.current.toFixed(4) : actuals?.current} 
              />
              <FeatureRow 
                label="Next hour actual activity" 
                value={typeof actuals?.next === 'number' ? actuals.next.toFixed(4) : actuals?.next} 
              />
              {/* -------------------------------- */}

              {FEATURE_ROWS.map((row) => (
                <FeatureRow key={row.key} label={row.label} value={f?.[row.key]} />
              ))}
            </dl>
          </div>

          {comparisonData.length > 0 && comparisonData.some((d) => d.network != null) && (
            <div className="grid-panel-section">
              <div className="grid-panel-section-title grid-panel-section-title-row">
                <span>Features vs network</span>
                <span className="grid-panel-legend">
                  <span className="grid-panel-legend-swatch grid-panel-legend-grid" />this grid
                  <span className="grid-panel-legend-swatch grid-panel-legend-net" />network
                </span>
              </div>
              <div className="grid-panel-compare">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart
                    data={comparisonData}
                    layout="vertical"
                    barGap={1}
                    margin={{ top: 0, right: 8, bottom: 0, left: 0 }}
                  >
                    <XAxis type="number" hide />
                    <YAxis
                      type="category"
                      dataKey="label"
                      width={78}
                      tick={{ fontSize: 10.5 }}
                      stroke="var(--text-faint)"
                    />
                    <Bar dataKey="network" fill="var(--series-baseline)" radius={[0, 2, 2, 0]} barSize={5} />
                    <Bar dataKey="grid" fill="var(--beacon)" radius={[0, 2, 2, 0]} barSize={5} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}

          <div className="grid-panel-section">
            <div className="grid-panel-section-title">Activity drop prediction</div>
            {!prediction ? (
              <button className="grid-panel-predict" onClick={handlePredict} disabled={predicting}>
                {predicting ? 'Predicting…' : 'Predict activity drop'}
              </button>
            ) : (
              <div className="grid-panel-prediction">
                <div className="grid-panel-gauge">
                  <ResponsiveContainer width="100%" height="100%">
                    <RadialBarChart
                      innerRadius="72%"
                      outerRadius="100%"
                      barSize={9}
                      data={[{ value: gaugeValue }]}
                      startAngle={90}
                      endAngle={-270}
                    >
                      <PolarAngleAxis type="number" domain={[0, 100]} angleAxisId={0} tick={false} />
                      <RadialBar
                        dataKey="value"
                        cornerRadius={5}
                        fill={prediction.anomaly_predicted ? 'var(--critical)' : 'var(--live)'}
                        background={{ fill: 'var(--border)' }}
                      />
                    </RadialBarChart>
                  </ResponsiveContainer>
                  <span className="grid-panel-gauge-value data">{gaugeValue}%</span>
                </div>
                <StatusBadge severity={prediction.anomaly_predicted ? 'critical' : 'ok'}>
                  {prediction.anomaly_predicted ? 'Drop predicted' : 'No drop predicted'}
                </StatusBadge>
              </div>
            )}
          </div>
        </>
      )}
    </aside>
  );
}

function FeatureRow({ label, value }) {
  return (
    <div className="grid-panel-feature-row">
      <dt>{label}</dt>
      <dd className="data">{value ?? '—'}</dd>
    </div>
  );
}