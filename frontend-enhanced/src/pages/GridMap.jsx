import { useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { MapContainer, TileLayer, Rectangle, useMap } from 'react-leaflet';
import { getGrids, getGridsActivityForDate } from '../api/client';
import GridDetailPanel from '../components/GridDetailPanel';
import './grid-map.css';

// Milan city center — the Telecom Italia grid dataset is centered here.
// Adjust if your dataset covers a different city.
const DEFAULT_CENTER = [45.4642, 9.19];
const DEFAULT_ZOOM = 12;
const DEFAULT_DATE = '2013-11-01';
// One day into the dataset, so a full trailing 24h window is available for
// the activity_lag_24 feature right out of the box.
const DEFAULT_AS_OF = '2013-11-02T00:00';
const MIN_GRID_ID = 1;
const MAX_GRID_ID = 10000;

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

// Blue -> amber -> red, matching the token heat scale. `ratio` is the
// grid's activity relative to the busiest grid that day (0-1), so the
// scale is always meaningful regardless of the raw units involved.
function heatColor(ratio) {
  if (ratio == null) return 'var(--text-faint)';
  if (ratio >= 0.75) return 'var(--heat-critical)';
  if (ratio >= 0.5) return 'var(--heat-high)';
  if (ratio >= 0.22) return 'var(--heat-mid)';
  return 'var(--heat-low)';
}

// No polygon geometry is stored, so every grid renders as a small square
// around its centroid — good enough at city zoom levels.
function squareAround([lat, lon], meters = 120) {
  const dLat = meters / 111_320;
  const dLon = meters / (111_320 * Math.cos((lat * Math.PI) / 180));
  return [
    [lat - dLat, lon - dLon],
    [lat + dLat, lon + dLon],
  ];
}

function FlyToGrid({ grids, gridId }) {
  const map = useMap();
  useEffect(() => {
    if (!gridId) return;
    const grid = grids.find((g) => String(g.grid_id) === String(gridId));
    if (grid) {
      map.flyTo([grid.centroid_lat, grid.centroid_lon], 15, { duration: 0.6 });
    }
  }, [gridId, grids, map]);
  return null;
}

export default function GridMap() {
  const [grids, setGrids] = useState([]);
  const [selectedDate, setSelectedDate] = useState(DEFAULT_DATE);
  const [asOf, setAsOf] = useState(DEFAULT_AS_OF);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedGrid = searchParams.get('grid');

  const [searchValue, setSearchValue] = useState('');
  const [searchError, setSearchError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    // With a date selected, fetch centroid + that day's activity in one
    // call so the map can be colored as a heat grid. Falling back to plain
    // getGrids() (no activity) only if the date is cleared.
    const request = selectedDate
      ? getGridsActivityForDate(selectedDate)
      : getGrids();

    request
      .then((data) => !cancelled && setGrids(Array.isArray(data) ? data : []))
      .catch(() => !cancelled && setError('Could not load grid data for that day.'))
      .finally(() => !cancelled && setLoading(false));

    return () => {
      cancelled = true;
    };
  }, [selectedDate]);

  // Normalize each grid's activity against the busiest grid in the current
  // dataset, so the heat scale is relative rather than tied to fixed units
  // that would break as soon as the underlying data volume changes.
  const maxActivity = useMemo(() => {
    const values = grids.map((g) => g.activity).filter((v) => typeof v === 'number');
    return values.length ? Math.max(...values, 1) : null;
  }, [grids]);

  const shapes = useMemo(
    () =>
      grids.map((g) => {
        const ratio =
          maxActivity && typeof g.activity === 'number' ? g.activity / maxActivity : null;
        return {
          gridId: g.grid_id,
          color: heatColor(ratio),
          bounds: squareAround([g.centroid_lat, g.centroid_lon]),
          center: [g.centroid_lat, g.centroid_lon],
        };
      }),
    [grids, maxActivity]
  );

  const selectedShape = useMemo(
  () => shapes.find((s) => String(s.gridId) === String(selectedGrid)),
  [shapes, selectedGrid]
);

{shapes.map((shape) => (
  <Rectangle
    key={shape.gridId}
    bounds={shape.bounds}
    pathOptions={{ color: shape.color, weight: 1, fillOpacity: 0.6 }}
    eventHandlers={{ click: () => selectGrid(shape.gridId) }}
  />
))}

{selectedShape && (
  <Rectangle
    bounds={squareAround(selectedShape.center, 170)}
    pathOptions={{ color: 'var(--ink)', weight: 3, fill: false, className: 'grid-cell-selected' }}
    interactive={false}
  />
)}

  function selectGrid(gridId) {
    setSearchParams({ grid: gridId });
  }

  function closePanel() {
    searchParams.delete('grid');
    setSearchParams(searchParams);
  }

  function handleSearchSubmit(e) {
    e.preventDefault();
    const id = Number(searchValue);
    if (!Number.isInteger(id) || id < MIN_GRID_ID || id > MAX_GRID_ID) {
      setSearchError(`Enter a grid ID between ${MIN_GRID_ID} and ${MAX_GRID_ID}.`);
      return;
    }
    if (loading && grids.length === 0) {
      setSearchError('Grid data is still loading — try again in a moment.');
      return;
    }
    const exists = grids.some((g) => String(g.grid_id) === String(id));
    if (!exists) {
      setSearchError(`Grid ${id} isn't in this dataset.`);
      return;
    }
    setSearchError(null);
    setSearchValue('');
    selectGrid(id);
  }

  return (
    <div className="grid-map-page">
      <header className="page-header grid-map-header">
        <div className="grid-map-title-group">
          <h1>Grid map</h1>
          {loading && <span className="page-header-meta">Loading grids…</span>}
          {error && <span className="page-header-meta grid-map-error">{error}</span>}
        </div>
        <form className="grid-map-search" onSubmit={handleSearchSubmit}>
          <input
            type="number"
            inputMode="numeric"
            min={MIN_GRID_ID}
            max={MAX_GRID_ID}
            placeholder={`Grid ID (${MIN_GRID_ID}\u2013${MAX_GRID_ID})`}
            value={searchValue}
            onChange={(e) => {
              setSearchValue(e.target.value);
              if (searchError) setSearchError(null);
            }}
          />
          <button type="submit">Go</button>
        </form>
      </header>
      {searchError && <div className="grid-map-search-error">{searchError}</div>}

      <div className="grid-map-toolbar">
        <label className="grid-map-date-filter">
          <span>Day</span>
          <input
            type="date"
            value={selectedDate}
            max={todayIso()}
            onChange={(e) => setSelectedDate(e.target.value)}
          />
        </label>
        <button
          type="button"
          className="grid-map-date-clear"
          onClick={() => setSelectedDate('')}
          disabled={!selectedDate}
        >
          All-time
        </button>

        <span className="grid-map-toolbar-divider" />

        <label className="grid-map-date-filter">
          <span>Predicting as of</span>
          <input
            type="datetime-local"
            value={asOf}
            max={`${todayIso()}T23:59`}
            onChange={(e) => setAsOf(e.target.value)}
          />
        </label>
      </div>

      <div className="grid-map-legend">
        <span className="grid-map-legend-title">Activity</span>
        <span className="grid-map-legend-swatch" style={{ background: 'var(--heat-low)' }} /> Low
        <span className="grid-map-legend-swatch" style={{ background: 'var(--heat-mid)' }} /> Medium
        <span className="grid-map-legend-swatch" style={{ background: 'var(--heat-high)' }} /> High
        <span className="grid-map-legend-swatch" style={{ background: 'var(--heat-critical)' }} /> Critical
      </div>

      <div className="grid-map-canvas">
        <MapContainer center={DEFAULT_CENTER} zoom={DEFAULT_ZOOM} style={{ height: '100%', width: '100%' }}>
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          <FlyToGrid grids={grids} gridId={selectedGrid} />
            {shapes.map((shape) => (
              <Rectangle
                key={shape.gridId}
                bounds={shape.bounds}
                pathOptions={{ color: shape.color, weight: 1, fillOpacity: 0.6 }}
                eventHandlers={{ click: () => selectGrid(shape.gridId) }}
              />
            ))}
        </MapContainer>

        <GridDetailPanel gridId={selectedGrid} asOf={asOf} onClose={closePanel} />
      </div>
    </div>
  );
}
