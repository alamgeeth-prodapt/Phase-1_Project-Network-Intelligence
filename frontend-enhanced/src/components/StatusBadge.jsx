const STYLES = {
  ok: { color: 'var(--ok)', label: 'Normal' },
  warn: { color: 'var(--warn)', label: 'Elevated' },
  critical: { color: 'var(--critical)', label: 'Critical' },
};

// severity accepts raw backend values like "HIGH_ACTIVITY" as well as the
// normalized keys above — normalize defensively so new alert types don't crash.
function normalize(severity) {
  const s = String(severity || '').toLowerCase();
  if (s.includes('crit') || s.includes('high')) return 'critical';
  if (s.includes('warn') || s.includes('elev') || s.includes('med')) return 'warn';
  return 'ok';
}

export default function StatusBadge({ severity, children }) {
  const key = normalize(severity);
  const style = STYLES[key];
  return (
    <span
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 6,
        fontSize: 12.5,
        color: style.color,
      }}
    >
      <span
        style={{
          width: 6,
          height: 6,
          borderRadius: '50%',
          background: style.color,
        }}
        aria-hidden="true"
      />
      {children || style.label}
    </span>
  );
}
