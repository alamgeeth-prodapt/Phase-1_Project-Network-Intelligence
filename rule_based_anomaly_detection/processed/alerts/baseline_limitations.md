# Within-Day Baseline: Known Limitations

This statement accompanies every `network_alerts` artifact produced by
`AnomalyDetector`. It should be read alongside the alerts, not filed away -
it defines what an alert does and does not mean.

## What the baseline is

Each grid/hour's baseline is the leave-one-out median of that grid's
`total_activity` across the other hours of the same day. It answers the
question "was this hour unusual relative to the rest of *this grid's this
day*?" - nothing more.

## What it structurally cannot distinguish

- **Whole-day anomalies are invisible.** The baseline is built entirely
  from that same day's other hours. If an entire grid is unusually busy or
  quiet ALL DAY - a real event, an outage, a pipeline bug - every hour
  looks "normal" relative to its neighbours. There is no day-over-day or
  week-over-week comparison in this baseline.
- **No cause attribution.** A HIGH_ACTIVITY alert cannot say whether it's a
  concert, a holiday, a network fault, a duplicate-ingestion bug, or a
  genuine incident. This is a pure statistical outlier flag, not a
  diagnosis.
- **Uneven false-positive rates across grid types.** A single global
  multiplier does not treat a naturally bursty downtown cell and a quiet
  residential cell equivalently. Even after the activity floor, some grids
  will alert more often simply because their ordinary hour-to-hour
  variance is higher - not because anything unusual happened.
- **Small-sample noise.** Each baseline is built from only 23 points.
  Alerts near a threshold are close to a coin flip against ordinary
  variation, especially for grids just above the floor.
- **No spatial correlation.** A real citywide event or network incident
  affecting many adjacent grids at once shows up as many independent
  single-grid alerts. Nothing here recognises they are the same event.
- **Overlapping rules muddy the alert count.** A single grid/hour can
  trigger more than one rule at once, so "alerts by type" is not the same
  as "grid/hours with a problem" - consumers should de-duplicate by
  (grid_id, timestamp) if they need the latter.
- **Data-quality artefacts look like anomalies.** A missed deduplication,
  a null-fill boundary effect, or a double-counted country_code slice
  upstream shifts a grid's numbers in exactly the same way a genuine event
  would, and this baseline cannot tell the two apart.

## What an alert should trigger downstream

An alert here is a candidate for review, not a confirmed incident. Any
consumer (automated or human) treating this artifact as ground truth for
network state should account for the above before acting on it.
