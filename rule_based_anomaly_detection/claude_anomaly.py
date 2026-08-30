"""
detector.py

AnomalyDetector: flags anomalous grid/hours in the grid/hour analytics
table produced by UsageProcessor.aggregate_to_grid_time() (see
processor.py), using a within-day, per-grid statistical baseline.

--------------------------------------------------------------------------
METHOD

1. Baseline: for each (grid_id, date), each hour's baseline is the
   LEAVE-ONE-OUT MEDIAN of that grid's total_activity across the day's
   other hours (the hour being scored is excluded from its own baseline,
   so a spiking hour can't inflate the very number it's compared against).
   Median (not mean) is used so one extreme hour elsewhere in the day
   doesn't drag the baseline up/down for every other hour.

2. Floor: grids/days with very low total daily activity produce
   meaningless ratios (a jump from 0.3 to 1.2 is "4x" but is noise, not a
   signal). The floor is DERIVED FROM THE DATA: the FLOOR_PERCENTILE-th
   percentile of grid-level daily total_activity (summed across all hours
   of that grid's day), computed from whatever data is loaded. Any
   (grid_id, date) whose daily total falls below that floor is excluded
   from all three rules. Documented rationale for percentile-of-daily-total
   (over percentile-of-hourly-baseline): it makes the exclusion decision
   once per grid/day rather than per hour, so a grid isn't "in" for most
   of the day and mysteriously "out" for a couple of hours just because
   its 23-point leave-one-out median happened to dip for those hours.

3. Partial-day handling: (grid_id, date) groups with fewer than
   EXPECTED_HOURS_PER_DAY (24) distinct hours get baseline_activity = NaN
   (flagged explicitly, not silently scored off a low-n median) and are
   excluded from alerting the same way floor-excluded groups are.

4. Three rules, each independently evaluated (a grid/hour CAN trigger more
   than one - each triggered rule produces its own alert record):
     - HIGH_ACTIVITY: current_activity >= HIGH_ACTIVITY_MULTIPLIER x baseline
     - ACTIVITY_DROP:  current_activity <= DROP_ACTIVITY_MULTIPLIER x baseline
     - ACTIVITY_SPIKE: current_activity >= SPIKE_MULTIPLIER x previous hour's
       activity (same grid, same day). Hour 0 has no preceding hour and is
       skipped. The previous hour's activity must itself be above an
       hourly-scale floor (floor_value / EXPECTED_HOURS_PER_DAY) before the
       spike ratio is evaluated - otherwise a previous hour of ~0 makes any
       positive current hour trivially "infinite x" and would falsely fire
       on noise. The daily-total floor_value can't be compared directly
       against a single hour's activity (wrong scale), hence the division.

--------------------------------------------------------------------------
WHAT THIS BASELINE CANNOT DISTINGUISH (read before wiring this into any
automated response)

- WHOLE-DAY ANOMALIES ARE INVISIBLE. The baseline is built entirely from
  that same day's other hours. If an entire grid is unusually busy or
  quiet ALL DAY (a real event, an outage, a pipeline bug), every hour
  looks "normal" relative to its neighbours - there is no day-over-day or
  week-over-week comparison here at all.
- NO CAUSE ATTRIBUTION. A HIGH_ACTIVITY alert cannot tell you whether it's
  a concert, a holiday, a network fault, a duplicate-ingestion bug, or a
  genuine incident. This is a pure statistical outlier flag.
- UNEVEN FALSE-POSITIVE RATES ACROSS GRID TYPES. A single global
  multiplier doesn't treat a naturally bursty downtown cell and a quiet
  residential cell equivalently. Even after the floor, some grids will
  alert more often simply because their normal hour-to-hour variance is
  higher, not because anything unusual happened.
- SMALL-SAMPLE NOISE. Each baseline is built from only 23 points.
  Borderline alerts near a threshold are close to a coin flip against
  ordinary variation, especially for grids just above the floor.
- NO SPATIAL CORRELATION. A real citywide event or network incident
  affecting many adjacent grids at once shows up as many independent
  single-grid alerts - nothing here recognises they're the same event.
- OVERLAPPING RULES MUDDY THE ALERT COUNT. Because a grid/hour can trigger
  multiple rule types, "alerts by type" is not the same as "grid/hours
  with a problem" - print_summary() reports both separately for this
  reason; don't conflate them downstream.
- DATA-QUALITY ARTEFACTS LOOK LIKE ANOMALIES. A dedup miss, a null-fill
  boundary effect, or a double-counted country_code slice upstream would
  shift a grid's numbers and be indistinguishable from a genuine event.
--------------------------------------------------------------------------
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


BASELINE_LIMITATIONS_STATEMENT = """\
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
"""


class AnomalyDetector:

    HIGH_ACTIVITY_MULTIPLIER = 2.0
    DROP_ACTIVITY_MULTIPLIER = 0.5
    SPIKE_MULTIPLIER = 3.0

    EXPECTED_HOURS_PER_DAY = 24
    FLOOR_PERCENTILE = 5

    def __init__(self, file_path=None, dataframe=None):

        self.file_path = file_path
        self.df = dataframe

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

        self.logger = logging.getLogger(__name__)

        self.floor_value = None
        self.alerts = None

    # ------------------------------------------------------------------ #
    def load_data(self):

        if self.df is not None:
            self.logger.info(
                f"Using provided DataFrame with {len(self.df)} rows"
            )
            return self.df

        if self.file_path is None:
            raise ValueError(
                "Either file_path or dataframe must be provided."
            )

        self.df = pd.read_csv(self.file_path)

        self.logger.info(
            f"Loaded {len(self.df)} rows from {self.file_path}"
        )

        return self.df

    # ------------------------------------------------------------------ #
    def calculate_baseline(self):

        required_columns = [
            "date",
            "grid_id",
            "hour",
            "total_activity"
        ]

        missing_columns = [
            col for col in required_columns
            if col not in self.df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {missing_columns}"
            )

        # Check that every grid/day has 24 distinct hourly intervals.
        # Groups with fewer are flagged: their leave-one-out median would
        # be built from very few points (e.g. 2-3), which is too noisy to
        # alert against reliably - baseline_activity is set to NaN for
        # those grid/hours instead of silently scoring them anyway. NaN
        # comparisons are False, so downstream rules naturally skip them.
        hour_counts = (
            self.df
            .groupby(["date", "grid_id"])["hour"]
            .nunique()
        )

        invalid_groups = hour_counts[hour_counts != self.EXPECTED_HOURS_PER_DAY]

        if not invalid_groups.empty:
            n_rows_affected = int(
                self.df.set_index(["date", "grid_id"]).index.isin(invalid_groups.index).sum()
            )
            self.logger.warning(
                f"{len(invalid_groups)} grid/day group(s) do not contain "
                f"exactly {self.EXPECTED_HOURS_PER_DAY} distinct hours "
                f"({n_rows_affected} row(s) affected) - baseline_activity "
                f"will be set to NaN for these and excluded from alerting."
            )

        def leave_one_out_median(values):

            n = values.shape[0]

            result = np.full(n, np.nan)

            if n < 2:
                return result

            # Sort the values
            order = np.argsort(values, kind="mergesort")

            sorted_vals = values[order]

            # rank[i] = position of values[i] in the sorted array
            rank = np.empty(n, dtype=np.int64)

            rank[order] = np.arange(n)

            # Number of values after removing the current observation
            m = n - 1

            if m % 2 == 1:

                # Odd number of remaining values
                r = m // 2

                pos = np.where(rank <= r, r + 1, r)

                result = sorted_vals[pos]

            else:

                # Even number of remaining values
                r1 = m // 2 - 1
                r2 = m // 2

                pos1 = np.where(rank <= r1, r1 + 1, r1)
                pos2 = np.where(rank <= r2, r2 + 1, r2)

                result = (
                    sorted_vals[pos1]
                    + sorted_vals[pos2]
                ) / 2.0

            return result

        # Calculate the baseline independently for every grid/day group
        self.df["baseline_activity"] = (
            self.df
            .groupby(["date", "grid_id"])["total_activity"]
            .transform(
                lambda values:
                leave_one_out_median(values.to_numpy())
            )
        )

        # explicit partial-day flag: NaN out baselines for any grid/day
        # that didn't have exactly 24 hours, even if n was >= 2
        if not invalid_groups.empty:
            invalid_mask = (
                self.df.set_index(["date", "grid_id"]).index.isin(invalid_groups.index)
            )
            self.df.loc[invalid_mask, "baseline_activity"] = np.nan

        self.logger.info(
            "Calculated within-day leave-one-out median baselines."
        )

        return self.df

    # ------------------------------------------------------------------ #
    def calculate_floor(self, percentile=None):
        """Derive the activity floor from the data: the given percentile
        (default FLOOR_PERCENTILE) of grid-level DAILY total_activity
        (summed across that grid's 24 hours). Any (grid_id, date) whose
        daily total falls below the floor is excluded from all three
        rules for every hour of that day - see module docstring for why
        the exclusion is made once per grid/day rather than per hour."""

        if "baseline_activity" not in self.df.columns:
            raise ValueError(
                "Run calculate_baseline() before calculate_floor()."
            )

        percentile = self.FLOOR_PERCENTILE if percentile is None else percentile

        daily_totals = (
            self.df
            .groupby(["grid_id", "date"])["total_activity"]
            .sum()
            .reset_index(name="grid_day_total_activity")
        )

        floor_value = float(np.percentile(daily_totals["grid_day_total_activity"], percentile))

        self.floor_value = floor_value
        self.floor_percentile = percentile
        # a scaled-down version of the same floor, for comparing against a
        # SINGLE HOUR's activity (used only by the spike guard below) -
        # floor_value itself is on the daily-total scale and would silence
        # nearly every legitimate spike if compared directly against one
        # hour's value
        self.hourly_floor_value = floor_value / self.EXPECTED_HOURS_PER_DAY

        self.df = self.df.merge(daily_totals, on=["grid_id", "date"], how="left")
        self.df["below_floor"] = self.df["grid_day_total_activity"] < floor_value

        n_groups_excluded = int((daily_totals["grid_day_total_activity"] < floor_value).sum())
        n_rows_excluded = int(self.df["below_floor"].sum())

        self.logger.info(
            f"Activity floor set at the {percentile}th percentile of "
            f"grid-level daily total_activity = {floor_value:.4f} "
            f"(hourly-scale equivalent for the spike guard = "
            f"{self.hourly_floor_value:.4f}). "
            f"{n_groups_excluded} grid/day group(s) "
            f"({n_rows_excluded} row(s)) fall below it and are excluded "
            f"from alerting."
        )

        return self.df

    # ------------------------------------------------------------------ #
    def apply_rules(self):
        """Compute per-row boolean flags for the three rules. A row can
        satisfy more than one rule at once - each is evaluated
        independently, not as a priority chain."""

        if self.floor_value is None:
            raise ValueError("Run calculate_floor() before apply_rules().")

        self.df = self.df.sort_values(["grid_id", "date", "hour"]).reset_index(drop=True)

        self.df["previous_hour_activity"] = (
            self.df.groupby(["grid_id", "date"])["total_activity"].shift(1)
        )

        eligible = (~self.df["below_floor"]) & self.df["baseline_activity"].notna()

        self.df["high_activity_flag"] = eligible & (
            self.df["total_activity"] >= self.HIGH_ACTIVITY_MULTIPLIER * self.df["baseline_activity"]
        )

        self.df["drop_flag"] = eligible & (
            self.df["total_activity"] <= self.DROP_ACTIVITY_MULTIPLIER * self.df["baseline_activity"]
        )

        # previous hour itself must be above the hourly-scale floor, or the
        # ratio is trivially satisfied by any positive current_activity
        # (see module docstring and calculate_floor())
        spike_eligible = (
            (~self.df["below_floor"])
            & self.df["previous_hour_activity"].notna()
            & (self.df["previous_hour_activity"] > self.hourly_floor_value)
        )

        self.df["spike_flag"] = spike_eligible & (
            self.df["total_activity"] >= self.SPIKE_MULTIPLIER * self.df["previous_hour_activity"]
        )

        self.logger.info(
            f"Rules applied: HIGH_ACTIVITY={int(self.df['high_activity_flag'].sum())}, "
            f"ACTIVITY_DROP={int(self.df['drop_flag'].sum())}, "
            f"ACTIVITY_SPIKE={int(self.df['spike_flag'].sum())} row(s) flagged."
        )

        return self.df

    # ------------------------------------------------------------------ #
    def generate_alerts(self):
        """Build the tidy alert-records table: one row per triggered rule,
        with grid_id, timestamp, alert_type, current_activity,
        baseline_activity and a human-readable reason."""

        required_flags = ["high_activity_flag", "drop_flag", "spike_flag"]
        missing = [c for c in required_flags if c not in self.df.columns]
        if missing:
            raise ValueError(f"Run apply_rules() before generate_alerts(). Missing: {missing}")

        if "timestamp" in self.df.columns:
            timestamps = pd.to_datetime(self.df["timestamp"],format="mixed")
        else:
            self.logger.warning(
                "No 'timestamp' column found - constructing one from "
                "'date' + 'hour' for alert records."
            )
            timestamps = pd.to_datetime(self.df["date"]) + pd.to_timedelta(self.df["hour"], unit="h")

        df = self.df.copy()
        df["_timestamp_for_alerts"] = timestamps

        records = []

        high = df[df["high_activity_flag"]]
        for _, row in high.iterrows():
            ratio = row["total_activity"] / row["baseline_activity"]
            records.append({
                "grid_id": row["grid_id"],
                "timestamp": row["_timestamp_for_alerts"],
                "alert_type": "HIGH_ACTIVITY",
                "current_activity": row["total_activity"],
                "baseline_activity": row["baseline_activity"],
                "reason": (
                    f"current activity {row['total_activity']:.2f} is {ratio:.2f}x "
                    f"the grid's within-day median baseline of "
                    f"{row['baseline_activity']:.2f} (threshold "
                    f"{self.HIGH_ACTIVITY_MULTIPLIER}x)"
                ),
            })

        drop = df[df["drop_flag"]]
        for _, row in drop.iterrows():
            ratio = row["total_activity"] / row["baseline_activity"] if row["baseline_activity"] else float("nan")
            records.append({
                "grid_id": row["grid_id"],
                "timestamp": row["_timestamp_for_alerts"],
                "alert_type": "ACTIVITY_DROP",
                "current_activity": row["total_activity"],
                "baseline_activity": row["baseline_activity"],
                "reason": (
                    f"current activity {row['total_activity']:.2f} is {ratio:.2f}x "
                    f"the grid's within-day median baseline of "
                    f"{row['baseline_activity']:.2f} (threshold "
                    f"{self.DROP_ACTIVITY_MULTIPLIER}x)"
                ),
            })

        spike = df[df["spike_flag"]]
        for _, row in spike.iterrows():
            ratio = row["total_activity"] / row["previous_hour_activity"]
            records.append({
                "grid_id": row["grid_id"],
                "timestamp": row["_timestamp_for_alerts"],
                "alert_type": "ACTIVITY_SPIKE",
                "current_activity": row["total_activity"],
                "baseline_activity": row["baseline_activity"],
                "reason": (
                    f"current activity {row['total_activity']:.2f} is {ratio:.2f}x "
                    f"the immediately preceding hour's activity of "
                    f"{row['previous_hour_activity']:.2f} (threshold "
                    f"{self.SPIKE_MULTIPLIER}x)"
                ),
            })

        alerts = pd.DataFrame.from_records(
            records,
            columns=[
                "grid_id", "timestamp", "alert_type",
                "current_activity", "baseline_activity", "reason"
            ],
        )

        if not alerts.empty:
            alerts = alerts.sort_values(["timestamp", "grid_id", "alert_type"]).reset_index(drop=True)

        self.alerts = alerts

        self.logger.info(f"Generated {len(alerts)} alert record(s).")

        return self.alerts

    # ------------------------------------------------------------------ #
    def export_alerts(self, output_path, fmt="csv"):

        if self.alerts is None:
            raise ValueError("Run generate_alerts() before export_alerts().")

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if fmt == "csv":
            self.alerts.to_csv(path, index=False)
        elif fmt == "json":
            self.alerts.to_json(path, orient="records", indent=2, date_format="iso")
        else:
            raise ValueError(f"Unsupported fmt: {fmt!r}. Use 'csv' or 'json'.")

        self.logger.info(f"Alerts exported to {path}")

        return str(path)

    # ------------------------------------------------------------------ #
    def print_summary(self):
        """Print a short operational summary: alerts by type, top 10 grids
        by alert count, and the proportion of ALL grid/hours (including
        floor- and partial-day-excluded ones) that alerted."""

        if self.alerts is None:
            raise ValueError("Run generate_alerts() before print_summary().")

        n_total_grid_hours = len(self.df)

        print("=" * 60)
        print("ANOMALY DETECTION - OPERATIONAL SUMMARY")
        print("=" * 60)

        if self.alerts.empty:
            print("No alerts generated.")
            print("=" * 60)
            return

        print(f"\nActivity floor: {self.floor_percentile}th percentile of "
              f"grid-level daily total_activity = {self.floor_value:.2f}")

        print("\nAlerts by type:")
        for alert_type, count in self.alerts["alert_type"].value_counts().items():
            print(f"  {alert_type}: {count}")

        print("\nTop 10 grids by alert count:")
        top_grids = self.alerts.groupby("grid_id").size().sort_values(ascending=False).head(10)
        for grid_id, count in top_grids.items():
            print(f"  grid {grid_id}: {count} alert(s)")

        # a grid/hour that triggered more than one rule is counted once here,
        # since this is "proportion of hours that alerted", not "alert count"
        alerted_grid_hours = self.alerts[["grid_id", "timestamp"]].drop_duplicates()
        n_alerted = len(alerted_grid_hours)
        proportion = n_alerted / n_total_grid_hours if n_total_grid_hours else 0.0

        print(
            f"\nGrid/hours that alerted: {n_alerted} / {n_total_grid_hours} "
            f"({proportion:.2%})"
        )
        print("=" * 60)

        self.logger.info(
            f"Summary printed: {len(self.alerts)} alert(s) across "
            f"{n_alerted} grid/hour(s) ({proportion:.2%} of {n_total_grid_hours})."
        )

    # ------------------------------------------------------------------ #
    def export_alert_summary(self, output_path):
        """Write the rule-based alert summary (same facts as
        print_summary()) as structured JSON, so it's a consumable artifact
        rather than only console output."""

        if self.alerts is None:
            raise ValueError("Run generate_alerts() before export_alert_summary().")

        n_total = len(self.df)
        alerted_grid_hours = self.alerts[["grid_id", "timestamp"]].drop_duplicates()
        n_alerted = len(alerted_grid_hours)
        proportion = n_alerted / n_total if n_total else 0.0

        top10 = (
            self.alerts.groupby("grid_id").size()
            .sort_values(ascending=False)
            .head(10)
        )

        summary = {
            "floor_percentile": self.floor_percentile,
            "floor_value": self.floor_value,
            "hourly_floor_value": self.hourly_floor_value,
            "total_grid_hours": n_total,
            "alerted_grid_hours": n_alerted,
            "alerted_proportion": proportion,
            "total_alert_records": int(len(self.alerts)),
            "alerts_by_type": {
                k: int(v) for k, v in self.alerts["alert_type"].value_counts().items()
            },
            "top_10_grids_by_alert_count": [
                {"grid_id": (int(gid) if pd.notna(gid) else gid), "alert_count": int(count)}
                for gid, count in top10.items()
            ],
            "rule_thresholds": {
                "HIGH_ACTIVITY_MULTIPLIER": self.HIGH_ACTIVITY_MULTIPLIER,
                "DROP_ACTIVITY_MULTIPLIER": self.DROP_ACTIVITY_MULTIPLIER,
                "SPIKE_MULTIPLIER": self.SPIKE_MULTIPLIER,
            },
        }

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(summary, f, indent=2, default=str)

        self.logger.info(f"Alert summary exported to {path}")

        return str(path)

    # ------------------------------------------------------------------ #
    def write_limitations_statement(self, output_path):
        """Write the within-day baseline's limitations as a standalone
        document, so it travels with the artifact rather than living only
        in code comments."""

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(BASELINE_LIMITATIONS_STATEMENT)

        self.logger.info(f"Baseline limitations statement written to {path}")

        return str(path)

    # ------------------------------------------------------------------ #
    def export_artifact_bundle(self, output_dir="processed/alerts", alerts_fmt="json"):
        """Produce the full serving artifact: network_alerts.<fmt>,
        alert_summary.json, baseline_limitations.md, and a manifest.json
        tying them together with generation metadata and the config that
        produced them - for downstream consumers (e.g. API3, RE4, or the
        Claude assistant) to read without re-deriving any of it."""

        output_dir = Path(output_dir)

        alerts_path = output_dir / f"network_alerts.{alerts_fmt}"
        summary_path = output_dir / "alert_summary.json"
        limitations_path = output_dir / "baseline_limitations.md"
        manifest_path = output_dir / "manifest.json"

        self.export_alerts(alerts_path, fmt=alerts_fmt)
        self.export_alert_summary(summary_path)
        self.write_limitations_statement(limitations_path)

        manifest = {
            "artifact": "network_alerts",
            "schema_version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "config": {
                "HIGH_ACTIVITY_MULTIPLIER": self.HIGH_ACTIVITY_MULTIPLIER,
                "DROP_ACTIVITY_MULTIPLIER": self.DROP_ACTIVITY_MULTIPLIER,
                "SPIKE_MULTIPLIER": self.SPIKE_MULTIPLIER,
                "floor_percentile": self.floor_percentile,
                "floor_value": self.floor_value,
                "hourly_floor_value": self.hourly_floor_value,
            },
            "files": {
                "alerts": alerts_path.name,
                "summary": summary_path.name,
                "limitations": limitations_path.name,
            },
            "total_grid_hours": len(self.df),
            "total_alerts": int(len(self.alerts)),
        }

        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2, default=str)

        self.logger.info(f"Artifact manifest written to {manifest_path}")

        self.print_summary()

        return {
            "alerts": str(alerts_path),
            "summary": str(summary_path),
            "limitations": str(limitations_path),
            "manifest": str(manifest_path),
        }

    # ------------------------------------------------------------------ #
    def run(self, output_dir="processed/alerts", fmt="json", floor_percentile=None):
        self.load_data()
        self.calculate_baseline()
        self.calculate_floor(floor_percentile)
        self.apply_rules()
        self.generate_alerts()
        return self.export_artifact_bundle(output_dir, alerts_fmt=fmt)


if __name__ == "__main__":

    # file_path = "../data_cleaning/processed/grid_hour/sms-call-internet-mi-2013-11-02_grid_hour.csv"

    # df = pd.read_csv(
    #     file_path,
    #     parse_dates=["timestamp"]
    # )

    # detector = AnomalyDetector(dataframe=df)

    # detector.run(
    #     output_dir="processed/alerts2/2013-11-01",
    #     fmt="json"
    # )

    import glob

    # AnomalyDetector operates on the grid/hour analytics table, which
    # UsageProcessor (processor.py) already produced and exported to
    # processed/grid_hour/*.csv - no need to re-read or re-clean the raw
    # sms-call-internet-mi-*.csv files again here.
    grid_hour_files = sorted(glob.glob("../data_cleaning/processed/grid_hour/*_grid_hour.csv"))

    if not grid_hour_files:
        raise FileNotFoundError(
            "No grid_hour files found under processed/grid_hour/ - run "
            "processor.py first to generate them."
        )

    grid_hour_frames = [
        pd.read_csv(f, parse_dates=["timestamp"]) for f in grid_hour_files
    ]

    # baseline stays within-day (grouped by date+grid_id) regardless of how
    # many days are pooled, but the floor is far more reliable computed
    # from a week's worth of grid/day samples than from a single day's -
    # and it produces one consistent threshold and one alerts file for the
    # week instead of 7
    weekly_grid_hour = pd.concat(grid_hour_frames, ignore_index=True)

    detector = AnomalyDetector(dataframe=weekly_grid_hour)
    detector.run("processed/alerts", fmt="json")

