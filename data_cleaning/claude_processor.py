import json
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


class UsageProcessor():

    def __init__(self, file_path=None, df=None):
        self.file_path = file_path
        self.df = df
        self.logger = logger

    def load_data(self):
        if self.df is not None:
            self.logger.info("using the df given")
            self._rows_loaded = len(self.df)
            return self.df
        if self.file_path is None:
            raise ValueError(
                "enter a valid file"
            )

        self.df = pd.read_csv(self.file_path)
        self._rows_loaded = len(self.df)

        self.logger.info(f"file has been loaded as CSV ({self._rows_loaded} rows)")

        return self.df

    def clean_data(self):

        required_columns = [
            "datetime",
            "CellID",
            "countrycode",
            "smsin",
            "smsout",
            "callin",
            "callout",
            "internet"
        ]

        activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet"
        ]

        missing_columns = [col for col in required_columns if col not in self.df.columns]

        if missing_columns:
            raise ValueError(
                f"there are missing columns in the input: {missing_columns}"
            )

        # keep an untouched snapshot of the raw layer (e.g. for raw-column
        # null counts in compute_kpis) before any renaming/cleaning happens
        self.raw_df = self.df.copy()

        mapping = {
            "datetime": "timestamp",
            "CellID": "grid_id",
            "countrycode": "country_code",
            "smsin": "sms_in",
            "smsout": "sms_out",
            "callin": "call_in",
            "callout": "call_out",
            "internet": "internet"
        }

        self.df = self.df.rename(columns=mapping)

        # datetime may arrive as an already-formatted string (e.g.
        # "2013-11-04 00:00:00") or as an epoch-millisecond integer,
        # depending on the source file. Only pass unit="ms" for the numeric
        # case - passing it for a numeric column that's really seconds/ms/ns
        # ambiguous, or applying it blindly, is how a column silently
        # collapses to 1970 with no error. Detect the dtype instead of
        # assuming one.
        if pd.api.types.is_numeric_dtype(self.df["timestamp"]):
            self.df["timestamp"] = pd.to_datetime(
                self.df["timestamp"], unit="ms", errors="coerce"
            )
        else:
            self.df["timestamp"] = pd.to_datetime(
                self.df["timestamp"], errors="coerce"
            )

        # sanity check: catch a wrong-unit/wrong-format parse even when it
        # doesn't produce NaT - e.g. epoch ms mistakenly read as ns lands
        # near 1970 instead of raising an error
        non_null_ts = self.df["timestamp"].dropna()
        if not non_null_ts.empty:
            min_year = non_null_ts.dt.year.min()
            if min_year < 2000:
                self.logger.warning(
                    f"Parsed timestamps include year {min_year} - this "
                    f"usually means the datetime unit/format was "
                    f"misdetected (e.g. epoch ms read as ns). Verify the "
                    f"source file's datetime format."
                )

        # coerce grid_id to numeric so blank/garbage identifiers become NaN
        # and get caught by the dropna below (a bare string wouldn't be)
        self.df["grid_id"] = pd.to_numeric(self.df["grid_id"], errors="coerce")

        rows_before = len(self.df)
        self.df = self.df.dropna(subset=["timestamp", "grid_id"])

        rows_dropped_identity = rows_before - len(self.df)

        self.logger.info(
            f"Dropped {rows_dropped_identity} rows due to missing timestamp/grid_id"
        )

        # blank activity measures
        rows_missing_activity = int(self.df[activity_columns].isna().any(axis=1).sum())
        if rows_missing_activity:
            self.logger.info(
                f"{rows_missing_activity} rows had blank activity measure(s); filling with 0"
            )

        negative_values = (
            self.df[activity_columns] < 0
        ).sum().sum()

        if negative_values > 0:
            self.logger.info(
                f"the number of negative values in the document is {negative_values}"
            )
            self.df[activity_columns] = (
                self.df[activity_columns].clip(lower=0)
            )

        self.df[activity_columns] = self.df[activity_columns].fillna(0)

        self.logger.info(
            "Null activity values filled with 0"
        )

        # exact duplicates
        rows_before_dupes = len(self.df)
        self.df = self.df.drop_duplicates(keep="first")
        rows_dropped_duplicates = rows_before_dupes - len(self.df)

        self.logger.info(
            f"Dropped {rows_dropped_duplicates} exact duplicate rows"
        )

        self.logger.info(
            f"Rows after cleaning: {len(self.df)}"
        )

        self._counts = {
            "rows_loaded": self._rows_loaded,
            "rows_dropped_missing_identity": rows_dropped_identity,
            "rows_with_missing_activity_filled": rows_missing_activity,
            "negative_activity_cells_clipped": int(negative_values),
            "rows_dropped_duplicates": rows_dropped_duplicates,
        }

        return self.df

    def derive_time_features(self):

        if "timestamp" not in self.df.columns:
            raise ValueError(
                "timestamp isnt in the dataframe"
            )

        # verify hourly cadence before slicing the timestamp into parts
        unique_timestamps = pd.Series(sorted(self.df["timestamp"].unique()))
        diffs = unique_timestamps.diff().dropna().unique()
        is_hourly_spacing = len(diffs) == 1 and pd.Timedelta(diffs[0]) == pd.Timedelta(hours=1)
        n_unique_timestamps = int(unique_timestamps.nunique())
        is_24_points = n_unique_timestamps == 24

        self.cadence_summary = {
            "unique_timestamps": n_unique_timestamps,
            "is_hourly_spacing": bool(is_hourly_spacing),
            "is_24_points": bool(is_24_points),
            "cadence_ok": bool(is_hourly_spacing and is_24_points),
        }

        if self.cadence_summary["cadence_ok"]:
            self.logger.info(
                f"Cadence verified: {n_unique_timestamps} distinct hourly "
                f"timestamps, all 1 hour apart"
            )
        else:
            self.logger.warning(
                f"Cadence check failed: {n_unique_timestamps} distinct "
                f"timestamps (expected 24), uniform 1-hour spacing="
                f"{is_hourly_spacing}"
            )

        self.df["date"] = self.df["timestamp"].dt.date
        self.df["hour"] = self.df["timestamp"].dt.hour
        self.df["day_of_week"] = self.df["timestamp"].dt.dayofweek

        self.logger.info("derived date, hour, day_of_week")

        return self.df

    def inspect_grain(self):
        """Confirm the raw grain is timestamp + grid_id + country_code by
        counting how many country-code rows exist for the same grid/hour."""

        if "country_code" not in self.df.columns:
            raise ValueError("country_code isnt in the dataframe")

        rows_per_grid_hour = self.df.groupby(["timestamp", "grid_id"])["country_code"].count()

        self.grain_stats = {
            "avg_country_rows_per_grid_hour": float(rows_per_grid_hour.mean()),
            "max_country_rows_per_grid_hour": int(rows_per_grid_hour.max()),
            "grain_confirmed": bool(rows_per_grid_hour.max() > 1),
        }

        self.logger.info(
            f"Grain check: avg {self.grain_stats['avg_country_rows_per_grid_hour']:.2f} "
            f"and max {self.grain_stats['max_country_rows_per_grid_hour']} "
            f"country-code rows per (timestamp, grid_id) -> raw grain is "
            f"timestamp + grid_id + country_code = "
            f"{self.grain_stats['grain_confirmed']}"
        )

        return self.grain_stats

    def aggregate_to_grid_time(self):
        self.grid_hour_df = self.df.groupby(
            ["timestamp", "grid_id"], as_index=False
        ).agg({
            "sms_in": "sum",
            "sms_out": "sum",
            "call_in": "sum",
            "call_out": "sum",
            "internet": "sum"
        })

        self.grid_hour_df["date"] = (
            self.grid_hour_df["timestamp"].dt.date
        )

        self.grid_hour_df["hour"] = (
            self.grid_hour_df["timestamp"].dt.hour
        )

        self.grid_hour_df["day_of_week"] = (
            self.grid_hour_df["timestamp"].dt.dayofweek
        )

        self.logger.info(
            f"Aggregated {len(self.df)} raw records into "
            f"{len(self.grid_hour_df)} grid-hour records "
            f"(country_code collapsed out)"
        )

        return self.grid_hour_df

    def derive_activity_features(self):
        # runs on grid_hour_df (one record per grid/hour) so these are the
        # clearly-labelled derived analytics measures, computed exactly once
        if not hasattr(self, "grid_hour_df"):
            raise ValueError("Run aggregate_to_grid_time() before derive_activity_features().")

        self.grid_hour_df["total_sms"] = (
            self.grid_hour_df["sms_in"] + self.grid_hour_df["sms_out"]
        )
        self.grid_hour_df["total_calls"] = (
            self.grid_hour_df["call_in"] + self.grid_hour_df["call_out"]
        )
        self.grid_hour_df["total_activity"] = (
            self.grid_hour_df["total_sms"]
            + self.grid_hour_df["total_calls"]
            + self.grid_hour_df["internet"]
        )

        self.logger.info(
            "Derived total_sms, total_calls and total_activity"
        )

        return self.grid_hour_df

    def compute_kpis(self):
        """Profiling facts for this single file: unique grids, time range,
        cadence, country-code categories, busiest hour, busiest grid, and
        null counts per raw column."""

        if not hasattr(self, "grid_hour_df"):
            raise ValueError("theres no object called grid_hour_df")

        busiest_hour_row = (
            self.grid_hour_df.groupby("hour", as_index=False)["total_activity"].sum()
            .sort_values("total_activity", ascending=False)
            .iloc[0]
        )
        busiest_grid_row = (
            self.grid_hour_df.groupby("grid_id", as_index=False)["total_activity"].sum()
            .sort_values("total_activity", ascending=False)
            .iloc[0]
        )

        self.kpis = {
            "file": self.file_path,
            "unique_grids": int(self.df["grid_id"].nunique()),
            "time_range_start": str(self.df["timestamp"].min()),
            "time_range_end": str(self.df["timestamp"].max()),
            "unique_timestamps": self.cadence_summary["unique_timestamps"],
            "cadence_ok": self.cadence_summary["cadence_ok"],
            "country_code_categories": int(self.df["country_code"].nunique()),
            "avg_country_rows_per_grid_hour": self.grain_stats["avg_country_rows_per_grid_hour"],
            "max_country_rows_per_grid_hour": self.grain_stats["max_country_rows_per_grid_hour"],
            "raw_grain_confirmed": self.grain_stats["grain_confirmed"],
            "busiest_hour": int(busiest_hour_row["hour"]),
            "busiest_hour_total_activity": float(busiest_hour_row["total_activity"]),
            "busiest_grid_id": int(busiest_grid_row["grid_id"]),
            "busiest_grid_total_activity": float(busiest_grid_row["total_activity"]),
            "null_counts_per_raw_column": {
                col: int(self.raw_df[col].isna().sum()) for col in self.raw_df.columns
            },
        }
        self.kpis.update(self._counts)

        self.logger.info(f"Computed {len(self.kpis)} KPI fields")

        return self.kpis

    def export_summary(self, output_dir="dataset_processed"):
        """Per-file export: cleaned data, grid-hour analytics, and this
        file's KPI profile. Cross-file summaries (daily/grid) are written
        once, after the whole batch, via export_combined_summaries()."""

        file_name = Path(self.file_path).stem if self.file_path else "input_dataframe"

        if not hasattr(self, "grid_hour_df"):
            raise ValueError(
                "Run aggregate_to_grid_time() before exporting."
            )

        if not hasattr(self, "kpis"):
            raise ValueError(
                "Run compute_kpis() before exporting."
            )

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        cleaned_path = output_path / "cleaned" / f"{file_name}_cleaned_data.csv"
        cleaned_path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_csv(cleaned_path, index=False)

        grid_hour_path = output_path / "grid_hour" / f"{file_name}_grid_hour.csv"
        grid_hour_path.parent.mkdir(parents=True, exist_ok=True)
        self.grid_hour_df.to_csv(grid_hour_path, index=False)

        kpis_path = output_path / "kpis" / f"{file_name}_kpis.json"
        kpis_path.parent.mkdir(parents=True, exist_ok=True)
        with open(kpis_path, "w") as f:
            json.dump(self.kpis, f, indent=2, default=str)

        self.logger.info(
            f"Cleaned data exported to {cleaned_path}"
        )
        self.logger.info(
            f"Grid-hour data exported to {grid_hour_path}"
        )
        self.logger.info(
            f"KPIs exported to {kpis_path}"
        )

        return {
            "cleaned": str(cleaned_path),
            "grid_hour": str(grid_hour_path),
            "kpis": str(kpis_path),
        }


def export_combined_summaries(grid_hour_frames, kpis_list=None, output_dir="dataset_processed"):
    """Combine grid_hour_df across every processed file into a SINGLE daily
    summary and a SINGLE grid summary (instead of one pair per input file).
    Optionally also writes one combined profiling_summary.csv, one row per
    file, from the list of per-file KPI dicts."""

    combined = pd.concat(grid_hour_frames, ignore_index=True)

    daily_summary = combined.groupby("date", as_index=False).agg(
        total_sms=("total_sms", "sum"),
        total_calls=("total_calls", "sum"),
        internet=("internet", "sum"),
        total_activity=("total_activity", "sum")
    )

    grid_summary = combined.groupby("grid_id", as_index=False).agg(
        total_sms=("total_sms", "sum"),
        total_calls=("total_calls", "sum"),
        internet=("internet", "sum"),
        total_activity=("total_activity", "sum")
    )

    output_path = Path(output_dir)

    daily_path = output_path / "daily_summary" / "daily_summary.csv"
    daily_path.parent.mkdir(parents=True, exist_ok=True)
    daily_summary.to_csv(daily_path, index=False)

    grid_path = output_path / "grid_summary" / "grid_summary.csv"
    grid_path.parent.mkdir(parents=True, exist_ok=True)
    grid_summary.to_csv(grid_path, index=False)

    result = {
        "daily_summary": str(daily_path),
        "grid_summary": str(grid_path),
    }

    logger.info(
        f"Combined daily summary ({len(daily_summary)} rows across "
        f"{len(grid_hour_frames)} files) exported to {daily_path}"
    )
    logger.info(
        f"Combined grid summary ({len(grid_summary)} rows across "
        f"{len(grid_hour_frames)} files) exported to {grid_path}"
    )

    if kpis_list:
        profiling_summary = pd.DataFrame(
            [{k: v for k, v in kpi.items() if k != "null_counts_per_raw_column"} for kpi in kpis_list]
        )
        profiling_path = output_path / "profiling_summary" / "profiling_summary.csv"
        profiling_path.parent.mkdir(parents=True, exist_ok=True)
        profiling_summary.to_csv(profiling_path, index=False)
        result["profiling_summary"] = str(profiling_path)
        logger.info(
            f"Combined profiling summary ({len(profiling_summary)} rows) "
            f"exported to {profiling_path}"
        )

    return result


if __name__ == "__main__":

    files = [
        "../data_set/sms-call-internet-mi-2013-11-01.csv",
        "../data_set/sms-call-internet-mi-2013-11-02.csv",
        "../data_set/sms-call-internet-mi-2013-11-03.csv",
        "../data_set/sms-call-internet-mi-2013-11-04.csv",
        "../data_set/sms-call-internet-mi-2013-11-05.csv",
        "../data_set/sms-call-internet-mi-2013-11-06.csv",
        "../data_set/sms-call-internet-mi-2013-11-07.csv"
    ]

    grid_hour_frames = []
    kpis_list = []

    for file in files:

        processor = UsageProcessor(file_path=file)

        processor.load_data()
        processor.clean_data()
        processor.derive_time_features()
        processor.inspect_grain()
        processor.aggregate_to_grid_time()
        processor.derive_activity_features()
        processor.compute_kpis()
        processor.export_summary("processed")

        grid_hour_frames.append(processor.grid_hour_df)
        kpis_list.append(processor.kpis)

    # one combined daily_summary.csv, one combined grid_summary.csv,
    # one combined profiling_summary.csv - not 7 of each
    export_combined_summaries(grid_hour_frames, kpis_list, "processed")