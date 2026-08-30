import pandas as pd
import logging
from pathlib import Path


class UsageProcessor():

    def __init__(self, file_path=None, df=None):
        self.file_path = file_path
        self.df = df

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

        self.logger = logging.getLogger(__name__)

    def load_data(self):
        if self.df is not None:
            self.logger.info("using the df given")
            return self.df
        if self.file_path is None:
            raise ValueError(
                "enter a valid file"
            )

        self.df = pd.read_csv(self.file_path)

        self.logger.info("file has been loaded as CSV")

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

        self.df["timestamp"] = pd.to_datetime(
            self.df["timestamp"],
            errors="coerce"
        )

        rows_before = len(self.df)
        self.df = self.df.dropna(subset=["timestamp", "grid_id"])

        rows_dropped = rows_before - len(self.df)

        self.logger.info(
            f"Dropped {rows_dropped} rows due to missing timestamp/grid_id"
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
            f"Rows after cleaning: {len(self.df)}"
        )

        self.logger.info(
            "Null activity values filled with 0"
        )

        return self.df

    def derive_time_features(self):

        if "timestamp" not in self.df.columns:
            raise ValueError(
                "timestamp isnt in the dataframe"
            )

        self.df["date"] = self.df["timestamp"].dt.date
        self.df["hour"] = self.df["timestamp"].dt.hour
        self.df["day_of_week"] = self.df["timestamp"].dt.dayofweek

        self.logger.info("derived date, hour, day_of_week")

        return self.df

    def derive_activity_features(self):

        self.df["total_sms"] = (self.df["sms_in"] + self.df["sms_out"])
        self.df["total_calls"] = (self.df["call_in"] + self.df["call_out"])
        self.df["total_activity"] = (self.df["total_sms"] + self.df["total_calls"]) + self.df["internet"]

        self.logger.info(
            "Derived total_sms, total_calls and total_activity"
        )

        return self.df

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

        self.grid_hour_df["total_sms"] = (
            self.grid_hour_df["sms_in"]
            + self.grid_hour_df["sms_out"]
        )

        self.grid_hour_df["total_calls"] = (
            self.grid_hour_df["call_in"]
            + self.grid_hour_df["call_out"]
        )

        self.grid_hour_df["total_activity"] = (
            self.grid_hour_df["total_sms"]
            + self.grid_hour_df["total_calls"]
            + self.grid_hour_df["internet"]
        )

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
            f"{len(self.grid_hour_df)} grid-hour records"
        )

        return self.grid_hour_df

    def compute_kpis(self):
        if not hasattr(self, "grid_hour_df"):
            raise ValueError("theres no object called grid_hour_df")

        self.daily_summary = (
            self.grid_hour_df.groupby("date", as_index=False)
        ).agg(
            total_sms=("total_sms", "sum"),
            total_calls=("total_calls", "sum"),
            internet=("internet", "sum"),
            total_activity=("total_activity", "sum")
        )

        self.grid_summary = (
            self.grid_hour_df.groupby("grid_id", as_index=False)
        ).agg(
            total_sms=("total_sms", "sum"),
            total_calls=("total_calls", "sum"),
            internet=("internet", "sum"),
            total_activity=("total_activity", "sum")
        )

        self.logger.info(
            f"Generated {len(self.daily_summary)} daily KPI records"
        )

        self.logger.info(
            f"Generated {len(self.grid_summary)} grid KPI records"
        )

        return self.daily_summary, self.grid_summary

    def export_summary(self, output_dir="dataset_processed"):
        file_name = Path(self.file_path).stem
        if not hasattr(self, "grid_hour_df"):
            raise ValueError(
                "Run aggregate_to_grid_time() before exporting."
            )

        if not hasattr(self, "daily_summary") or not hasattr(self, "grid_summary"):
            raise ValueError(
                "Run compute_kpis() before exporting."
            )

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        cleaned_path = output_path / "cleaned" / f"{file_name}_cleaned_data.csv"
        self.df.to_csv(cleaned_path, index=False)

        # Export grid-hour analytics
        grid_hour_path = output_path / "grid_hour" / f"{file_name}_grid_hour.csv"
        self.grid_hour_df.to_csv(grid_hour_path, index=False)

        # Export daily KPI summary
        daily_path = output_path / "daily_summary" / f"{file_name}_daily_summary.csv"
        self.daily_summary.to_csv(daily_path, index=False)

        # Export grid KPI summary
        grid_path = output_path / "grid_summary" / f"{file_name}_grid_summary.csv"
        self.grid_summary.to_csv(grid_path, index=False)

        # Logging
        self.logger.info(
            f"Grid-hour data exported to {grid_hour_path}"
        )

        self.logger.info(
            f"Daily summary exported to {daily_path}"
        )

        self.logger.info(
            f"Grid summary exported to {grid_path}"
        )

        return {
            "grid_hour": str(grid_hour_path),
            "daily_summary": str(daily_path),
            "grid_summary": str(grid_path)
        }

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

    for file in files:

        processor = UsageProcessor(file_path=file)

        processor.load_data()
        processor.clean_data()
        processor.derive_time_features()
        processor.derive_activity_features()
        processor.aggregate_to_grid_time()
        processor.compute_kpis()
        processor.export_summary("processed")