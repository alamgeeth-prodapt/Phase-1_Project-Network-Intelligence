from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    lit,
    when,
    to_timestamp,
    to_date,
    hour,
    dayofweek,
    sum as spark_sum,
    max as spark_max,
    round as spark_round,
    desc
)

import os

os.environ["HADOOP_HOME"] = r"D:\hadoop"
os.environ["PATH"] = r"D:\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession

import glob
import pandas as pd



class SparkCleaning:

    def __init__(self, df):

        self.df = df

        # Keep the original row count before ANY cleaning
        self.raw_count = df.count()

        self.rejected_df = None
        self.clean_network_df = None

        # NEW
        self.hourly_grid_summary = None

        self.rejected_summary = {}
        self.null_handling_report = {}

    # =============================================================
    # 1. RENAME RAW COLUMNS TO CANONICAL NAMES
    # =============================================================

    def canonicalize_columns(self):

        self.df = (
            self.df
            .withColumnRenamed("datetime", "timestamp")
            .withColumnRenamed("CellID", "grid_id")
            .withColumnRenamed("countrycode", "country_code")
            .withColumnRenamed("smsin", "sms_in")
            .withColumnRenamed("smsout", "sms_out")
            .withColumnRenamed("callin", "call_in")
            .withColumnRenamed("callout", "call_out")
        )

        return self.df

    # =============================================================
    # 2. CAST TIMESTAMP AND ACTIVITY MEASURES
    # =============================================================

    def cast_columns(self):

        self.df = (
            self.df

            .withColumn(
                "timestamp",
                to_timestamp("timestamp")
            )

            .withColumn(
                "grid_id",
                col("grid_id").cast("integer")
            )

            .withColumn(
                "country_code",
                col("country_code").cast("integer")
            )

            .withColumn(
                "sms_in",
                col("sms_in").cast("double")
            )

            .withColumn(
                "sms_out",
                col("sms_out").cast("double")
            )

            .withColumn(
                "call_in",
                col("call_in").cast("double")
            )

            .withColumn(
                "call_out",
                col("call_out").cast("double")
            )

            .withColumn(
                "internet",
                col("internet").cast("double")
            )
        )

        return self.df

    # =============================================================
    # 3. PROFILE NULL ACTIVITY MEASURES
    # =============================================================

    def profile_activity_nulls(self):

        activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet"
        ]

        null_counts = {}

        for column in activity_columns:

            count = (
                self.df
                .filter(col(column).isNull())
                .count()
            )

            null_counts[column] = count

        self.null_handling_report["before_null_to_zero"] = null_counts

        return null_counts

    # =============================================================
    # 4. QUARANTINE INVALID RECORDS
    # =============================================================

    def quarantine_invalid_records(self):

        invalid_condition = (
            col("grid_id").isNull()
            | col("timestamp").isNull()

            | (coalesce(col("sms_in"), lit(0.0)) < 0)
            | (coalesce(col("sms_out"), lit(0.0)) < 0)
            | (coalesce(col("call_in"), lit(0.0)) < 0)
            | (coalesce(col("call_out"), lit(0.0)) < 0)
            | (coalesce(col("internet"), lit(0.0)) < 0)
        )

        self.rejected_df = (
            self.df
            .filter(invalid_condition)
        )

        rejected_count = self.rejected_df.count()

        self.rejected_summary["raw_record_count"] = self.raw_count
        self.rejected_summary["rejected_record_count"] = rejected_count
        self.rejected_summary["accepted_record_count"] = (
            self.raw_count - rejected_count
        )

        # Keep only trusted records
        self.clean_network_df = (
            self.df
            .filter(~invalid_condition)
        )

        return self.clean_network_df

    # =============================================================
    # 5. CURATED-LAYER NULL -> ZERO
    # =============================================================

    def apply_null_to_zero(self):

        activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet"
        ]

        handled_nulls = 0

        for column in activity_columns:

            null_count = (
                self.clean_network_df
                .filter(col(column).isNull())
                .count()
            )

            handled_nulls += null_count

            self.clean_network_df = (
                self.clean_network_df
                .withColumn(
                    column,
                    when(
                        col(column).isNull(),
                        0.0
                    ).otherwise(col(column))
                )
            )

        self.null_handling_report["total_activity_nulls_handled"] = (
            handled_nulls
        )

        return self.clean_network_df

    # =============================================================
    # 6. CREATE DERIVED ACTIVITY MEASURES
    # =============================================================

    def create_activity_metrics(self):

        self.clean_network_df = (
            self.clean_network_df

            .withColumn(
                "total_sms",
                col("sms_in") + col("sms_out")
            )

            .withColumn(
                "total_calls",
                col("call_in") + col("call_out")
            )

            .withColumn(
                "total_activity",
                col("sms_in")
                + col("sms_out")
                + col("call_in")
                + col("call_out")
                + col("internet")
            )
        )

        return self.clean_network_df

    # =============================================================
    # 7. DERIVE DATE / HOUR / DAY OF WEEK
    # =============================================================

    def derive_time_features(self):

        self.clean_network_df = (
            self.clean_network_df

            .withColumn(
                "date",
                to_date("timestamp")
            )

            .withColumn(
                "hour",
                hour("timestamp")
            )

            .withColumn(
                "day_of_week",
                dayofweek("timestamp")
            )
        )

        return self.clean_network_df

    # =============================================================
    # 8. VERIFY HOURLY CADENCE
    # =============================================================

    def verify_hourly_cadence(self):

        cadence = (
            self.clean_network_df
            .select("date", "hour")
            .distinct()
            .orderBy("date", "hour")
        )

        print("\n--- HOURLY CADENCE ---")

        cadence.groupBy("date").count().orderBy("date").show()

        return cadence

    # =============================================================
    # 9. COLLAPSE COUNTRY-CODE RECORDS
    #
    # RAW STRUCTURE:
    #
    # grid_id | timestamp | country_code | sms_in | ...
    #
    # CANONICAL STRUCTURE:
    #
    # grid_id | timestamp | sms_in | ...
    #
    # One row per grid + timestamp.
    # =============================================================

    def create_hourly_grid_summary(self):

        print("\n--- CREATING HOURLY GRID SUMMARY ---")

        # Keep count before aggregation for validation
        clean_count = self.clean_network_df.count()

        # ---------------------------------------------------------
        # Aggregate all country-code records belonging to the
        # same grid and timestamp.
        # ---------------------------------------------------------

        hourly_grid_summary = (
            self.clean_network_df
            .groupBy(
                "grid_id",
                "timestamp"
            )
            .agg(
                spark_sum("sms_in").alias("sms_in"),
                spark_sum("sms_out").alias("sms_out"),
                spark_sum("call_in").alias("call_in"),
                spark_sum("call_out").alias("call_out"),
                spark_sum("internet").alias("internet_activity")
            )
        )

        # ---------------------------------------------------------
        # Derived hourly metrics
        # ---------------------------------------------------------

        hourly_grid_summary = (
            hourly_grid_summary

            .withColumn(
                "total_sms",
                col("sms_in") + col("sms_out")
            )

            .withColumn(
                "total_calls",
                col("call_in") + col("call_out")
            )

            .withColumn(
                "total_activity",
                col("total_sms")
                + col("total_calls")
                + col("internet_activity")
            )

            .withColumn(
                "date",
                to_date("timestamp")
            )

            .withColumn(
                "hour",
                hour("timestamp")
            )

            .withColumn(
                "day_of_week",
                dayofweek("timestamp")
            )
        )

        # ---------------------------------------------------------
        # Daily activity per grid
        #
        # This calculates the total activity for a grid across
        # the entire day and attaches it to every hourly row.
        # ---------------------------------------------------------

        daily_activity = (
            hourly_grid_summary
            .groupBy(
                "grid_id",
                "date"
            )
            .agg(
                spark_sum("total_activity").alias("daily_activity")
            )
        )

        hourly_grid_summary = (
            hourly_grid_summary
            .join(
                daily_activity,
                on=["grid_id", "date"],
                how="left"
            )
        )

        # ---------------------------------------------------------
        # Internet share of total activity
        #
        # internet / total activity
        #
        # If total activity is zero, return 0 instead of NULL.
        # ---------------------------------------------------------

        hourly_grid_summary = (
            hourly_grid_summary
            .withColumn(
                "internet_share",
                when(
                    col("total_activity") == 0,
                    lit(0.0)
                )
                .otherwise(
                    col("internet_activity") / col("total_activity")
                )
            )
        )

        # ---------------------------------------------------------
        # Store canonical downstream DataFrame
        # ---------------------------------------------------------

        self.hourly_grid_summary = hourly_grid_summary

        # =========================================================
        # REQUIRED ASSERTIONS
        # =========================================================

        print("\n--- HOURLY GRID SUMMARY VALIDATION ---")

        # ---------------------------------------------------------
        # REQUIREMENT 1:
        # Zero duplicates on (grid_id, timestamp)
        # ---------------------------------------------------------

        duplicate_count = (
            self.hourly_grid_summary
            .groupBy("grid_id", "timestamp")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        assert duplicate_count == 0, (
            f"Duplicate grid/hour records found: {duplicate_count}"
        )

        print("PASS: Zero duplicates on (grid_id, timestamp)")

        # ---------------------------------------------------------
        # REQUIREMENT 2:
        # hourly_grid_summary row count must be strictly less
        # than clean_network_df row count.
        # ---------------------------------------------------------

        hourly_count = self.hourly_grid_summary.count()

        assert hourly_count < clean_count, (
            "hourly_grid_summary row count is not strictly "
            "less than clean_network_df row count."
        )

        print(
            "PASS: hourly_grid_summary row count "
            f"({hourly_count}) < clean_network_df "
            f"row count ({clean_count})"
        )

        # ---------------------------------------------------------
        # REQUIREMENT 3:
        # Row count <= D × 24 × 10000
        #
        # D = number of distinct dates in the cleaned data.
        # 24 = hours per day.
        # 10000 = maximum supported grids.
        # ---------------------------------------------------------

        number_of_days = (
            self.clean_network_df
            .select("date")
            .distinct()
            .count()
        )

        maximum_allowed_rows = (
            number_of_days * 24 * 10000
        )

        assert hourly_count <= maximum_allowed_rows, (
            f"Row count {hourly_count} exceeds maximum allowed "
            f"count {maximum_allowed_rows} "
            f"(D={number_of_days})"
        )

        print(
            "PASS: Row count <= D × 24 × 10000 "
            f"({hourly_count} <= {maximum_allowed_rows})"
        )

        # ---------------------------------------------------------
        # REQUIREMENT 4:
        # Hand-check one grid/hour.
        #
        # Take one actual grid/hour from the cleaned data and
        # independently calculate the raw aggregation.
        # ---------------------------------------------------------

        sample_key = (
            self.clean_network_df
            .select("grid_id", "timestamp")
            .dropDuplicates()
            .limit(1)
            .collect()
        )

        assert len(sample_key) == 1, (
            "Unable to select a grid/hour for hand-check."
        )

        sample_grid = sample_key[0]["grid_id"]
        sample_timestamp = sample_key[0]["timestamp"]

        # Independent aggregation from clean_network_df
        hand_check = (
            self.clean_network_df
            .filter(
                (col("grid_id") == sample_grid)
                & (col("timestamp") == sample_timestamp)
            )
            .agg(
                spark_sum("sms_in").alias("sms_in"),
                spark_sum("sms_out").alias("sms_out"),
                spark_sum("call_in").alias("call_in"),
                spark_sum("call_out").alias("call_out"),
                spark_sum("internet").alias("internet_activity")
            )
            .collect()[0]
        )

        # Value from canonical summary
        summary_check = (
            self.hourly_grid_summary
            .filter(
                (col("grid_id") == sample_grid)
                & (col("timestamp") == sample_timestamp)
            )
            .collect()[0]
        )

        # Compare every aggregated measure
        measures = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity"
        ]

        for measure in measures:

            expected = hand_check[measure]
            actual = summary_check[measure]

            assert expected == actual, (
                f"Hand-check failed for {measure}: "
                f"expected={expected}, actual={actual}"
            )

        print(
            "PASS: Hand-check reproduced exactly for "
            f"grid_id={sample_grid}, "
            f"timestamp={sample_timestamp}"
        )

        # ---------------------------------------------------------
        # REQUIREMENT 5:
        # country_code must NOT exist.
        # ---------------------------------------------------------

        assert "country_code" not in (
            self.hourly_grid_summary.columns
        ), (
            "country_code must not appear in hourly_grid_summary."
        )

        print(
            "PASS: country_code does not appear in "
            "hourly_grid_summary"
        )

        # ---------------------------------------------------------
        # Final validation summary
        # ---------------------------------------------------------

        print("\n--- SUMMARY VALIDATION ---")

        print(f"Clean records       : {clean_count}")
        print(f"Hourly summary rows : {hourly_count}")
        print(f"Number of days      : {number_of_days}")
        print(f"Maximum allowed     : {maximum_allowed_rows}")

        return self.hourly_grid_summary

    # =============================================================
    # 10. TOP TEN HIGH-ACTIVITY GRIDS
    #
    # start_timestamp / end_timestamp are optional.
    #
    # If supplied, only that window is considered.
    # =============================================================

    def top_ten_high_activity_grids(
        self,
        start_timestamp=None,
        end_timestamp=None
    ):

        df = self.hourly_grid_summary

        if start_timestamp is not None:

            df = df.filter(
                col("timestamp") >= to_timestamp(
                    lit(start_timestamp)
                )
            )

        if end_timestamp is not None:

            df = df.filter(
                col("timestamp") <= to_timestamp(
                    lit(end_timestamp)
                )
            )

        top_ten = (
            df
            .groupBy("grid_id")
            .agg(
                spark_sum("total_activity")
                .alias("window_total_activity")
            )
            .orderBy(
                desc("window_total_activity")
            )
            .limit(10)
        )

        print("\n--- TOP 10 HIGH-ACTIVITY GRIDS ---")

        top_ten.show(truncate=False)

        return top_ten

    # =============================================================
    # 11. PEAK ACTIVITY HOUR
    #
    # Overall peak hour across all grids.
    # =============================================================

    def compute_peak_activity_hour(self):

        peak_hour = (
            self.hourly_grid_summary
            .groupBy("timestamp", "hour")
            .agg(
                spark_sum("total_activity")
                .alias("total_activity")
            )
            .orderBy(
                desc("total_activity")
            )
            .limit(1)
        )

        print("\n--- PEAK ACTIVITY HOUR ---")

        peak_hour.show(truncate=False)

        return peak_hour

    # =============================================================
    # 12. RUN SP2
    # =============================================================

    def run(self):

        self.canonicalize_columns()
        print("After canonicalize:", self.df.count())

        self.cast_columns()
        print("After cast:", self.df.count())

        self.profile_activity_nulls()
        print("After null profiling:", self.df.count())

        self.quarantine_invalid_records()

        print(
            "After quarantine:",
            self.clean_network_df.count()
        )

        print(
            "Rejected:",
            self.rejected_df.count()
        )

        self.apply_null_to_zero()

        print(
            "After null → zero:",
            self.clean_network_df.count()
        )

        self.create_activity_metrics()

        print(
            "After activity metrics:",
            self.clean_network_df.count()
        )

        self.derive_time_features()

        print(
            "After time features:",
            self.clean_network_df.count()
        )

        self.verify_hourly_cadence()

        print(
            "After cadence:",
            self.clean_network_df.count()
        )

        self.final_profile()

        # =========================================================
        # NEW ANALYTICS / AGGREGATION LAYER
        # =========================================================

        self.create_hourly_grid_summary()

        # Example analytics
        self.top_ten_high_activity_grids()

        self.compute_peak_activity_hour()

        return (
            self.clean_network_df,
            self.hourly_grid_summary,
            self.rejected_df,
            self.rejected_summary,
            self.null_handling_report,
        )

    # =============================================================
    # 13. FINAL PROFILE
    # =============================================================

    def final_profile(self):

        final_count = self.clean_network_df.count()

        self.rejected_summary["final_record_count"] = final_count

        print("\n" + "=" * 60)
        print("SP2 - CLEANING SUMMARY")
        print("=" * 60)

        print(
            f"Records before cleaning : {self.raw_count}"
        )

        print(
            f"Rejected records        : "
            f"{self.rejected_summary['rejected_record_count']}"
        )

        print(
            f"Accepted records        : "
            f"{self.rejected_summary['accepted_record_count']}"
        )

        print(
            f"Final records           : {final_count}"
        )

        print("\n--- NULL HANDLING ---")

        for column, count in self.null_handling_report[
            "before_null_to_zero"
        ].items():

            print(f"{column}: {count}")

        print(
            "\nTotal activity nulls handled: "
            f"{self.null_handling_report['total_activity_nulls_handled']}"
        )

        print("=" * 60)


# =================================================================
# MAIN
# =================================================================

if __name__ == "__main__":

    INPUT_PATH = "../data_set/sms-call-internet-mi-*.csv"

    files = glob.glob(INPUT_PATH)

    spark = (
        SparkSession.builder
        .appName("SP2_Cleaning")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=")
        .config("spark.hadoop.home.dir",r"D:\hadoop")
        .getOrCreate()
    )

    print(
    "Hadoop version:",
    spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
    )

    print(
        "Hadoop home:",
        os.environ.get("HADOOP_HOME")
    )
    # -------------------------------------------------------------
    # Load raw input files
    # -------------------------------------------------------------

    raw_network_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(files)
    )

    print("\nRaw input loaded successfully.")

    # -------------------------------------------------------------
    # SP2 - CLEANING + AGGREGATION
    # -------------------------------------------------------------

    cleaning = SparkCleaning(raw_network_df)

    (
        clean_network_df,
        hourly_grid_summary,
        rejected_df,
        rejected_summary,
        null_handling_report
    ) = cleaning.run()

    # -------------------------------------------------------------
    # CLEAN DATA
    # -------------------------------------------------------------

    print("\n--- CLEAN NETWORK DATA ---")

    clean_network_df.show(
        10,
        truncate=False
    )

    # clean_network_df.to_csv("./results/clean_network/clean.csv",index="False")
    # clean_network_df.write.mode("overwrite").option("header", True).csv("./results/clean_network/clean_csv")
    # clean_network_df.coalesce(1).write.mode("overwrite").option("header", True).csv("./results/clean_network/clean_csv")
    clean_network_df.write.mode("overwrite").parquet("./results/clean_network/clean.parquet")
    hourly_grid_summary.write.mode("overwrite").parquet("./results/hourly_grid_summary/hourly_grid_summary.parquet")
    # -------------------------------------------------------------
    # CANONICAL DOWNSTREAM ANALYTICS DATAFRAME
    # -------------------------------------------------------------

    print("\n--- HOURLY GRID SUMMARY ---")

    hourly_grid_summary.show(
        10,
        truncate=False
    )

    # -------------------------------------------------------------
    # REJECTED RECORDS
    # -------------------------------------------------------------

    print("\n--- REJECTED RECORDS ---")

    rejected_df.show(
        10,
        truncate=False
    )

    # -------------------------------------------------------------
    # SUMMARIES
    # -------------------------------------------------------------

    print("\n--- REJECTED RECORD SUMMARY ---")
    print(rejected_summary)

    print("\n--- NULL HANDLING REPORT ---")
    print(null_handling_report)

    spark.stop()
