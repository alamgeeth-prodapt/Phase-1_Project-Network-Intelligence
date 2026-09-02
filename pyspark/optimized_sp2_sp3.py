from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
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
    desc,
)

import os
import glob

# os.environ["HADOOP_HOME"] = r"D:\hadoop"
# os.environ["PATH"] = r"D:\hadoop\bin;" + os.environ["PATH"]


class SparkCleaning:
    """
    Optimization summary vs. the original version
    -----------------------------------------------
    1. No intermediate `.count()` calls between lazy transformation steps.
       Each `.count()` was forcing a full re-execution of everything
       upstream (all the way back to the CSV read) because nothing was
       cached. With 8-10 of these scattered through `run()`, the raw
       input was effectively being re-read/re-parsed 8-10 times before
       any real work happened.

    2. `profile_activity_nulls` now does ONE aggregation pass (conditional
       sums) instead of 5 separate `.filter().count()` passes over the
       full dataset.

    3. `quarantine_invalid_records` computes the invalid/valid split with
       a single flag column + one `groupBy().count()`, instead of
       evaluating the (multi-clause) invalid condition twice and running
       two separate `.count()` actions.

    4. `self.clean_network_df` is persisted (MEMORY_AND_DISK, so it
       spills instead of OOM-ing if it doesn't fit in RAM) right after
       all row-level transforms are done and before it's reused by both
       cadence verification and the grouped summary build.

    5. `self.hourly_grid_summary` is persisted BEFORE the 5 validation
       assertions run. In the original code, each assertion
       (`.count()`, `.collect()`, `.groupBy(...).count()`) recomputed the
       full aggregate-join pipeline from `clean_network_df` onward. This
       was the single biggest source of repeated shuffle work, and is
       almost certainly the direct cause of the parquet write failing
       (by the time the write started, the cluster/local executor had
       already redone this expensive join+groupBy chain 5+ times).

    6. Cached/persisted DataFrames are explicitly `.unpersist()`-ed once
       we're done with them, so memory doesn't stay pinned into the
       final write stage.

    7. Parquet writes are repartitioned deliberately (by `grid_id`) so
       you get a controlled, reasonable number of output files instead
       of relying on whatever partition count survived the last shuffle
       (which, with `shuffle.partitions=8`, was likely fine in count but
       could easily be skewed if grid activity isn't uniform).
    """

    ACTIVITY_COLUMNS = ["sms_in", "sms_out", "call_in", "call_out", "internet"]

    def __init__(self, df):
        self.df = df
        self.raw_count = None  # computed lazily, once, only when needed

        self.rejected_df = None
        self.clean_network_df = None
        self.hourly_grid_summary = None

        self.rejected_summary = {}
        self.null_handling_report = {}

        os.environ["HADOOP_HOME"] = r"D:\hadoop"
        os.environ["PATH"] = r"D:\hadoop\bin;" + os.environ["PATH"]

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
            .withColumn("timestamp", to_timestamp("timestamp"))
            .withColumn("grid_id", col("grid_id").cast("integer"))
            .withColumn("country_code", col("country_code").cast("integer"))
            .withColumn("sms_in", col("sms_in").cast("double"))
            .withColumn("sms_out", col("sms_out").cast("double"))
            .withColumn("call_in", col("call_in").cast("double"))
            .withColumn("call_out", col("call_out").cast("double"))
            .withColumn("internet", col("internet").cast("double"))
        )
        return self.df

    # =============================================================
    # 3. PROFILE NULL ACTIVITY MEASURES  (single pass, was 5 passes)
    # =============================================================

    def profile_activity_nulls(self):
        agg_exprs = [
            spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in self.ACTIVITY_COLUMNS
        ]
        row = self.df.agg(*agg_exprs).collect()[0]
        null_counts = {c: (row[c] or 0) for c in self.ACTIVITY_COLUMNS}

        self.null_handling_report["before_null_to_zero"] = null_counts
        return null_counts

    # =============================================================
    # 4. QUARANTINE INVALID RECORDS
    #    (condition evaluated once, split + counted in a single job)
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

        flagged_df = self.df.withColumn("_is_invalid", invalid_condition).persist(
            StorageLevel.MEMORY_AND_DISK
        )

        # One aggregation gets both bucket counts (and raw count) at once.
        counts = (
            flagged_df.groupBy("_is_invalid").count().collect()
        )
        count_map = {r["_is_invalid"]: r["count"] for r in counts}
        rejected_count = count_map.get(True, 0)
        accepted_count = count_map.get(False, 0)

        self.raw_count = rejected_count + accepted_count
        self.rejected_summary["raw_record_count"] = self.raw_count
        self.rejected_summary["rejected_record_count"] = rejected_count
        self.rejected_summary["accepted_record_count"] = accepted_count

        self.rejected_df = flagged_df.filter(col("_is_invalid")).drop("_is_invalid")
        self.clean_network_df = flagged_df.filter(~col("_is_invalid")).drop(
            "_is_invalid"
        )

        return self.clean_network_df

    # =============================================================
    # 5. CURATED-LAYER NULL -> ZERO  (single pass over all columns)
    # =============================================================

    def apply_null_to_zero(self):
        # Reuse the counts we already computed in profile_activity_nulls
        # instead of re-scanning column by column.
        before = self.null_handling_report.get("before_null_to_zero")
        if before is None:
            before = self.profile_activity_nulls()

        fill_values = {c: 0.0 for c in self.ACTIVITY_COLUMNS}
        self.clean_network_df = self.clean_network_df.fillna(fill_values)

        self.null_handling_report["total_activity_nulls_handled"] = sum(
            before.values()
        )
        return self.clean_network_df

    # =============================================================
    # 6. CREATE DERIVED ACTIVITY MEASURES + 7. TIME FEATURES
    #    (merged into one chain — no reason to split these into two
    #    separate `withColumn` passes/actions)
    # =============================================================

    def create_activity_metrics(self):
        self.clean_network_df = (
            self.clean_network_df
            .withColumn("total_sms", col("sms_in") + col("sms_out"))
            .withColumn("total_calls", col("call_in") + col("call_out"))
            .withColumn(
                "total_activity",
                col("sms_in")
                + col("sms_out")
                + col("call_in")
                + col("call_out")
                + col("internet"),
            )
        )
        return self.clean_network_df

    def derive_time_features(self):
        self.clean_network_df = (
            self.clean_network_df
            .withColumn("date", to_date("timestamp"))
            .withColumn("hour", hour("timestamp"))
            .withColumn("day_of_week", dayofweek("timestamp"))
        )

        # This is the point where clean_network_df is "finished" and gets
        # reused repeatedly downstream (cadence check, grouped summary,
        # daily-activity join). Persist it ONCE here.
        self.clean_network_df = self.clean_network_df.persist(
            StorageLevel.MEMORY_AND_DISK
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
        )

        print("\n--- HOURLY CADENCE ---")
        cadence.groupBy("date").count().orderBy("date").show()

        return cadence

    # =============================================================
    # 9. COLLAPSE COUNTRY-CODE RECORDS -> HOURLY GRID SUMMARY
    # =============================================================

    def create_hourly_grid_summary(self):
        print("\n--- CREATING HOURLY GRID SUMMARY ---")

        clean_count = self.clean_network_df.count()  # clean_network_df is cached

        hourly_grid_summary = (
            self.clean_network_df
            .groupBy("grid_id", "timestamp")
            .agg(
                spark_sum("sms_in").alias("sms_in"),
                spark_sum("sms_out").alias("sms_out"),
                spark_sum("call_in").alias("call_in"),
                spark_sum("call_out").alias("call_out"),
                spark_sum("internet").alias("internet_activity"),
            )
            .withColumn("total_sms", col("sms_in") + col("sms_out"))
            .withColumn("total_calls", col("call_in") + col("call_out"))
            .withColumn(
                "total_activity",
                col("total_sms") + col("total_calls") + col("internet_activity"),
            )
            .withColumn("date", to_date("timestamp"))
            .withColumn("hour", hour("timestamp"))
            .withColumn("day_of_week", dayofweek("timestamp"))
        )

        daily_activity = (
            hourly_grid_summary
            .groupBy("grid_id", "date")
            .agg(spark_sum("total_activity").alias("daily_activity"))
        )

        hourly_grid_summary = hourly_grid_summary.join(
            daily_activity, on=["grid_id", "date"], how="left"
        ).withColumn(
            "internet_share",
            when(col("total_activity") == 0, lit(0.0)).otherwise(
                col("internet_activity") / col("total_activity")
            ),
        )

        # ---------------------------------------------------------
        # THE key fix: persist BEFORE running validations. Every
        # assertion below is an action; without this they'd each
        # redo the groupBy + self-join above from scratch.
        # ---------------------------------------------------------
        self.hourly_grid_summary = hourly_grid_summary.persist(
            StorageLevel.MEMORY_AND_DISK
        )

        print("\n--- HOURLY GRID SUMMARY VALIDATION ---")

        # REQUIREMENT 1: zero duplicates on (grid_id, timestamp)
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

        # REQUIREMENT 2: strictly fewer rows than clean_network_df
        hourly_count = self.hourly_grid_summary.count()
        assert hourly_count < clean_count, (
            "hourly_grid_summary row count is not strictly "
            "less than clean_network_df row count."
        )
        print(
            "PASS: hourly_grid_summary row count "
            f"({hourly_count}) < clean_network_df row count ({clean_count})"
        )

        # REQUIREMENT 3: row count <= D * 24 * 10000
        number_of_days = self.clean_network_df.select("date").distinct().count()
        maximum_allowed_rows = number_of_days * 24 * 10000
        assert hourly_count <= maximum_allowed_rows, (
            f"Row count {hourly_count} exceeds maximum allowed "
            f"count {maximum_allowed_rows} (D={number_of_days})"
        )
        print(
            "PASS: Row count <= D x 24 x 10000 "
            f"({hourly_count} <= {maximum_allowed_rows})"
        )

        # REQUIREMENT 4: hand-check one grid/hour
        sample_key = (
            self.clean_network_df
            .select("grid_id", "timestamp")
            .limit(1)
            .collect()
        )
        assert len(sample_key) == 1, "Unable to select a grid/hour for hand-check."

        sample_grid = sample_key[0]["grid_id"]
        sample_timestamp = sample_key[0]["timestamp"]

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
                spark_sum("internet").alias("internet_activity"),
            )
            .collect()[0]
        )

        summary_check = (
            self.hourly_grid_summary
            .filter(
                (col("grid_id") == sample_grid)
                & (col("timestamp") == sample_timestamp)
            )
            .collect()[0]
        )

        for measure in ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]:
            expected = hand_check[measure]
            actual = summary_check[measure]
            assert expected == actual, (
                f"Hand-check failed for {measure}: expected={expected}, actual={actual}"
            )
        print(
            "PASS: Hand-check reproduced exactly for "
            f"grid_id={sample_grid}, timestamp={sample_timestamp}"
        )

        # REQUIREMENT 5: country_code must not exist
        assert "country_code" not in self.hourly_grid_summary.columns, (
            "country_code must not appear in hourly_grid_summary."
        )
        print("PASS: country_code does not appear in hourly_grid_summary")

        print("\n--- SUMMARY VALIDATION ---")
        print(f"Clean records       : {clean_count}")
        print(f"Hourly summary rows : {hourly_count}")
        print(f"Number of days      : {number_of_days}")
        print(f"Maximum allowed     : {maximum_allowed_rows}")

        return self.hourly_grid_summary

    # =============================================================
    # 10. TOP TEN HIGH-ACTIVITY GRIDS
    # =============================================================

    def top_ten_high_activity_grids(self, start_timestamp=None, end_timestamp=None):
        df = self.hourly_grid_summary  # already persisted

        if start_timestamp is not None:
            df = df.filter(col("timestamp") >= to_timestamp(lit(start_timestamp)))
        if end_timestamp is not None:
            df = df.filter(col("timestamp") <= to_timestamp(lit(end_timestamp)))

        top_ten = (
            df.groupBy("grid_id")
            .agg(spark_sum("total_activity").alias("window_total_activity"))
            .orderBy(desc("window_total_activity"))
            .limit(10)
        )

        print("\n--- TOP 10 HIGH-ACTIVITY GRIDS ---")
        top_ten.show(truncate=False)
        return top_ten

    # =============================================================
    # 11. PEAK ACTIVITY HOUR
    # =============================================================

    def compute_peak_activity_hour(self):
        peak_hour = (
            self.hourly_grid_summary  # already persisted
            .groupBy("timestamp", "hour")
            .agg(spark_sum("total_activity").alias("total_activity"))
            .orderBy(desc("total_activity"))
            .limit(1)
        )

        print("\n--- PEAK ACTIVITY HOUR ---")
        peak_hour.show(truncate=False)
        return peak_hour

    # =============================================================
    # 12. RUN
    # =============================================================

    def run(self):
        # Transform steps are chained lazily with NO intermediate
        # actions — Spark only executes once something downstream
        # actually needs the result.
        self.canonicalize_columns()
        self.cast_columns()
        self.profile_activity_nulls()          # 1 action (single-pass agg)
        self.quarantine_invalid_records()       # 1 action (single-pass agg), persists split
        self.apply_null_to_zero()               # 0 actions (reuses prior counts)
        self.create_activity_metrics()
        self.derive_time_features()             # persists clean_network_df

        self.verify_hourly_cadence()            # 1 action (.show)
        self.final_profile()                    # uses cached counts, 0 new full scans

        self.create_hourly_grid_summary()       # persists hourly_grid_summary
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
    # 13. FINAL PROFILE — reuses numbers already computed, no re-scan
    # =============================================================

    def final_profile(self):
        final_count = self.rejected_summary["accepted_record_count"]
        self.rejected_summary["final_record_count"] = final_count

        print("\n" + "=" * 60)
        print("SP2 - CLEANING SUMMARY")
        print("=" * 60)
        print(f"Records before cleaning : {self.raw_count}")
        print(f"Rejected records        : {self.rejected_summary['rejected_record_count']}")
        print(f"Accepted records        : {self.rejected_summary['accepted_record_count']}")
        print(f"Final records           : {final_count}")

        print("\n--- NULL HANDLING ---")
        for column, count in self.null_handling_report["before_null_to_zero"].items():
            print(f"{column}: {count}")

        print(
            "\nTotal activity nulls handled: "
            f"{self.null_handling_report['total_activity_nulls_handled']}"
        )
        print("=" * 60)

    # =============================================================
    # 14. RELEASE CACHED MEMORY once outputs are written
    # =============================================================

    def unpersist_all(self):
        for df in (self.clean_network_df, self.hourly_grid_summary):
            if df is not None:
                df.unpersist()


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
        # A modest bump in shuffle partitions can help avoid skewed /
        # oversized partitions during the groupBy+join in step 9 —
        # tune based on your actual data volume.
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.hadoop.home.dir", r"D:\hadoop")
        .getOrCreate()
    )

    print(
        "Hadoop version:",
        spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
    )
    print("Hadoop home:", os.environ.get("HADOOP_HOME"))

    raw_network_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(files)
    )
    print("\nRaw input loaded successfully.")

    cleaning = SparkCleaning(raw_network_df)

    (
        clean_network_df,
        hourly_grid_summary,
        rejected_df,
        rejected_summary,
        null_handling_report,
    ) = cleaning.run()

    print("\n--- CLEAN NETWORK DATA ---")
    clean_network_df.show(10, truncate=False)

    # Repartition deliberately before writing so file count/size is
    # controlled rather than inherited from the last shuffle stage.
    (
        clean_network_df
        .repartition(8, "grid_id")
        .write.mode("overwrite")
        .parquet("./results/clean_network/clean.parquet")
    )

    (
        hourly_grid_summary
        .repartition(8, "grid_id")
        .write.mode("overwrite")
        .parquet("./results/hourly_grid_summary/hourly_grid_summary.parquet")
    )

    print("\n--- HOURLY GRID SUMMARY ---")
    hourly_grid_summary.show(10, truncate=False)

    print("\n--- REJECTED RECORDS ---")
    rejected_df.show(10, truncate=False)

    print("\n--- REJECTED RECORD SUMMARY ---")
    print(rejected_summary)

    print("\n--- NULL HANDLING REPORT ---")
    print(null_handling_report)

    # Free cached memory now that everything's written.
    cleaning.unpersist_all()

    spark.stop()