import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum


class SP6Storage:

    # ============================================================
    # INITIALIZATION
    # ============================================================

    def __init__(self):

        self.spark = (
            SparkSession.builder
            .appName("SP6_Storage")
            .master("local[4]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )

        # --------------------------------------------------------
        # INPUT PATHS
        # --------------------------------------------------------

        self.CLEAN_INPUT = (
            "./results/clean_network/clean.parquet"
        )

        self.HOURLY_INPUT = (
            "./results/hourly_grid_summary/"
            "hourly_grid_summary.parquet"
        )

        # --------------------------------------------------------
        # OUTPUT PATHS
        # --------------------------------------------------------

        self.CLEAN_OUTPUT = (
            "./results/storage/clean_activity"
        )

        self.HOURLY_OUTPUT = (
            "./results/storage/hourly_grid_summary"
        )

        self.DASHBOARD_OUTPUT = (
            "./results/storage/dashboard_summary"
        )

        # --------------------------------------------------------
        # DataFrames
        # --------------------------------------------------------

        self.clean_df = None
        self.hourly_df = None

        self.hourly_count = None

        # Original row counts used for validation
        self.original_clean_count = None
        self.original_hourly_count = None


    # ============================================================
    # STEP 1 + 2
    # LOAD CLEAN DATA
    # PARTITION BY DATE AND WRITE PARQUET
    # ============================================================

    def write_clean_activity(self):

        print("\n--- LOADING CLEAN PARQUET ---")

        self.clean_df = self.spark.read.parquet(
            self.CLEAN_INPUT
        )

        print("\n--- CLEAN DATA SCHEMA ---")

        self.clean_df.printSchema()

        print("\n--- CLEAN DATA ROW COUNT ---")

        self.original_clean_count = self.clean_df.count()

        print(
            "Rows:",
            self.original_clean_count
        )

        print("\n--- SAMPLE DATA ---")

        self.clean_df.show(
            5,
            truncate=False
        )

        # --------------------------------------------------------
        # Partition cleaned data by date
        # --------------------------------------------------------

        print(
            "\n--- WRITING PARTITIONED CLEAN PARQUET ---"
        )

        (
            self.clean_df
            .write
            .mode("overwrite")
            .partitionBy("date")
            .parquet(self.CLEAN_OUTPUT)
        )

        print(
            "Partitioned clean Parquet "
            "written successfully."
        )

        print(
            "Output:",
            self.CLEAN_OUTPUT
        )


    # ============================================================
    # STEP 3
    # WRITE HOURLY GRID SUMMARY
    #
    # Geometry is intentionally NOT included.
    # Geometry remains in the static GeoJSON reference.
    # ============================================================

    def write_hourly_grid_summary(self):

        print(
            "\n--- LOADING HOURLY GRID SUMMARY ---"
        )

        self.hourly_df = self.spark.read.parquet(
            self.HOURLY_INPUT
        )

        print(
            "\n--- HOURLY GRID SUMMARY SCHEMA ---"
        )

        self.hourly_df.printSchema()

        print(
            "\n--- HOURLY GRID SUMMARY ROW COUNT ---"
        )

        self.hourly_count = self.hourly_df.count()

        self.original_hourly_count = (
            self.hourly_count
        )

        print(
            "Rows:",
            self.hourly_count
        )

        # --------------------------------------------------------
        # Verify one record per grid + timestamp
        # --------------------------------------------------------

        print(
            "\n--- CHECKING GRID + HOUR STRUCTURE ---"
        )

        duplicate_grid_hours = (
            self.hourly_df
            .groupBy(
                "grid_id",
                "timestamp"
            )
            .count()
            .filter("count > 1")
            .count()
        )

        print(
            "Duplicate grid + timestamp combinations:",
            duplicate_grid_hours
        )

        assert duplicate_grid_hours == 0, \
            "Duplicate grid + timestamp records found!"

        # --------------------------------------------------------
        # Geometry validation
        # --------------------------------------------------------

        print("\n--- GEOMETRY CHECK ---")

        if "geometry" in self.hourly_df.columns:

            print(
                "WARNING: geometry exists "
                "in hourly_grid_summary!"
            )

        else:

            print(
                "PASS: No geometry column "
                "in hourly_grid_summary."
            )

        # --------------------------------------------------------
        # Write hourly summary as Parquet
        # --------------------------------------------------------

        print(
            "\n--- WRITING HOURLY GRID SUMMARY PARQUET ---"
        )

        (
            self.hourly_df
            .write
            .mode("overwrite")
            .parquet(self.HOURLY_OUTPUT)
        )

        print(
            "Hourly grid summary "
            "written successfully."
        )

        print(
            "Output:",
            self.HOURLY_OUTPUT
        )


    # ============================================================
    # STEP 4
    # CREATE SMALL DASHBOARD SUMMARY
    # ============================================================

    def create_dashboard_summary(self):

        print(
            "\n--- CREATING DASHBOARD SUMMARY ---"
        )

        dashboard_summary = (
            self.hourly_df
            .groupBy("grid_id")
            .agg(
                spark_sum(
                    "total_activity"
                ).alias("total_activity")
            )
            .orderBy(
                col("total_activity").desc()
            )
            .limit(10)
        )

        dashboard_summary.show(
            10,
            truncate=False
        )

        # --------------------------------------------------------
        # Write dashboard summary as CSV
        # --------------------------------------------------------

        (
            dashboard_summary
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(self.DASHBOARD_OUTPUT)
        )

        print(
            "Dashboard summary "
            "written successfully."
        )

        print(
            "Output:",
            self.DASHBOARD_OUTPUT
        )


    # ============================================================
    # STEP 5
    # READ PARQUET OUTPUTS BACK
    # VALIDATE SCHEMA AND COUNTS
    # ============================================================

    def validate_parquet_outputs(self):

        print(
            "\n--- STEP 5: "
            "READING PARQUET OUTPUTS BACK ---"
        )

        # --------------------------------------------------------
        # Clean activity
        # --------------------------------------------------------

        print(
            "\n--- READING CLEAN ACTIVITY PARQUET ---"
        )

        clean_check_df = self.spark.read.parquet(
            self.CLEAN_OUTPUT
        )

        print("\nClean activity schema:")

        clean_check_df.printSchema()

        clean_check_count = (
            clean_check_df.count()
        )

        print(
            "Clean activity rows after reload:",
            clean_check_count
        )

        print(
            "Original clean activity rows:",
            self.original_clean_count
        )

        assert (
            clean_check_count
            == self.original_clean_count
        ), "Clean activity row count changed!"

        print(
            "PASS: Clean activity row count matches."
        )

        # --------------------------------------------------------
        # Hourly grid summary
        # --------------------------------------------------------

        print(
            "\n--- READING HOURLY GRID SUMMARY PARQUET ---"
        )

        hourly_check_df = self.spark.read.parquet(
            self.HOURLY_OUTPUT
        )

        print(
            "\nHourly grid summary schema:"
        )

        hourly_check_df.printSchema()

        hourly_check_count = (
            hourly_check_df.count()
        )

        print(
            "Hourly grid summary rows after reload:",
            hourly_check_count
        )

        print(
            "Original hourly grid summary rows:",
            self.original_hourly_count
        )

        assert (
            hourly_check_count
            == self.original_hourly_count
        ), "Hourly grid summary row count changed!"

        print(
            "PASS: Hourly grid summary row count matches."
        )

        # --------------------------------------------------------
        # Geometry validation
        # --------------------------------------------------------

        print(
            "\n--- HOURLY GEOMETRY VALIDATION ---"
        )

        assert "geometry" not in hourly_check_df.columns, \
            "Geometry should not be stored in hourly summary!"

        print(
            "PASS: Geometry is not duplicated "
            "in hourly grid summary."
        )

        # --------------------------------------------------------
        # Dashboard CSV validation
        # --------------------------------------------------------

        print(
            "\n--- DASHBOARD SUMMARY VALIDATION ---"
        )

        dashboard_check_df = (
            self.spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(self.DASHBOARD_OUTPUT)
        )

        dashboard_check_df.printSchema()

        dashboard_count = (
            dashboard_check_df.count()
        )

        print(
            "Dashboard summary rows:",
            dashboard_count
        )

        assert dashboard_count == 10, \
            "Dashboard summary should contain 10 rows!"

        print(
            "PASS: Dashboard summary contains 10 rows."
        )


    # ============================================================
    # STEP 6
    # FILE SIZE COMPARISON
    # ============================================================

    def get_directory_size(self, path):

        total_size = 0

        for root, dirs, files in os.walk(path):

            for file in files:

                file_path = os.path.join(
                    root,
                    file
                )

                total_size += os.path.getsize(
                    file_path
                )

        return total_size


    def bytes_to_mb(self, size):

        return size / (
            1024 * 1024
        )


    def compare_file_sizes(self):

        print(
            "\n--- STEP 6: FILE SIZE COMPARISON ---"
        )

        # --------------------------------------------------------
        # Calculate directory sizes
        # --------------------------------------------------------

        clean_size = self.get_directory_size(
            self.CLEAN_OUTPUT
        )

        hourly_size = self.get_directory_size(
            self.HOURLY_OUTPUT
        )

        dashboard_size = self.get_directory_size(
            self.DASHBOARD_OUTPUT
        )

        # --------------------------------------------------------
        # Convert to MB
        # --------------------------------------------------------

        clean_mb = self.bytes_to_mb(
            clean_size
        )

        hourly_mb = self.bytes_to_mb(
            hourly_size
        )

        dashboard_mb = self.bytes_to_mb(
            dashboard_size
        )

        # --------------------------------------------------------
        # Display results
        # --------------------------------------------------------

        print(
            f"Clean activity Parquet : "
            f"{clean_mb:.2f} MB"
        )

        print(
            f"Hourly grid Parquet    : "
            f"{hourly_mb:.2f} MB"
        )

        print(
            f"Dashboard CSV          : "
            f"{dashboard_mb:.2f} MB"
        )

        print(
            "\n--- COLUMNAR STORAGE OBSERVATIONS ---"
        )

        print(
            "1. Parquet stores data in a columnar format, "
            "which is efficient for analytical queries."
        )

        print(
            "2. Parquet supports compression and efficient "
            "storage of numerical and repeated values."
        )

        print(
            "3. Spark can perform column pruning and read "
            "only the columns required by a query."
        )

        print(
            "4. Date partitioning allows Spark to avoid "
            "reading unrelated date partitions when filters "
            "are applied."
        )


    # ============================================================
    # RUN COMPLETE SP6
    # ============================================================

    def run(self):

        try:

            self.write_clean_activity()

            self.write_hourly_grid_summary()

            self.create_dashboard_summary()

            self.validate_parquet_outputs()

            self.compare_file_sizes()

            print(
                "\n===================================="
            )

            print(
                "SP6 COMPLETED SUCCESSFULLY!"
            )

            print(
                "===================================="
            )

        finally:

            self.spark.stop()


# ================================================================
# MAIN
# ================================================================

if __name__ == "__main__":

    sp6 = SP6Storage()

    sp6.run()