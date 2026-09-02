from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = (
    SparkSession.builder
    .appName("sp6")
    .master("local[4]")
    .config("spark.driver.memory","4g")
    .config("spark.sql.shuffle.partition","8")
    .getOrCreate()
)

CLEAN_INPUT = "./results/clean_network/clean.parquet"

print("\n--- LOADING CLEAN PARQUET ---")

clean_df = spark.read.parquet(CLEAN_INPUT)

print("\n--- CLEAN DATA SCHEMA ---")
clean_df.printSchema()

print("\n--- CLEAN DATA ROW COUNT ---")
print("Rows:", clean_df.count())

print("\n--- SAMPLE DATA ---")
clean_df.show(5, truncate=False)
# =============================================
# step - partition by date

CLEAN_OUTPUT = "./results/storage/clean_activity"

print("\n--- WRITING PARTITIONED CLEAN PARQUET ---")

(
    clean_df
    .write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(CLEAN_OUTPUT)
)

print("Partitioned clean Parquet written successfully.")
print("Output:", CLEAN_OUTPUT)
###################################
#step - 3 -> hourly-grid

HOURLY_INPUT = "./results/hourly_grid_summary/hourly_grid_summary.parquet"
HOURLY_OUTPUT = "./results/storage/hourly_grid_summary"

print("\n--- LOADING HOURLY GRID SUMMARY ---")

hourly_df = spark.read.parquet(HOURLY_INPUT)

print("\n--- HOURLY GRID SUMMARY SCHEMA ---")
hourly_df.printSchema()

print("\n--- HOURLY GRID SUMMARY ROW COUNT ---")
hourly_count = hourly_df.count()
print("Rows:", hourly_count)


# ------------------------------------------------------------
# Verify one record represents a grid + hour
# ------------------------------------------------------------

print("\n--- CHECKING GRID + HOUR STRUCTURE ---")

duplicate_grid_hours = (
    hourly_df
    .groupBy("grid_id", "timestamp")
    .count()
    .filter("count > 1")
    .count()
)

print("Duplicate grid + timestamp combinations:", duplicate_grid_hours)


# ------------------------------------------------------------
# Make sure geometry is NOT stored here
# ------------------------------------------------------------

print("\n--- GEOMETRY CHECK ---")

if "geometry" in hourly_df.columns:
    print("WARNING: geometry exists in hourly_grid_summary!")
else:
    print("PASS: No geometry column in hourly_grid_summary.")


# ------------------------------------------------------------
# Write as Parquet
# ------------------------------------------------------------

print("\n--- WRITING HOURLY GRID SUMMARY PARQUET ---")

(
    hourly_df
    .write
    .mode("overwrite")
    .parquet(HOURLY_OUTPUT)
)

print("Hourly grid summary written successfully.")
print("Output:", HOURLY_OUTPUT)

print("\n--- CREATING DASHBOARD SUMMARY ---")

dashboard_summary = (
    hourly_df
    .groupBy("grid_id")
    .agg(
        spark_sum("total_activity").alias("total_activity")
    )
    .orderBy(
        col("total_activity").desc()
    )
    .limit(10)
)

dashboard_summary.show(10, truncate=False)

DASHBOARD_OUTPUT = "./results/storage/dashboard_summary"

(
    dashboard_summary
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(DASHBOARD_OUTPUT)
)

print("Dashboard summary written successfully.")
print(f"Output: {DASHBOARD_OUTPUT}")

print("\n--- STEP 5: READING PARQUET OUTPUTS BACK ---")

# ============================================================
# STEP 5: READ PARQUET BACK AND VALIDATE
# ============================================================

print("\n--- STEP 5: READING PARQUET OUTPUTS BACK ---")

# ------------------------------------------------------------
# Read partitioned clean activity
# ------------------------------------------------------------

print("\n--- READING CLEAN ACTIVITY PARQUET ---")

clean_check_df = spark.read.parquet(CLEAN_OUTPUT)

print("\nClean activity schema:")
clean_check_df.printSchema()

clean_check_count = clean_check_df.count()

print("Clean activity rows after reload:", clean_check_count)
print("Original clean activity rows:", clean_df.count())

assert clean_check_count == clean_df.count(), \
    "Clean activity row count changed after writing/reading!"

print("PASS: Clean activity row count matches.")


# ------------------------------------------------------------
# Read hourly grid summary
# ------------------------------------------------------------

print("\n--- READING HOURLY GRID SUMMARY PARQUET ---")

hourly_check_df = spark.read.parquet(HOURLY_OUTPUT)

print("\nHourly grid summary schema:")
hourly_check_df.printSchema()

hourly_check_count = hourly_check_df.count()

print("Hourly grid summary rows after reload:", hourly_check_count)
print("Original hourly grid summary rows:", hourly_count)

assert hourly_check_count == hourly_count, \
    "Hourly grid summary row count changed after writing/reading!"

print("PASS: Hourly grid summary row count matches.")


# ------------------------------------------------------------
# Geometry validation
# ------------------------------------------------------------

print("\n--- HOURLY GEOMETRY VALIDATION ---")

assert "geometry" not in hourly_check_df.columns, \
    "Geometry should not be stored in hourly_grid_summary!"

print("PASS: Geometry is not duplicated in hourly grid summary.")


# ------------------------------------------------------------
# Dashboard CSV validation
# ------------------------------------------------------------

print("\n--- DASHBOARD SUMMARY VALIDATION ---")

dashboard_check_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(DASHBOARD_OUTPUT)

dashboard_check_df.printSchema()

dashboard_count = dashboard_check_df.count()

print("Dashboard summary rows:", dashboard_count)

assert dashboard_count == 10, \
    "Dashboard summary should contain 10 rows!"

print("PASS: Dashboard summary contains 10 rows.")

spark.stop()