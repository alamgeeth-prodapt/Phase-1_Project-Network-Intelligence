import json
import os

from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import (
    col,
    broadcast,
    sum as spark_sum,
    count as spark_count,
    when,
)


# ============================================================
# Optimization summary vs. the original version
# ------------------------------------------------------------
# 1. `enriched_df` (the broadcast join result) is persisted
#    ONCE, right after it's built. In the original script it
#    was recomputed from scratch by every downstream action:
#    distinct_after, missing_geometry_grids, before/after row
#    counts, geometry_types.count(), top_grids, and finally the
#    parquet write — 7+ full re-executions of the join.
#
# 2. `activity_df` is persisted after the initial read since
#    it's scanned/counted multiple times before the join
#    (activity_count, distinct_before, before_rows).
#
# 3. `distinct_after` + `missing_geometry_grids` (previously two
#    separate `.distinct().count()` passes) are now computed
#    together in a single aggregation over one cached distinct
#    grid/geometry set.
#
# 4. `before_rows` reuses the `activity_count` already computed,
#    instead of re-counting `activity_df` a second time.
#
# 5. `grid_lookup` is small (comes from an in-memory Python list,
#    not a file read) so caching it is cheap insurance, not a
#    meaningful win — done anyway since it's reused 3x.
# ============================================================


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("SP4_GeoEnrichment")
    .master("local[4]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)


# ============================================================
# PATHS
# ============================================================

INPUT_PATH = "./results/hourly_grid_summary/hourly_grid_summary.parquet"
GEOJSON_PATH = "../data_set/milano-grid.geojson"
OUTPUT_PATH = "./results/enriched_network/enriched_network.parquet"


# ============================================================
# 1. LOAD NETWORK ACTIVITY
# ============================================================

activity_df = spark.read.parquet(INPUT_PATH).persist(StorageLevel.MEMORY_AND_DISK)

print("\n--- ACTIVITY DATA ---")
activity_df.printSchema()

activity_count = activity_df.count()
print("Activity rows:", activity_count)


# ============================================================
# 2. LOAD AND INSPECT GEOJSON
# ============================================================

with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
    geojson = json.load(f)

print("\n--- GEOJSON STRUCTURE ---")
print("Top-level type:", geojson.get("type"))

features = geojson.get("features", [])
print("Number of features:", len(features))

if features:
    first_feature = features[0]
    print("Feature type:", first_feature.get("type"))
    print("Grid identifier:", first_feature.get("properties", {}).get("cellId"))
    print("Geometry type:", first_feature.get("geometry", {}).get("type"))


# ============================================================
# 3. FLATTEN GEOJSON INTO GRID LOOKUP
# ============================================================

grid_rows = []

for feature in features:
    properties = feature.get("properties", {})
    geometry = feature.get("geometry")
    cell_id = properties.get("cellId")

    if cell_id is not None and geometry is not None:
        grid_rows.append((int(cell_id), json.dumps(geometry)))

grid_lookup = spark.createDataFrame(grid_rows, ["grid_id", "geometry"]).persist(
    StorageLevel.MEMORY_AND_DISK
)

print("\n--- GRID LOOKUP ---")
grid_lookup.printSchema()

grid_count = grid_lookup.count()
print("Grid lookup rows:", grid_count)
grid_lookup.show(5, truncate=False)


# ============================================================
# 4. SIZE COMPARISON
# ============================================================

print("\n--- SIZE COMPARISON ---")
print("Activity rows:", activity_count)
print("Grid lookup rows:", grid_count)
print("Grid lookup is much smaller:", grid_count < activity_count)


# ============================================================
# 5. DISTINCT ACTIVITY GRIDS BEFORE JOIN
# ============================================================

distinct_before = (
    activity_df
    .select("grid_id")
    .where(col("grid_id").isNotNull())
    .distinct()
    .count()
)

print("\nDistinct activity grids before join:", distinct_before)


# ============================================================
# 6. STANDARD JOIN EXECUTION PLAN  (plan only — no execution)
# ============================================================

print("\n--- STANDARD JOIN PLAN ---")

standard_join = activity_df.join(grid_lookup, on="grid_id", how="left")
standard_join.explain(mode="formatted")


# ============================================================
# 7. BROADCAST JOIN EXECUTION PLAN  (plan only — no execution)
# ============================================================

print("\n--- BROADCAST JOIN PLAN ---")

broadcast_join = activity_df.join(broadcast(grid_lookup), on="grid_id", how="left")
broadcast_join.explain(mode="formatted")


# ============================================================
# 8. PERFORM BROADCAST LEFT JOIN
#    Persist immediately — everything from here on reuses this
#    result instead of recomputing the join.
# ============================================================

enriched_df = broadcast_join.persist(StorageLevel.MEMORY_AND_DISK)


# ============================================================
# 9. JOIN VALIDATION
#    distinct_after + missing_geometry_grids combined into one
#    aggregation over one cached distinct grid/geometry set.
# ============================================================

grid_geometry_status = (
    enriched_df
    .select("grid_id", "geometry")
    .where(col("grid_id").isNotNull())
    .distinct()
    .persist(StorageLevel.MEMORY_AND_DISK)
)

status_row = grid_geometry_status.agg(
    spark_count(col("grid_id")).alias("distinct_after"),
    spark_sum(when(col("geometry").isNull(), 1).otherwise(0)).alias(
        "missing_geometry_grids"
    ),
).collect()[0]

distinct_after = status_row["distinct_after"]
missing_geometry_grids = status_row["missing_geometry_grids"] or 0

grid_geometry_status.unpersist()

if distinct_before > 0:
    enriched_percentage = (
        (distinct_before - missing_geometry_grids) / distinct_before
    ) * 100
else:
    enriched_percentage = 0.0

print("\n--- JOIN VALIDATION ---")
print("Distinct activity grids before join:", distinct_before)
print("Distinct grids after join:", distinct_after)
print("Grids with missing geometry:", missing_geometry_grids)
print(f"Geometry enrichment percentage: {enriched_percentage:.2f}%")


# ============================================================
# 10. VALIDATE ROW PRESERVATION
#     Reuses activity_count instead of re-counting activity_df.
# ============================================================

before_rows = activity_count
after_rows = enriched_df.count()

print("\n--- ROW COUNT VALIDATION ---")
print("Rows before join:", before_rows)
print("Rows after join :", after_rows)

assert before_rows == after_rows, "LEFT JOIN changed the number of activity rows!"
print("PASS: Left join preserved activity row count.")


# ============================================================
# 11. GEOGRAPHICAL VALIDATION
# ============================================================

print("\n--- GEOGRAPHICAL VALIDATION ---")

geometry_present_df = enriched_df.filter(col("geometry").isNotNull()).select(
    "geometry"
)

print("Rows containing geometry:", geometry_present_df.count())
print("Sample geometries:")
geometry_present_df.show(5, truncate=False)


# ============================================================
# 12. CREATE REQUIRED ENRICHED DATASET
# ============================================================

enriched_df = enriched_df.select(
    "timestamp",
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
    "total_activity",
    "geometry",
)


# ============================================================
# 13. TOP HIGH-ACTIVITY GRIDS
# ============================================================

print("\n--- TOP HIGH-ACTIVITY GRIDS ---")

top_grids = (
    enriched_df
    .groupBy("grid_id", "geometry")
    .agg(spark_sum("total_activity").alias("window_total_activity"))
    .orderBy(col("window_total_activity").desc())
    .limit(10)
)

top_grids.show(10, truncate=False)


# ============================================================
# 14. WRITE ENRICHED PARQUET
# ============================================================

print("\n--- WRITING ENRICHED DATASET ---")

(
    enriched_df
    .repartition(8, "grid_id")
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print(f"SP4 completed successfully!\nOutput: {OUTPUT_PATH}")

# Release cached memory now that everything's written.
# (unpersist the DataFrame objects that were actually persisted —
# `enriched_df` was reassigned to a projection of it in step 12,
# so the original persisted object is only reachable via `broadcast_join`.)
activity_df.unpersist()
grid_lookup.unpersist()
broadcast_join.unpersist()

spark.stop()