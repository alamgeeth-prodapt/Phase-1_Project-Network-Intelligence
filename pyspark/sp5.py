from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.storagelevel import StorageLevel
import time
import json
from pyspark.sql.functions import broadcast

# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("SP5_Performance")
    .master("local[4]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)


# ============================================================
# PATH
# ============================================================

INPUT_PATH = "./results/hourly_grid_summary/hourly_grid_summary.parquet"


# ============================================================
# LOAD DATA
# ============================================================

activity_df = spark.read.parquet(INPUT_PATH)

print("\n--- ACTIVITY DATA ---")
activity_df.printSchema()

print("Rows:", activity_df.count())


# ============================================================
# STEP 1: HOTSPOT AGGREGATION
# ============================================================

print("\n--- HOTSPOT AGGREGATION ---")

hotspots = (
    activity_df
    .groupBy("grid_id")
    .agg(
        spark_sum("total_activity").alias("total_activity")
    )
    .orderBy(
        col("total_activity").desc()
    )
    .limit(10)
)



# ============================================================
# PHYSICAL EXECUTION PLAN
# ============================================================

print("\n--- HOTSPOT PHYSICAL PLAN ---")

hotspots.explain(mode="formatted")


# ============================================================
# ACTUAL RESULT
# ============================================================

print("\n--- TOP 10 HOTSPOTS ---")

hotspots.show(
    10,
    truncate=False
)

#step - 2 - caching
# ============================================================
print("\n--- WITHOUT CACHE ---")

start = time.perf_counter()

activity_df.count()

first_uncached = time.perf_counter() - start


start = time.perf_counter()

activity_df.count()

second_uncached = time.perf_counter() - start


print(f"First count  : {first_uncached:.3f} seconds")
print(f"Second count : {second_uncached:.3f} seconds")


# ============================================================

cached_df = activity_df.persist(StorageLevel.MEMORY_AND_DISK)

# Materialize the cache
start = time.perf_counter()

cached_df.count()

cache_materialization = time.perf_counter() - start


# Repeated actions using cached data
start = time.perf_counter()

cached_df.count()

first_cached = time.perf_counter() - start


start = time.perf_counter()

cached_df.count()

second_cached = time.perf_counter() - start


print(f"Cache materialization : {cache_materialization:.3f} seconds")
print(f"First cached count    : {first_cached:.3f} seconds")
print(f"Second cached count   : {second_cached:.3f} seconds")


cached_df.unpersist()

# ============================================================
#step - 3 - partitioning

original_partitions = activity_df.rdd.getNumPartitions()
repartitioned_df = activity_df.repartition(8,"date")

repartitioned_partitions = (repartitioned_df.rdd.getNumPartitions())

print("\n--- REPARTITIONED BY DATE ---")
print(
    "Partition count after repartition:",
    repartitioned_partitions
)

print("\n--- PARTITION DISTRIBUTION ---")

partition_distribution = (
    repartitioned_df
    .groupBy("date")
    .count()
    .orderBy("date")
)

partition_distribution.show(
    50,
    truncate=False
)
# ============================================================
#step - 4 - Column Pruning

print("\n--- COLUMN PRUNING ---")

pruned_df = activity_df.select(
    "grid_id",
    "total_activity"
)

print("Columns before pruning:")
print(activity_df.columns)

print("\nColumns after pruning:")
print(pruned_df.columns)

pruned_hotspots = (
    pruned_df
    .groupBy("grid_id")
    .agg(
        spark_sum("total_activity").alias("total_activity")
    )
    .orderBy(
        col("total_activity").desc()
    )
    .limit(10)
)

print("\n--- COLUMN PRUNING PHYSICAL PLAN ---")

pruned_hotspots.explain(mode="formatted")

# ============================================================
#step - 5 - 

GEOJSON_PATH = "../data_set/milano-grid.geojson"

with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
    geojson = json.load(f)

features = geojson.get("features", [])

grid_rows = []

for feature in features:
    properties = feature.get("properties", {})
    geometry = feature.get("geometry")
    cell_id = properties.get("cellId")

    if cell_id is not None and geometry is not None:
        grid_rows.append(
            (int(cell_id), json.dumps(geometry))
        )

grid_lookup = spark.createDataFrame(
    grid_rows,
    ["grid_id", "geometry"]
)

print("\n--- GRID LOOKUP ---")
print("Grid lookup rows:", grid_lookup.count())
grid_lookup.printSchema()

print("\n--- STANDARD JOIN PLAN ---")

standard_join = (
    activity_df
    .join(
        grid_lookup,
        on="grid_id",
        how="left"
    )
)

standard_join.explain(mode="formatted")

print("\n--- BROADCAST JOIN PLAN ---")

broadcast_join = (
    activity_df
    .join(
        broadcast(grid_lookup),
        on="grid_id",
        how="left"
    )
)

broadcast_join.explain(mode="formatted")
# ============================================================
#step - 6 - over-partitioning is bad
print("\n--- OVER-PARTITIONING EXPERIMENT ---")

partition_counts = [4, 8, 32, 100]

for num_partitions in partition_counts:

    test_df = activity_df.repartition(num_partitions)

    start = time.perf_counter()

    test_df.groupBy("grid_id").agg(
        spark_sum("total_activity").alias("total_activity")
    ).count()

    elapsed = time.perf_counter() - start

    print(
        f"Partitions: {num_partitions:3d} | "
        f"Time: {elapsed:.3f} seconds"
    )

# ============================================================
# STEP 7: PERFORMANCE OBSERVATIONS
# ============================================================

print("\n--- PERFORMANCE OBSERVATIONS ---")

print("""
1. COLUMN PRUNING
   Evidence:
   - Activity dataset contains 15 columns.
   - Hotspot aggregation requires only grid_id and total_activity.
   - Physical plan shows ReadSchema containing only these 2 columns.
   Conclusion:
   Spark avoids reading unnecessary Parquet columns, reducing I/O.

2. BROADCAST JOIN
   Evidence:
   - Activity dataset: 1,679,994 rows.
   - Grid lookup: 10,000 rows.
   - Standard join uses SortMergeJoin with Exchange and Sort stages.
   - Broadcast join uses BroadcastHashJoin with BroadcastExchange.
   Conclusion:
   The small static grid lookup is a suitable broadcast candidate,
   reducing shuffle-related join overhead.

3. PARTITIONING
   Evidence:
   - Spark runs locally with 4 execution threads.
   - Data was repartitioned into 8 partitions by date.
   - The seven dates have approximately 240,000 rows each,
     indicating relatively balanced data volume by date.
   Conclusion:
   A moderate partition count provides parallelism without creating
   excessive task and shuffle overhead. Over-partitioning a local
   workload can increase scheduling overhead without proportional gains.
""")

spark.stop()