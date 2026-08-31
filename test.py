from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TestParquet")
    .master("local[1]")
    .getOrCreate()
)

df = spark.createDataFrame(
    [(1, "a"), (2, "b"), (3, "c")],
    ["id", "value"]
)

print("About to write...")

df.write \
    .mode("overwrite") \
    .parquet(r"D:\phase_1_project\pyspark\results\test_parquet")

print("WRITE SUCCESS")

spark.stop()
