from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    broadcast,
    sum as spark_sum
)

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)

import json
import glob

from sp2_sp3 import SparkCleaning

class SparkGeoEnrichment:

    def __init__(self, hourly_grid_summary, geojson_path):
        self.hourly_grid_summary = hourly_grid_summary
        self.geojson_path = geojson_path

        self.grid_lookup = None
        self.grid_activity_geo_df = None
        self.unmatched_grid_ids = None
        self.enrichment_report = {}

    def load_geojson(self):
        with open(self.geojson_path, 'r') as f:
            geojson = json.load(f)

        print("Top-level type:", geojson.get("type"))
        print("Number of features:", len(geojson.get("features", [])))

        feature = geojson["features"][0]

        print("Feature type:", feature.get("type"))
        print("Properties:", feature.get("properties"))
        print("Geometry type:", feature.get("geometry", {}).get("type"))

        return geojson

if __name__ == "__main__":

    INPUT_PATH = "../data_set/sms-call-internet-mi-*.csv"
    GEOJSON_PATH = "../data_set/milano-grid.geojson"

    files = glob.glob(INPUT_PATH)

    spark = (
        SparkSession.builder
        .appName("SP4_GeoEnrichment")
        .master("local[*]")
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
            "2"
        )
        .config(
            "spark.hadoop.fs.file.impl",
            "org.apache.hadoop.fs.RawLocalFileSystem"
        )
        .getOrCreate()
    )

    print("\nRaw input files:")
    for file in files:
        print(file)

    # -------------------------------------------------------------
    # LOAD RAW ACTIVITY DATA
    # -------------------------------------------------------------

    raw_network_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(files)
    )

    print("\nRaw network data loaded.")

    # -------------------------------------------------------------
    # RUN SP2 + SP3
    # -------------------------------------------------------------

    cleaning = SparkCleaning(raw_network_df)

    (
        clean_network_df,
        rejected_df,
        rejected_summary,
        null_handling_report,
        hourly_grid_summary,
    ) = cleaning.run()

    print("\nSP2 + SP3 completed.")

    print("\n--- HOURLY GRID SUMMARY ---")
    hourly_grid_summary.show(10, truncate=False)

    # -------------------------------------------------------------
    # RUN SP4
    # -------------------------------------------------------------

    geo_enrichment = SparkGeoEnrichment(
        hourly_grid_summary,
        GEOJSON_PATH
    )

    geo_enrichment.load_geojson()

    # -------------------------------------------------------------
    # STOP SPARK
    # -------------------------------------------------------------

    spark.stop()
