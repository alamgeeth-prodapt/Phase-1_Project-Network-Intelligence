import os
import json
import logging
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)
from pyspark.sql import functions as F

# =========================================================
# ENVIRONMENT & LOGGING CONFIGURATION
# =========================================================
load_dotenv()

hadoop_home = os.getenv("HADOOP_HOME")
if hadoop_home:
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = os.environ["PATH"] + os.pathsep + os.path.join(hadoop_home, "bin")

LOGS_DIR = Path(__file__).resolve().parent / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOGS_DIR / "geo_enrichment.log"

logger = logging.getLogger("MilanoSpatialEnrichment")
logger.setLevel(logging.INFO)
logger.propagate = False

if not logger.handlers:
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

logger.info("Logging initialized. Output routed to: %s", LOG_FILE)


# =========================================================
# 1. GEOJSON STRUCTURE INSPECTION
# =========================================================

def inspect_geojson_structure(geojson_path: str) -> Dict[str, Any]:
    """
    Parses and inspects the raw GeoJSON structure of `milano-grid.geojson`.
    """
    path = Path(geojson_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"GeoJSON file not found at: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    top_level_type = data.get("type", "Unknown")
    features = data.get("features", [])
    total_features = len(features)

    if not features:
        raise ValueError(f"GeoJSON file '{path}' contains no features.")

    sample_feature = features[0]
    props = sample_feature.get("properties", {})

    if "cellId" in props:
        id_location = "features[].properties.cellId"
        sample_id = props["cellId"]
    elif "cell_id" in props:
        id_location = "features[].properties.cell_id"
        sample_id = props["cell_id"]
    elif "id" in props:
        id_location = "features[].properties.id"
        sample_id = props["id"]
    elif "id" in sample_feature:
        id_location = "features[].id"
        sample_id = sample_feature["id"]
    else:
        id_location = "Unknown property key"
        sample_id = None

    geom = sample_feature.get("geometry", {})
    geometry_type = geom.get("type", "Unknown")
    coordinates_rings = len(geom.get("coordinates", []))

    summary = {
        "top_level_type": top_level_type,
        "total_features": total_features,
        "grid_id_location": id_location,
        "sample_grid_id": sample_id,
        "geometry_type": geometry_type,
        "sample_coordinates_rings": coordinates_rings,
        "sample_properties": props,
    }

    logger.info("================ GEOJSON INSPECTION REPORT ================")
    logger.info("1. Top-Level Type          : %s", top_level_type)
    logger.info("2. Total Grid Cells        : %d", total_features)
    logger.info("3. Grid ID Location        : %s (Sample value: %s)", id_location, sample_id)
    logger.info("4. Geometry Type           : %s", geometry_type)
    logger.info("===========================================================")

    return summary


# =========================================================
# 2. STATIC GRID REFERENCE EXTRACTION
# =========================================================

def export_static_grid_reference(
    spark: SparkSession,
    geojson_path: str,
    output_path: str
):
    """
    Parses `milano-grid.geojson` and exports a static dimension table containing
    (grid_id: StringType, geometry: StringType).
    
    Keeping the full Polygon geometry in this static reference table avoids
    duplicating large coordinate arrays into millions of hourly analytical records.
    """
    resolved_geojson = Path(geojson_path).resolve()
    resolved_output = Path(output_path).resolve()

    if not resolved_geojson.exists():
        raise FileNotFoundError(f"GeoJSON file not found at: {resolved_geojson}")

    with open(resolved_geojson, "r", encoding="utf-8") as f:
        data = json.load(f)

    grid_records = []
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        cell_id = (
            props.get("cellId")
            or props.get("cell_id")
            or props.get("id")
            or feature.get("id")
        )

        geometry_json_str = json.dumps(feature.get("geometry", {}))

        if cell_id is not None:
            grid_records.append((str(cell_id), geometry_json_str))

    schema = StructType([
        StructField("grid_id", StringType(), False),
        StructField("geometry", StringType(), False),
    ])

    grid_df = spark.createDataFrame(grid_records, schema=schema)
    resolved_output.parent.mkdir(parents=True, exist_ok=True)

    grid_df.write.mode("overwrite").parquet(str(resolved_output))
    logger.info(
        "Static grid reference (%d cells) persisted to: %s",
        len(grid_records),
        resolved_output,
    )
    return grid_df


# =========================================================
# 3. HOURLY GRID SUMMARY GENERATION (ANALYTICS)
# =========================================================

def build_hourly_grid_summary(
    spark: SparkSession,
    curated_usage_path: str,
    output_path: str
):
    """
    Aggregates curated telecom activity at one record per grid cell and hour.
    Does NOT duplicate Polygon geometry strings on every record.
    """
    curated_path_obj = Path(curated_usage_path).resolve()
    out_dir_obj = Path(output_path).resolve()

    if not curated_path_obj.exists():
        raise FileNotFoundError(
            f"Curated usage path not found at: '{curated_path_obj}'. "
            "Please ensure the UsageProcessor ingestion pipeline has finished writing."
        )

    logger.info("Loading curated usage dataset from: %s", curated_path_obj)
    usage_df = spark.read.parquet(str(curated_path_obj))

    # Standardize column naming if necessary
    column_mapping = {
        "sms_in_count": "sms_in",
        "sms_out_count": "sms_out",
        "call_in_count": "call_in",
        "call_out_count": "call_out",
        "internet_usage": "internet_activity",
    }

    standardized_df = usage_df
    for raw_col, target_col in column_mapping.items():
        if raw_col in standardized_df.columns:
            standardized_df = standardized_df.withColumnRenamed(raw_col, target_col)

    activity_cols = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]
    for col in activity_cols:
        if col not in standardized_df.columns:
            standardized_df = standardized_df.withColumn(col, F.lit(0.0).cast(DoubleType()))
        else:
            standardized_df = standardized_df.withColumn(col, F.col(col).cast(DoubleType()))

    # Ensure temporal columns exist for hourly grouping
    if "date" not in standardized_df.columns:
        standardized_df = standardized_df.withColumn(
            "date", F.coalesce(F.to_date("timestamp"), F.lit("1970-01-01"))
        )

    if "hour" not in standardized_df.columns:
        standardized_df = standardized_df.withColumn("hour", F.hour("timestamp"))

    # Hourly aggregation: 1 record per grid and hour
    hourly_grid_df = (
        standardized_df
        .groupBy("date", "hour", "grid_id")
        .agg(
            F.sum("sms_in").alias("sms_in"),
            F.sum("sms_out").alias("sms_out"),
            F.sum("call_in").alias("call_in"),
            F.sum("call_out").alias("call_out"),
            F.sum("internet_activity").alias("internet_activity"),
            (
                F.sum("sms_in")
                + F.sum("sms_out")
                + F.sum("call_in")
                + F.sum("call_out")
                + F.sum("internet_activity")
            ).alias("total_activity"),
            F.count("timestamp").alias("record_count"),
        )
    )

    # Persist partitioned by date
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    out_dir_obj.mkdir(parents=True, exist_ok=True)

    (
        hourly_grid_df
        .repartition("date")
        .write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(str(out_dir_obj))
    )

    logger.info("Hourly grid summary persisted to: %s", out_dir_obj)
    return hourly_grid_df


# =========================================================
# ENTRYPOINT
# =========================================================

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("MilanoSpatialHourlyGridPipeline")
        .master("local[*]")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Resolved absolute paths based on project root
    BASE_DIR = Path(__file__).resolve().parent
    GEOJSON_FILE = str(BASE_DIR / "data" / "milano-grid.geojson")
    CURATED_USAGE_DIR = str(BASE_DIR / "report_spark" / "curated_usage")
    
    GRID_REFERENCE_DIR = str(BASE_DIR / "report_spark" / "grid_reference.parquet")
    HOURLY_SUMMARY_DIR = str(BASE_DIR / "report_spark" / "hourly_grid_summary")

    try:
        # Step 1: Inspect GeoJSON metadata
        inspect_geojson_structure(GEOJSON_FILE)

        # Step 2: Save static grid reference dimension table (with Polygon geometry)
        export_static_grid_reference(
            spark=spark,
            geojson_path=GEOJSON_FILE,
            output_path=GRID_REFERENCE_DIR,
        )

        # Step 3: Generate and save hourly_grid_summary (one row per grid and hour, no duplicate geometry)
        hourly_summary_data = build_hourly_grid_summary(
            spark=spark,
            curated_usage_path=CURATED_USAGE_DIR,
            output_path=HOURLY_SUMMARY_DIR,
        )

        logger.info("Sample hourly grid summary records:")
        hourly_summary_data.select(
            "date", "hour", "grid_id", "sms_in", "call_in", "internet_activity", "total_activity"
        ).show(5, truncate=False)

    finally:
        spark.stop()