
#the Sp1-6 are built as single pipeline and exercise scripts, not necessarily as reusable modules, hence we are building sp7 as its own program

from pathlib import Path
import json
from pyspark.sql import SparkSession
import logging
import os
from datetime import datetime, timezone
from pyspark.sql.functions import (
    col,
    dayofweek,
    hour,
    to_date,
    to_timestamp,
    when,
    broadcast,
    sum as spark_sum
)
from dotenv import load_dotenv

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
LOG_DIR = PROJECT_ROOT / "logs"

load_dotenv(BASE_DIR / ".env.de3")

input_path = os.getenv("INPUT_PATH")
output_path = os.getenv("OUTPUT_PATH")
reference_path = os.getenv("REFERENCE_PATH")
analytics_path = os.getenv("ANALYTICS_PATH")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

class TelecomPipeline:

    def __init__(self, spark, input_path, output_path,analytics_path, reference_path):
        self.spark = spark
        self.input_path = input_path
        self.output_path = output_path
        self.analytics_path = analytics_path
        self.reference_path = reference_path
        self.df = None

        # Populated as the pipeline runs; kept at safe defaults so a
        # status write is possible even after a partial failure.
        self.input_rows = 0
        self.rejected_rows = 0
        self.output_rows = 0
        self.nulls_handled = 0
        self.clean_df = None
        self.aggregated_df = None
        self.enriched_df = None


    def read_raw(self   ):
        logging.info(f"Reading raw data from: {self.input_path}")
        if not os.path.exists(self.input_path):
            raise FileNotFoundError(f"Input path does not exist: {self.input_path}")
        input_files = [
        file for file in os.listdir(self.input_path)
        if file.lower().endswith(".csv")
        ]

        if not input_files:
            raise FileNotFoundError(
                f"No CSV input files found in: {self.input_path}"
            )

        logging.info(f"Found {len(input_files)} CSV input files")

        self.schema = StructType([
                    StructField("datetime", StringType(), True),
                    StructField("CellID", IntegerType(), True),
                    StructField("countrycode", IntegerType(), True),
                    StructField("smsin", DoubleType(), True),
                    StructField("smsout", DoubleType(), True),
                    StructField("callin", DoubleType(), True),
                    StructField("callout", DoubleType(), True),
                    StructField("internet", DoubleType(), True)
                ])
        
        self.df = self.spark.read.option("header",True).schema(self.schema).csv(self.input_path)

        self.df = self.df.withColumnsRenamed({
        "datetime": "timestamp",
        "CellID": "grid_id",
        "countrycode": "country_code",
        "smsin": "sms_in",
        "smsout": "sms_out",
        "callin": "call_in",
        "callout": "call_out",
        "internet": "internet"
        })

        self.df = self.df.withColumn(
            "timestamp",
            to_timestamp(
                col("timestamp"),
                "yyyy-MM-dd HH:mm:ss"
            )
        )

        return self.df

    def clean(self):    
        logging.info("Starting data cleaning")

        self.input_rows = self.df.count()

        activity_columns = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet"
        ]

        self.nulls_handled = 0

        for column_name in activity_columns:

            null_count = (
                self.df.filter(col(column_name).isNull())
                .count()
            )

            self.nulls_handled += null_count

            self.df = self.df.withColumn(
                column_name,
                when(
                    col(column_name).isNull(),
                    0.0
                ).otherwise(col(column_name))
            )

        for column_name in activity_columns:

            self.df = self.df.withColumn(
                column_name,
                when(
                    col(column_name) < 0,
                    0.0
                ).otherwise(col(column_name))
            )

        self.rejected_df = self.df.filter(
            col("grid_id").isNull() |
            col("timestamp").isNull()
        )

        self.rejected_rows = self.rejected_df.count()
    
        self.clean_df = self.df.filter(
            col("grid_id").isNotNull() &
            col("timestamp").isNotNull()
        )

        self.clean_df = (
                self.clean_df
                .withColumn("date", to_date(col("timestamp")))
                .withColumn("hour", hour(col("timestamp")))
                .withColumn("day_of_week", dayofweek(col("timestamp")))
        )

        self.output_rows = self.clean_df.count()

        logging.info(f"Input rows: {self.input_rows}")
        logging.info(f"Null activity values handled: {self.nulls_handled}")
        logging.info(f"Rejected rows: {self.rejected_rows}")
        logging.info(f"Clean output rows: {self.output_rows}")
        self.df = self.clean_df

        return self.clean_df, self.rejected_df

    def aggregate(self):

        logging.info("Starting aggregation")

        input_rows = self.df.count()

        self.aggregated_df = (
        self.df
        .filter(col("grid_id").isNotNull() & col("timestamp").isNotNull())
        .groupBy(
            "grid_id",
            "date",
            "timestamp",
            "hour",
            "day_of_week"
        )
        .agg(
            spark_sum("sms_in").alias("sms_in"),
            spark_sum("sms_out").alias("sms_out"),
            spark_sum("call_in").alias("call_in"),
            spark_sum("call_out").alias("call_out"),
            spark_sum("internet").alias("internet_activity")
            )
        )

        self.aggregated_df = (
        self.aggregated_df
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
        )

        output_rows = self.aggregated_df.count()

        logging.info(f"Rows before aggregation: {input_rows}")
        logging.info(f"Rows after aggregation: {output_rows}")

        self.df = self.aggregated_df

        return self.df
    
    def enrich(self):
        logging.info("Starting enrichment")

        if not os.path.exists(self.reference_path):
            raise FileNotFoundError(f"Reference path does not exist: {self.reference_path}")

        # reference_df = self.spark.read.option("header",True).option("inferSchema",True).json(self.reference_path)

        with open(self.reference_path, "r", encoding="utf-8") as f:
            geojson = json.load(f)

        features = geojson.get("features",[])

        if not features:
            raise ValueError(
            f"No features found in reference file: {self.reference_path}"
        )

        grid_rows = []

        for feature in features:

            properties = feature.get("properties", {})
            geometry = feature.get("geometry")

            cell_id = properties.get("cellId")

            if cell_id is not None and geometry is not None:
                grid_rows.append(
                  (int(cell_id), json.dumps(geometry))
                )

        if not grid_rows:
            raise ValueError(
                "No valid grid/geometry records found in reference file."
            )

        reference_df = self.spark.createDataFrame(
            grid_rows,
            ["grid_id", "geometry"]
        )

        self.enriched_df = (
            self.df
            .join(
                broadcast(reference_df),
                on="grid_id",
                how="left"
            )
        )

        enriched_rows = self.enriched_df.count()

        logging.info(f"Rows after enrichment: {enriched_rows}")
        self.df = self.enriched_df

        return self.enriched_df

    def write_outputs(self):
        logging.info(f"Writing outputs to: {self.output_path}")

        if self.df is None:
            raise ValueError("No DataFrame available to write")
    
        os.makedirs(self.output_path, exist_ok=True)
        os.makedirs(self.analytics_path, exist_ok=True)

        output_rows = self.df.count()

        logging.info(f"Output rows: {output_rows}")

        (
            self.clean_df
            .write #.repartitionBy("date")  # Optional: partition by date for better organization
            .mode("overwrite")
            .partitionBy("date")
            .parquet(self.output_path)
        )

        logging.info(f"Output written to: {self.output_path}")
        

        logging.info(
        f"Writing analytics output to: {self.analytics_path}"
        )

        (
        self.enriched_df
        .write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(self.analytics_path)
        )

        logging.info("Output write completed")
    
    def quality_check(self):

        logging.info("starting quality check")

        failures = []
        
        if self.output_rows == 0:
            failures.append("No output rows found after processing")

        if self.rejected_rows > 0:
            failures.append("rejected rows found, please check the rejected data for issues")

        if self.output_rows + self.rejected_rows != self.input_rows:
            failures.append("Row count mismatch: input rows do not match output + rejected rows")

        required_clean_columns = [
            "timestamp",
            "grid_id",
            "country_code",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet",
            "date",
            "hour",
            "day_of_week"
        ]

        missing_columns = [
            column_name
            for column_name in required_clean_columns
            if column_name not in self.clean_df.columns
        ]

        if missing_columns:
            failures.append(f"Missing required columns in clean DataFrame: {missing_columns}")

        null_grid_rows = self.clean_df.filter(
            col("grid_id").isNull()
        ).count()

        null_timestamp_rows = self.clean_df.filter(
            col("timestamp").isNull()
        ).count()

        if null_grid_rows > 0 or null_timestamp_rows > 0:
            failures.append(
                "Null values found in required columns after cleaning"
            )

        activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet"
        ]

        for column_name in activity_columns:

            negative_rows = self.clean_df.filter(
                col(column_name) < 0
            ).count()

            if negative_rows > 0:
                failures.append(
                    f"Clean output contains {negative_rows} "
                    f"negative values in {column_name}"
                )

        aggregated_rows = self.aggregated_df.count()

        if aggregated_rows == 0:
            failures.append("Aggregated output is empty")

        if aggregated_rows > self.output_rows:
            failures.append(
                f"Aggregation increased row count: "
                f"clean={self.output_rows}, aggregated={aggregated_rows}"
            )

        required_aggregated_columns = [
            "grid_id",
            "date",
            "timestamp",
            "hour",
            "day_of_week",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "total_sms",
            "total_calls",
            "total_activity"
        ]

        missing_aggregated_columns = [
            column_name
            for column_name in required_aggregated_columns
            if column_name not in self.aggregated_df.columns
        ]

        if missing_aggregated_columns:
            failures.append(f"Missing aggregated columns: {missing_aggregated_columns}")

        aggregated_null_keys = self.aggregated_df.filter(
            col("grid_id").isNull() |
            col("timestamp").isNull()
        ).count()

        if aggregated_null_keys > 0:
            failures.append(
                f"Aggregated output contains "
                f"{aggregated_null_keys} null grid/timestamp keys"
            )

        aggregated_activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "total_sms",
            "total_calls",
            "total_activity"
        ]

        for column_name in aggregated_activity_columns:

            negative_rows = self.aggregated_df.filter(
                col(column_name) < 0
            ).count()

            if negative_rows > 0:
                failures.append(
                    f"Aggregated output contains {negative_rows} "
                    f"negative values in {column_name}"
                )

        enriched_rows = self.enriched_df.count()

        if enriched_rows == 0:
            failures.append("Enriched output is empty")

        if enriched_rows != aggregated_rows:
            failures.append(
                f"Enrichment changed row count: "
                f"aggregated={aggregated_rows}, enriched={enriched_rows}"
            )

        null_geometry_rows = self.enriched_df.filter(
            col("geometry").isNull()
        ).count()

        if null_geometry_rows > 0:
            failures.append(
                f"Enriched output contains {null_geometry_rows} "
                f"rows without matching geometry"
            )


        if failures:
            logging.error("DE3 quality check failed")
            for failure in failures:
                logging.error(f"QC FAILURE: {failure}")

            raise ValueError(
                "DE3 quality check failed. "
                f"{len(failures)} validation(s) failed."
            )
    def write_status(self, status="SUCCESS", reason=None):
        logging.info("Writing DE3 machine-readable status")

        os.makedirs(LOG_DIR, exist_ok=True)

        rows_published = (
            self.enriched_df.count()
            if self.enriched_df is not None
            else 0
        )

        status_payload = {
            "status": status,
            "rows_in": self.input_rows,
            "rows_rejected": self.rejected_rows,
            "nulls_handled": self.nulls_handled,
            "rows_published": rows_published,
            "reason": reason,
            "processed_at": datetime.now(
                timezone.utc
            ).isoformat()
        }

        status_file = os.path.join(
            LOG_DIR,
            "de3_status.json"
        )

        with open(
            status_file,
            "w",
            encoding="utf-8"
        ) as file:
            json.dump(
                status_payload,
                file,
                indent=2
            )

        logging.info(
            f"DE3 status written to: {status_file}"
        )

    def run(self):
        try:
            self.read_raw()
            self.clean()
            self.aggregate()
            self.enrich()
            self.quality_check()
            self.write_outputs()

        except Exception as exc:

            logging.error(
                f"DE3 pipeline failed: {exc}"
            )

            self.write_status(
                status="FAILURE",
                reason=str(exc)
            )

            raise

        self.write_status(status="SUCCESS")

if __name__ == "__main__":

    spark = (SparkSession.builder
            .appName("TelecomPipeline")
            .master("local[4]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
            )

    pipeline = TelecomPipeline(spark, input_path, output_path, analytics_path, reference_path)
    pipeline.run()

    spark.stop()

