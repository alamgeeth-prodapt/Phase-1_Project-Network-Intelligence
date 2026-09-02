
#the Sp1-6 are built as single pipeline and exercise scripts, not necessarily as reusable modules, hence we are building sp7 as its own program

from pathlib import Path
import json
from pyspark.sql import SparkSession
import logging
import os
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

        input_rows = self.df.count()

        activity_columns = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet"
        ]

        nulls_handled = 0

        for column_name in activity_columns:

            null_count = (
                self.df.filter(col(column_name).isNull())
                .count()
            )

            nulls_handled += null_count

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

        logging.info(f"Input rows: {input_rows}")
        logging.info(f"Null activity values handled: {nulls_handled}")
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

    def run(self):
        self.read_raw()
        self.clean()
        self.aggregate()
        self.enrich()
        self.write_outputs()


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

