import json
import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    hour,
    dayofweek,
    date_format,
    row_number
)
from pyspark.sql.window import Window
from pyspark.sql import functions as F


# ---------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
ENV_PATH = os.path.join(BASE_DIR, ".env.de6")

load_dotenv(ENV_PATH)


ANALYTICS_PATH = os.getenv("ANALYTICS_PATH")
REFERENCE_PATH = os.getenv("REFERENCE_PATH")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

MYSQL_JAR = os.getenv("MYSQL_JAR")


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# ---------------------------------------------------------
# Warehouse Processor
# ---------------------------------------------------------

class NetworkWarehouse:

    def __init__(
        self,
        spark,
        analytics_path,
        reference_path,
        jdbc_url,
        jdbc_properties
    ):

        self.spark = spark
        self.analytics_path = analytics_path
        self.reference_path = reference_path
        self.jdbc_url = jdbc_url
        self.jdbc_properties = jdbc_properties
        self.day_summary = None
        self.analytics_df = None
        self.dim_grid = None
        self.dim_time = None
        self.fact_network_activity = None
        self.grid_daily_summary = None
        self.tables_written = []

    # -----------------------------------------------------
    # Read DE3 analytics output
    # -----------------------------------------------------

    def read_analytics(self):

        logging.info(
            f"Reading analytics data from: {self.analytics_path}"
        )

        if not os.path.exists(self.analytics_path):
            raise FileNotFoundError(
                f"Analytics path does not exist: {self.analytics_path}"
            )

        self.analytics_df = (
            self.spark.read
            .parquet(self.analytics_path)
        )

        row_count = self.analytics_df.count()

        logging.info(
            f"Analytics rows loaded: {row_count}"
        )

        logging.info(
            f"Analytics schema:\n{self.analytics_df.schema}"
        )

        if row_count == 0:
            raise ValueError(
                "Analytics output is empty."
            )

        return self.analytics_df

    # -----------------------------------------------------
    # Create dimension: dim_grid
    # -----------------------------------------------------

    def create_dim_grid(self):

        logging.info("Creating dim_grid")

        if not os.path.exists(self.reference_path):
            raise FileNotFoundError(
                f"Reference file does not exist: {self.reference_path}"
            )

        with open(
            self.reference_path,
            "r",
            encoding="utf-8"
        ) as file:

            geojson = json.load(file)

        features = geojson.get("features", [])

        if not features:
            raise ValueError(
                "No features found in GeoJSON reference."
            )

        grid_rows = []

        for feature in features:

            properties = feature.get(
                "properties",
                {}
            )

            geometry = feature.get(
                "geometry"
            )

            grid_id = properties.get(
                "cellId"
            )

            if grid_id is None:
                continue

            # geometry_reference = (
            #     self.reference_path
            # )

            # grid_rows.append(
            #     (
            #         int(grid_id),
            #         json.dumps(geometry)
            #     )
            # )

            geometry_type = geometry.get("type") if geometry else None
            coordinates = geometry.get("coordinates") if geometry else None

            if not coordinates:
                continue

            points = []

            if geometry_type == "Polygon":

                for ring in coordinates:
                    points.extend(ring)

            elif geometry_type == "MultiPolygon":

                for polygon in coordinates:
                    for ring in polygon:
                        points.extend(ring)

            else:
                logging.warning(
                    f"Unsupported geometry type for grid {grid_id}: "
                    f"{geometry_type}"
                )
                continue

            if not points:
                continue

            centroid_longitude = sum(
            point[0] for point in points
            ) / len(points)

            centroid_latitude = sum(
                point[1] for point in points
                ) / len(points)

            grid_rows.append(
                (
                    int(grid_id),
                    float(centroid_latitude),
                    float(centroid_longitude)
                )
            ) 


        if not grid_rows:
            raise ValueError(
                "No valid grid records found in GeoJSON."
            )

        self.dim_grid = self.spark.createDataFrame(
        grid_rows,
        [
            "grid_id",
            "centroid_latitude",
            "centroid_longitude"
        ]
        )

        self.dim_grid = (
        self.dim_grid
        .dropDuplicates(["grid_id"])
        )

        logging.info(
            f"dim_grid rows: {self.dim_grid.count()}"
        )

        return self.dim_grid

    # -----------------------------------------------------
    # Create dimension: dim_time
    # -----------------------------------------------------

    def create_dim_time(self):

        logging.info("Creating dim_time")

        timestamps = (
            self.analytics_df
            .select("timestamp")
            .where(col("timestamp").isNotNull())
            .distinct()
        )

        window = Window.orderBy("timestamp")

        self.dim_time = (
            timestamps
            .withColumn(
                "time_key",
                row_number().over(window)
            )
            .withColumn(
                "date",
                col("timestamp").cast("date")
            )
            .withColumn(
                "year",
                year(col("timestamp"))
            )
            .withColumn(
                "month",
                month(col("timestamp"))
            )
            .withColumn(
                "day",
                dayofmonth(col("timestamp"))
            )
            .withColumn(
                "hour",
                hour(col("timestamp"))
            )
            .withColumn(
                "day_of_week",
                dayofweek(col("timestamp"))
            )
            .withColumn(
                "day_name",
                date_format(
                    col("timestamp"),
                    "EEEE"
                )
            )
            .select(
                "time_key",
                "timestamp",
                "date",
                "year",
                "month",
                "day",
                "hour",
                "day_of_week",
                "day_name"
            )
        )

        logging.info(
            f"dim_time rows: {self.dim_time.count()}"
        )

        return self.dim_time

    # -----------------------------------------------------
    # Create fact table
    # -----------------------------------------------------

    def create_fact_network_activity(self):

        logging.info(
            "Creating fact_network_activity"
        )

        # Create lookup from timestamp → time_key
        time_lookup = self.dim_time.select(
            "time_key",
            "timestamp"
        )

        # Only activity measures belong in the fact table.
        # Geometry is intentionally NOT included.

        self.fact_network_activity = (
            self.analytics_df
            .join(
                time_lookup,
                on="timestamp",
                how="inner"
            )
            .select(
                "time_key",
                col("grid_id").alias("grid_id"),
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "internet_activity",
                "total_sms",
                "total_calls",
                "total_activity"
            )
        )

        logging.info(
            "Fact table schema:"
        )

        logging.info(
            f"\n{self.fact_network_activity.schema}"
        )

        logging.info(
            f"fact_network_activity rows: "
            f"{self.fact_network_activity.count()}"
        )

        return self.fact_network_activity

    # -----------------------------------------------------
    # Write DataFrame to MySQL
    # -----------------------------------------------------
    def create_day_summary(self):
        logging.info("creating day summary")

        fact_with_time = (
            self.fact_network_activity
            .join(
                self.dim_time.select(
                    "time_key",
                    "date",
                    "hour"
                ),
                on="time_key",
                how="inner"
            )
        )

        daily_metrics = (
        fact_with_time
            .groupBy("date")
            .agg(
                F.sum("total_activity").alias("total_activity"),
                F.countDistinct("grid_id").alias("active_grids")
            )
        )

        hourly_activity = (
        fact_with_time
        .groupBy("date", "hour")
        .agg(
            F.sum("total_activity").alias("hour_activity")
            )
        )

        peak_window = Window.partitionBy("date").orderBy(
        F.desc("hour_activity"),
        F.asc("hour")
        )

        peak_hours = (
        hourly_activity
        .withColumn(
            "rank",
            F.row_number().over(peak_window)
        )
        .filter(F.col("rank") == 1)
        .select(
            "date",
            F.col("hour").alias("peak_hour")
        )
        )

        grid_activity = (
        fact_with_time
        .groupBy("date", "grid_id")
        .agg(
            F.sum("total_activity").alias("grid_activity")
        )
    )

        grid_window = Window.partitionBy("date").orderBy(
            F.desc("grid_activity"),
            F.asc("grid_id")
        )

        top_grids = (
            grid_activity
            .withColumn(
                "rank",
                F.row_number().over(grid_window)
            )
            .filter(F.col("rank") == 1)
            .select(
                "date",
                F.col("grid_id").alias("top_grid")
            )
        )

        self.day_summary = (
        daily_metrics
        .join(
            peak_hours,
            on="date",
            how="inner"
        )
        .join(
            top_grids,
            on="date",
            how="inner"
        )
        .withColumnRenamed(
            "date",
            "summary_date"
        )
        .select(
            "summary_date",
            "total_activity",
            "active_grids",
            "peak_hour",
            "top_grid"
        )
        .orderBy("summary_date")
        )

        logging.info(
        f"day_summary rows: {self.day_summary.count()}"
        )

        logging.info(
        f"day_summary schema:\n{self.day_summary.schema}"
        )

        return self.day_summary
    
    def create_grid_daily_summary(self):

        logging.info("Creating grid_daily_summary")

        fact_with_time = (
            self.fact_network_activity
            .join(
                self.dim_time.select("time_key", "date"),
                on="time_key",
                how="inner"
            )
        )

        self.grid_daily_summary = (
            fact_with_time
            .groupBy("date", "grid_id")
            .agg(
                F.sum("total_activity").alias("total_activity")
            )
            .withColumnRenamed("date", "day")
            .select("grid_id", "day", "total_activity")
            .orderBy("grid_id", "day")
        )

        logging.info(
            f"grid_daily_summary rows: {self.grid_daily_summary.count()}"
        )

        logging.info(
            f"grid_daily_summary schema:\n{self.grid_daily_summary.schema}"
        )

        return self.grid_daily_summary

    def write_table(
        self,
        dataframe,
        table_name
    ):

        logging.info(
            f"Writing {table_name} to MySQL"
        )

        (
            dataframe.write
            .mode("overwrite")
            .jdbc(
                url=self.jdbc_url,
                table=table_name,
                properties=self.jdbc_properties
            )
        )

        self.tables_written.append(table_name)

        logging.info(
            f"{table_name} successfully written."
        )



    # -----------------------------------------------------
    # Create indexes
    # -----------------------------------------------------

    def create_indexes(self):

        logging.info(
            "Creating warehouse indexes"
        )

        connection = None

        try:

            import mysql.connector

            connection = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                database=MYSQL_DATABASE,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD
            )

            cursor = connection.cursor()

            indexes = [
                """
                CREATE INDEX idx_fact_grid
                ON fact_network_activity(grid_id)
                """,

                """
                CREATE INDEX idx_fact_time
                ON fact_network_activity(time_key)
                """,

                """
                CREATE INDEX idx_time_timestamp
                ON dim_time(timestamp)
                """,

                """
                CREATE INDEX idx_grid_grid_id
                ON dim_grid(grid_id)
                """
            ]

            for statement in indexes:

                try:
                    cursor.execute(statement)
                except Exception as error:

                    # MySQL may report duplicate index
                    # if the script is run again.
                    logging.warning(
                        f"Index creation skipped: {error}"
                    )

            connection.commit()

            cursor.close()

            logging.info(
                "Indexes created successfully."
            )

        finally:

            if connection is not None:
                connection.close()

    # -----------------------------------------------------
    # Validate warehouse
    # -----------------------------------------------------

    def validate(self):

        logging.info(
            "Validating warehouse tables"
        )

        tables = [
            "dim_grid",
            "dim_time",
            "fact_network_activity"
        ]

        connection = None

        try:

            import mysql.connector

            connection = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                database=MYSQL_DATABASE,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD
            )

            cursor = connection.cursor()

            for table in tables:

                cursor.execute(
                    f"SELECT COUNT(*) FROM {table}"
                )

                count = cursor.fetchone()[0]

                logging.info(
                    f"{table}: {count} rows"
                )

                if count == 0:
                    raise ValueError(
                        f"{table} is empty."
                    )

            cursor.close()

        finally:

            if connection is not None:
                connection.close()

    # -----------------------------------------------------
    # Write machine-readable status
    # -----------------------------------------------------

    def write_status(self, status="SUCCESS", reason=None):

        logging.info("Writing DE6 machine-readable status")

        os.makedirs(LOG_DIR, exist_ok=True)

        dim_grid_rows = (
            self.dim_grid.count()
            if self.dim_grid is not None
            else 0
        )

        dim_time_rows = (
            self.dim_time.count()
            if self.dim_time is not None
            else 0
        )

        fact_rows = (
            self.fact_network_activity.count()
            if self.fact_network_activity is not None
            else 0
        )

        daysum = (
            self.day_summary.count()
            if self.day_summary is not None
            else 0
        )

        gridsum = (
            self.grid_daily_summary.count()
            if self.grid_daily_summary is not None
            else 0 
        )

        status_payload = {
            "status": status,
            "dim_grid_rows": dim_grid_rows,
            "dim_time_rows": dim_time_rows,
            "fact_network_activity_rows": fact_rows,
            "day_summary": daysum,
            "grid_daily_summary": gridsum,
            "tables_written": self.tables_written,
            "reason": reason,
            "processed_at": datetime.now(
                timezone.utc
            ).isoformat()
        }

        status_file = os.path.join(
            LOG_DIR,
            "de6_status.json"
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
            f"DE6 status written to: {status_file}"
        )

    # -----------------------------------------------------
    # Run complete warehouse pipeline
    # -----------------------------------------------------

    def run(self):

        logging.info(
            "========== DE6 WAREHOUSE START =========="
        )

        try:

            self.read_analytics()

            self.create_dim_grid()

            self.create_dim_time()

            self.create_fact_network_activity()

            self.create_day_summary()

            self.create_grid_daily_summary()

            self.write_table(
                self.dim_grid,
                "dim_grid"
            )

            self.write_table(
                self.dim_time,
                "dim_time"
            )

            self.write_table(
                self.fact_network_activity,
                "fact_network_activity"
            )

            self.write_table(
                self.day_summary,
                "day_summary"
            )

            self.write_table(
                self.grid_daily_summary,
                "grid_daily_summary"
            )

            self.create_indexes()

            self.validate()

        except Exception as exc:

            logging.error(
                f"DE6 warehouse pipeline failed: {exc}"
            )

            self.write_status(
                status="FAILURE",
                reason=str(exc)
            )

            raise

        self.write_status(status="SUCCESS")

        logging.info(
            "========== DE6 WAREHOUSE COMPLETE =========="
        )


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

if __name__ == "__main__":

    required_variables = {
        "ANALYTICS_PATH": ANALYTICS_PATH,
        "REFERENCE_PATH": REFERENCE_PATH,
        "MYSQL_DATABASE": MYSQL_DATABASE,
        "MYSQL_USER": MYSQL_USER,
        "MYSQL_PASSWORD": MYSQL_PASSWORD
    }

    missing = [
        name
        for name, value in required_variables.items()
        if not value
    ]

    if missing:

        raise EnvironmentError(
            "Missing environment variables: "
            + ", ".join(missing)
        )

    if not MYSQL_JAR:

        raise EnvironmentError(
            "MYSQL_JAR is not configured."
        )

    jdbc_url = (
        f"jdbc:mysql://"
        f"{MYSQL_HOST}:"
        f"{MYSQL_PORT}/"
        f"{MYSQL_DATABASE}"
    )

    jdbc_properties = {
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    spark = (
        SparkSession.builder
        .appName("NetworkWarehouseDE6")
        .master("local[4]")
        .config(
            "spark.driver.memory",
            "4g"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )
        .config(
            "spark.jars",
            MYSQL_JAR
        )
        .getOrCreate()
    )

    try:

        warehouse = NetworkWarehouse(
            spark=spark,
            analytics_path=ANALYTICS_PATH,
            reference_path=REFERENCE_PATH,
            jdbc_url=jdbc_url,
            jdbc_properties=jdbc_properties
        )

        warehouse.run()

    finally:

        spark.stop()