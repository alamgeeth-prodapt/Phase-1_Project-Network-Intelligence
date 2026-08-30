import glob

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)


class SparkIngestion:

    def __init__(self, input_path, master="local[*]"):

        self.input_path = input_path

        self.spark = (
            SparkSession.builder
            .appName("SP1_NetworkIngestion")
            .master(master)
            .getOrCreate()
        )

        self.df = None

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

    # =============================================================
    # 1. LOAD DATA
    # =============================================================

    def load_data(self):

        files = glob.glob(self.input_path)

        if not files:
            raise FileNotFoundError(
                f"No files found matching: {self.input_path}"
            )

        print("\nFiles found:")
        for file in files:
            print(file)

        self.df = (
            self.spark.read
            .option("header", True)
            .schema(self.schema)
            .csv(files)
        )

        return self.df

    # =============================================================
    # 2. PROFILE DATA
    # =============================================================

    def profile_data(self):

        if self.df is None:
            raise ValueError("Run load_data() first.")

        print("\n" + "=" * 60)
        print("SP1 - SPARK INGESTION PROFILE")
        print("=" * 60)

        print("\n--- SCHEMA ---")
        self.df.printSchema()

        print("\n--- TOTAL ROWS ---")
        print(self.df.count())

        print("\n--- SOURCE FILES ---")

        self.df = self.df.withColumn(
            "input_file_name",
            input_file_name()
        )
        self.df.select(
            "input_file_name"
        ).distinct().show(
            truncate=False
        )

        missing_files = (
                    self.df.filter(self.df.input_file_name.isNull())
                    .count()
                )
        print("\n--- MISSING INPUT FILE NAMES ---")
        print(missing_files)
        
        print("\n--- UNIQUE GRIDS ---")
        print(
            self.df
            .select("CellID")
            .distinct()
            .count()
        )

        invalid_grids = (
            self.df.filter((self.df.CellID < 1) | (self.df.CellID > 10000)).count()
        )

        print("\n--- INVALID GRID IDs ---")
        print(invalid_grids)

        print("\n--- COUNTRY CODE CATEGORIES ---")
        print(
            self.df
            .select("countrycode")
            .distinct()
            .count()
        )

        print("\n--- COUNTRY CODE VALUES ---")
        self.df.select(
            "countrycode"
        ).distinct().orderBy(
            "countrycode"
        ).show()

        print("\n--- DISTINCT DATETIME VALUES ---")
        print(
            self.df
            .select("datetime")
            .distinct()
            .count()
        )

        print("\n--- PARTITIONS ---")
        print(
            self.df.rdd.getNumPartitions()
        )

        print("=" * 60)

        return self.df

    # =============================================================
    # 3. RUN SP1
    # =============================================================

    def run(self):

        self.load_data()
        self.profile_data()

        return self.df


# =================================================================
# MAIN
# =================================================================

if __name__ == "__main__":

    INPUT_PATH = "../data_set/sms-call-internet-mi-*.csv"

    ingestion = SparkIngestion(INPUT_PATH)

    df = ingestion.run()

    ingestion.spark.stop()