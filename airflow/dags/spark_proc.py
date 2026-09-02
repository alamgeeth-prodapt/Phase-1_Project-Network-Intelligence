from datetime import datetime
import os

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


# ============================================================
# PATHS
# ============================================================

PROJECT_DIR = "/mnt/d/phase_1_project"

SPARK_SCRIPT = os.path.join(
    PROJECT_DIR,
    "spark",
    "de3_spark.py"
)


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="de3_spark_processing",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["DE3", "spark", "processing"],
)
def de3_spark_processing():

    # ========================================================
    # 1. CHECK RAW INPUT
    # ========================================================

    @task
    def check_raw_input():

        raw_path ="/mnt/d/phase_1_project/airflow/dags/data/raw"

        if not os.path.exists(raw_path):
            raise FileNotFoundError(
                f"Raw directory does not exist: {raw_path}"
            )

        csv_files = [
            file
            for file in os.listdir(raw_path)
            if file.lower().endswith(".csv")
        ]

        if not csv_files:
            raise FileNotFoundError(
                "No CSV files found in raw zone."
            )

        print(
            f"Found {len(csv_files)} raw CSV file(s)."
        )

        for file in csv_files:
            print(f"  - {file}")

    # ========================================================
    # 2. RUN SPARK PROCESSING
    # ========================================================

    spark_job = BashOperator(
        task_id="run_spark_processing",

        bash_command=(
            f"cd /mnt/d/phase_1_project/airflow/dags && "
            f"python spark/de3_spark.py"
        ),
    )

    # ========================================================
    # 3. VERIFY OUTPUTS
    # ========================================================

    @task
    def verify_outputs():

        processed_path ="/mnt/d/phase_1_project/airflow/dags/data/processed"

        analytics_path = "/mnt/d/phase_1_project/airflow/dags/data/analytics"

        if not os.path.exists(processed_path):
            raise FileNotFoundError(
                "Processed output was not created."
            )

        if not os.path.exists(analytics_path):
            raise FileNotFoundError(
                "Analytics output was not created."
            )

        print("Processed output exists.")
        print("Analytics output exists.")
        print("DE3 Spark processing completed successfully.")

    # ========================================================
    # DEPENDENCIES
    # ========================================================

    check = check_raw_input()

    check >> spark_job

    spark_job >> verify_outputs()


# ============================================================
# DAG INSTANCE
# ============================================================

de3_spark_processing()