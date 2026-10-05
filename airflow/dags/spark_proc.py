from datetime import datetime
import json
import os

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


# ============================================================
# PATHS
# ============================================================

# PROJECT_DIR = "/mnt/d/phase_1_project"

# SPARK_SCRIPT = os.path.join(
#     PROJECT_DIR,
#     "spark",
#     "de3_spark.py"
# )

DAGS_DIR = "/mnt/d/phase_1_project/airflow/dags"

# de3_spark.py sets LOG_DIR = PROJECT_ROOT / "logs", where
# PROJECT_ROOT is the parent of the script's own directory
# (spark/). Since the BashOperator below cds into DAGS_DIR and
# runs "spark/de3_spark.py", PROJECT_ROOT resolves to DAGS_DIR,
# so the status file lands at DAGS_DIR/logs/de3_status.json.
STATUS_PATH = os.path.join(
    DAGS_DIR,
    "logs",
    "de3_status.json"
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

        # ----------------------------------------------------
        # Gate on de3_spark.py's own machine-readable status,
        # not just directory existence — a prior successful
        # run can leave these directories in place even if
        # today's run failed partway through.
        # ----------------------------------------------------

        if not os.path.exists(STATUS_PATH):
            raise FileNotFoundError(
                f"DE3 status file was not created: {STATUS_PATH}"
            )

        with open(STATUS_PATH, "r", encoding="utf-8") as status_file:
            status = json.load(status_file)

        print(f"DE3 status payload: {status}")

        if status.get("status") != "SUCCESS":
            raise ValueError(
                "DE3 Spark processing reported failure: "
                f"{status.get('reason')}"
            )

        print("Processed output exists.")
        print("Analytics output exists.")
        print(
            f"Rows in: {status.get('rows_in')}, "
            f"rejected: {status.get('rows_rejected')}, "
            f"published: {status.get('rows_published')}"
        )
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