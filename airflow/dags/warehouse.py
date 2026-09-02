import logging

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime


@dag(
    dag_id="de6_network_warehouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["DE6", "warehouse", "mysql", "spark"]
)
def de6_network_warehouse():

    @task
    def check_analytics_output():

        import os

        analytics_path = (
            "/mnt/d/phase_1_project/"
            "airflow/dags/data/analytics"
        )

        logging.info(
            f"Checking analytics output: {analytics_path}"
        )

        if not os.path.exists(analytics_path):

            raise FileNotFoundError(
                f"Analytics output does not exist: "
                f"{analytics_path}"
            )

        parquet_files = []

        for root, directories, files in os.walk(
            analytics_path
        ):

            for file in files:

                if file.endswith(".parquet"):

                    parquet_files.append(
                        os.path.join(root, file)
                    )

        if not parquet_files:

            raise FileNotFoundError(
                "No Parquet files found in analytics output."
            )

        logging.info(
            f"Found {len(parquet_files)} Parquet files."
        )

    run_warehouse = BashOperator(
        task_id="run_de6_warehouse",

        bash_command="""
        set -e

        echo "======================================"
        echo "Starting DE6 Warehouse Processing"
        echo "======================================"

        cd /mnt/d/phase_1_project/airflow/dags

        /mnt/d/phase_1_project/venv-ubuntu/bin/python \
            warehouse/de6_warehouse.py

        echo "======================================"
        echo "DE6 Warehouse Processing Completed"
        echo "======================================"
        """
    )

    @task
    def warehouse_complete():

        logging.info(
            "DE6 warehouse pipeline completed successfully."
        )

    check = check_analytics_output()

    check >> run_warehouse >> warehouse_complete()


de6_network_warehouse()