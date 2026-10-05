from datetime import datetime
from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

@dag(
    dag_id="de7_end_to_end",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["DE7", "end-to-end", "orchestration"],
)

def orchestrator():

    ingest = TriggerDagRunOperator(
        task_id="ingest",
        trigger_dag_id="de2_landing_to_raw",
        wait_for_completion=True,
        poke_interval=10,
    )

    spark_process = TriggerDagRunOperator(
        task_id="spark_process",
        trigger_dag_id="de3_spark_processing",
        wait_for_completion=True,
        poke_interval=10,
    )

    load_warehouse = TriggerDagRunOperator(
        task_id="load_warehouse",
        trigger_dag_id="de6_network_warehouse",
        wait_for_completion=True,
        poke_interval=10,
    )

    ingest >> spark_process >> load_warehouse

orchestrator()

