
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

PROJECT_ID = "godaddy-auctions-inventory-v11"
REGION = "europe-west9"
BUCKET = "godaddy-auctions-v11-data-49757842710"

with DAG(
    dag_id="godaddy_auctions_pipeline",
    description="GoDaddy Auctions V1.1 daily ingestion and dbt pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["godaddy", "auctions", "v1.1"],
) as dag:

    # 1. Run ingestion for the Airflow logical date
    run_ingestion = CloudRunExecuteJobOperator(
        task_id="run_ingestion",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="godaddy-auctions-processor",
                overrides={
            "container_overrides": [
                {
                    "args": ["--snapshot-date={{ ds }}"]
                }
            ]
        },
        deferrable=False,
    )

    # 2. Load the daily Parquet file into its matching partition
    load_bigquery = BigQueryInsertJobOperator(
        task_id="load_bigquery",
        project_id=PROJECT_ID,
        location=REGION,
        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{BUCKET}/transformed/snapshots/{{{{ ds }}}}/stg_auction_inventory.parquet"
                ],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": "godaddy_auctions",
                    "tableId": "auction_inventory${{ ds_nodash }}",
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_NEVER",
            }
        },
    )

    # 3. Run dbt models and tests
    run_dbt = CloudRunExecuteJobOperator(
        task_id="run_dbt",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="godaddy-auctions-dbt",
        deferrable=False,
    )

    run_ingestion >> load_bigquery >> run_dbt
