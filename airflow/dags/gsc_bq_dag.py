import os
import logging
from re import M

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

TRIP_TYPES = {
    'yellow': 'tpep_pickup_datetime',
    'green': 'lpep_pickup_datetime',
    # 'fhv': 'Pickup_datetime'
}

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="gcs_bg_tlc_data",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for trip_type, dt_col in TRIP_TYPES.items():

        gcs_to_bq_ext_task = BigQueryCreateExternalTableOperator(
            task_id=f"gsc_to_bq_{trip_type}_ext_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{trip_type}_external",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/raw/{trip_type}_tripdata/*"],
                }
            },
        )

        CREATE_PARTITION_TABLE = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{trip_type}_partitioned "
            f"PARTITION BY DATE({dt_col}) "
            f"AS SELECT * FROM {BIGQUERY_DATASET}.{trip_type}_external;"
        )

        bq_partition_task = BigQueryInsertJobOperator(
            task_id=f"bq_{trip_type}_partition_task",
            configuration={
                "query": {
                    "query": CREATE_PARTITION_TABLE,
                    "useLegacySql": False,
                }
            }
        )

        gcs_to_bq_ext_task >> bq_partition_task
