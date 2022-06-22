import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator


from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def gcs_to_bq_dag(
    dag, trip_type, dtime_field
):
    with dag:
        gcs_to_bq_ext_task = BigQueryCreateExternalTableOperator(
            task_id="gsc_to_bg_ext_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{trip_type}_external",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/raw/{trip_type}_tripdata/2019/*"]
                }
            },
        )

        CREATE_PARTITION_TABLE = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{trip_type}_partitioned "
            f"PARTITION BY DATE({dtime_field}) "
            f"AS SELECT * FROM {BIGQUERY_DATASET}.{trip_type}_external;"
        )

        bg_partition_task = BigQueryInsertJobOperator(
            task_id="bg_partition_task",
            configuration={
                "query": {
                    "query": CREATE_PARTITION_TABLE,
                    "useLegacySql": False,
                }
            }
        )

        gcs_to_bq_ext_task >> bg_partition_task

# Yellow Taxi DAG
gcs_bg_yellow_taxi_dag = DAG(
    dag_id="gcs_bg_yellow_taxi_v0.3",
    schedule_interval="0 6 3 * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['dtc-de'],
)

gcs_to_bq_dag(
    dag=gcs_bg_yellow_taxi_dag,
    trip_type="yellow",
    dtime_field='tpep_pickup_datetime',
)

# Green Taxi DAG
gcs_bg_green_taxi_dag = DAG(
    dag_id="gcs_bg_green_taxi_v1.1",
    schedule_interval="0 7 3 * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['dtc-de'],
)

gcs_to_bq_dag(
    dag=gcs_bg_green_taxi_dag,
    trip_type="green",
    dtime_field='lpep_pickup_datetime',
)

# FHV Taxi DAG
gcs_bg_fhv_taxi_dag = DAG(
    dag_id="gcs_bg_fhv_taxi_v1.1",
    schedule_interval="0 8 3 * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['dtc-de'],
)

gcs_to_bq_dag(
    dag=gcs_bg_fhv_taxi_dag,
    trip_type="fhv",
    dtime_field='tpep_pickup_datetime',
)