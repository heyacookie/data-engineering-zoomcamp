import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

def download_upload_dag(
    dag, url_template, local_file_path_template, gcs_path_template
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template}.csv > {local_file_path_template}.csv"
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{local_file_path_template}.csv"
            }
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"{gcs_path_template}.parquet",
                "local_file": f"{local_file_path_template}.parquet",
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_file_path_template}.csv {local_file_path_template}.parquet"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/csv_backup'
DAG_RUN_YEAR = '{{ dag_run.logical_date.strftime(\'%Y\') }}'

# Yellow Taxi DAG
YELLOW_TAXI_FILE = 'yellow_tripdata_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}'
YELLOW_TAXI_URL_TEMPLATE = f"{URL_PREFIX}/{YELLOW_TAXI_FILE}"
YELLOW_TAXI_LOCAL_FILE_TEMPLATE = f"{AIRFLOW_HOME}/{YELLOW_TAXI_FILE}"
YELLOW_TAXI_GCS_PATH_TEMPLATE = f"raw/yellow_tripdata/{DAG_RUN_YEAR}/{YELLOW_TAXI_FILE}"

yellow_taxi_data_dag = DAG(
    dag_id="yellow_taxi_data",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
)

download_upload_dag(
    dag=yellow_taxi_data_dag,
    url_template=YELLOW_TAXI_URL_TEMPLATE,
    local_file_path_template=YELLOW_TAXI_LOCAL_FILE_TEMPLATE,
    gcs_path_template=YELLOW_TAXI_GCS_PATH_TEMPLATE
)

# Green Taxi DAG
GREEN_TAXI_FILE = 'green_tripdata_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}'
GREEN_TAXI_URL_TEMPLATE = f"{URL_PREFIX}/{GREEN_TAXI_FILE}"
GREEN_TAXI_LOCAL_FILE_TEMPLATE = f"{AIRFLOW_HOME}/{GREEN_TAXI_FILE}"
GREEN_TAXI_GCS_PATH_TEMPLATE = f"raw/green_tripdata/{DAG_RUN_YEAR}/{GREEN_TAXI_FILE}"

green_taxi_data_dag = DAG(
    dag_id="green_taxi_data",
    schedule_interval="0 7 2 * *",
    start_date=datetime(2019, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
)

download_upload_dag(
    dag=green_taxi_data_dag,
    url_template=GREEN_TAXI_URL_TEMPLATE,
    local_file_path_template=GREEN_TAXI_LOCAL_FILE_TEMPLATE,
    gcs_path_template=GREEN_TAXI_GCS_PATH_TEMPLATE
)

# FHV Taxi DAG
FHV_TAXI_FILE = 'fhv_tripdata_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}'
FHV_TAXI_URL_TEMPLATE = f"{URL_PREFIX}/{FHV_TAXI_FILE}"
FHV_TAXI_LOCAL_FILE_TEMPLATE = f"{AIRFLOW_HOME}/{FHV_TAXI_FILE}"
FHV_TAXI_GCS_PATH_TEMPLATE = f"raw/fhv_tripdata/{DAG_RUN_YEAR}/{FHV_TAXI_FILE}"

fhv_taxi_data_dag = DAG(
    dag_id="fhv_taxi_data",
    schedule_interval="0 8 2 * *",
    start_date=datetime(2019, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
)

download_upload_dag(
    dag=fhv_taxi_data_dag,
    url_template=FHV_TAXI_URL_TEMPLATE,
    local_file_path_template=FHV_TAXI_LOCAL_FILE_TEMPLATE,
    gcs_path_template=FHV_TAXI_GCS_PATH_TEMPLATE
)

# Zones
ZONES_URL_TEMPLATE = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
ZONES_LOCAL_FILE_TEMPLATE = AIRFLOW_HOME + '/taxi_zone_lookup.csv'
ZONES_GCS_PATH_TEMPLATE = "raw/taxi_zone/taxi_zone_lookup.csv"

zones_data_dag = DAG(
    dag_id="zones_data",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
)

download_upload_dag(
    dag=zones_data_dag,
    url_template=ZONES_URL_TEMPLATE,
    local_file_path_template=ZONES_LOCAL_FILE_TEMPLATE,
    gcs_path_template=ZONES_GCS_PATH_TEMPLATE
)