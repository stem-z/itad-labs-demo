from datetime import datetime, timedelta
import os
import duckdb
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Настройки строго под ваш вариант (Airbnb)
LOCAL_FILE_PATH = "/opt/airflow/data/source/listings.csv"
S3_BUCKET = "raw"
S3_KEY = "listings/listings.csv"

default_args = {
    'owner': 'student',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def upload_to_s3(**kwargs):
    print(f"Начинаем загрузку файла {LOCAL_FILE_PATH} в S3...")
    if not os.path.exists(LOCAL_FILE_PATH):
        raise FileNotFoundError(f"Файл не найден: {LOCAL_FILE_PATH}")
    
    hook = S3Hook(aws_conn_id='S3_CONN')
    hook.load_file(filename=LOCAL_FILE_PATH, key=S3_KEY, bucket_name=S3_BUCKET, replace=True)
    print(f"Файл успешно загружен в s3://{S3_BUCKET}/{S3_KEY}")

def verify_duckdb(**kwargs):
    print("Проверка файла через DuckDB...")
    try:
        con = duckdb.connect()
        df = con.execute(f"SELECT * FROM '{LOCAL_FILE_PATH}' LIMIT 5").df()
        print("Файл успешно прочитан! Первые 5 строк:")
        print(df)
    except Exception as e:
        print(f"Ошибка при чтении файла через DuckDB: {e}")
        raise

with DAG(
    dag_id='ingest_raw',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['lab2', 'airbnb'],
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=LOCAL_FILE_PATH,
        poke_interval=10,
        timeout=60,
    )

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
    )

    verify_task = PythonOperator(
        task_id='verify_duckdb',
        python_callable=verify_duckdb,
    )

    wait_for_file >> upload_task >> verify_task