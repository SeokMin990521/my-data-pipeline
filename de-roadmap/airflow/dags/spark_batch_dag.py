from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'hsm_data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    '15_spark_batch_postgres_dw_pipeline',
    default_args=default_args,
    description='Docker Airflow 환경 기반의 Spark 배치 파이프라인',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    infra_check = BashOperator(
        task_id='check_data_lake_directory',
        bash_command='ls -ld /opt/airflow/de-roadmap/minio_data && echo "🐳 데이터 레이크 상태 양호!"'
    )

    run_spark_batch = BashOperator(
        task_id='execute_pyspark_batch_clean',
        bash_command='python3 /opt/airflow/de-roadmap/spark_batch_clean.py'
    )

    infra_check >> run_spark_batch
