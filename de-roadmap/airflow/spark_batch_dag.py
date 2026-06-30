from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# [취업 포트폴리오용 실무형 기본 설정]
default_args = {
    'owner': 'hsm_data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 13),  # 15일차 미션 가동일 기준
    'retries': 1,                         # 실패 시 딱 1번만 재시도
    'retry_delay': timedelta(minutes=2),  # 재시도 대기 시간 2분
}

with DAG(
    '15_spark_batch_postgres_dw_pipeline',
    default_args=default_args,
    description='Docker Airflow 환경 기반의 Spark 배치 파이프라인 자동화',
    schedule_interval='@hourly',          # 매시간 정각마다 자동 배치 모사
    catchup=False                         # 과거 밀린 배치 잡 실행 방지
) as dag:

    # [Task 1] 마운트된 데이터 레이크 저장소 상태 체크
    infra_check = BashOperator(
        task_id='check_data_lake_directory',
        bash_command='ls -ld /opt/airflow/de-roadmap/minio_data && echo "🐳 데이터 레이크 상태 양호!"'
    )

    # [Task 2] 컨테이너 내부의 파이썬 엔진으로 호스트의 정제 코드 다이렉트 실행
    run_spark_batch = BashOperator(
        task_id='execute_pyspark_batch_clean',
        bash_command='python3 /opt/airflow/de-roadmap/spark_batch_clean.py'
    )

    # 파이프라인 실행 순서 정의 (체크 완료 후 정제 가동)
    infra_check >> run_spark_batch
