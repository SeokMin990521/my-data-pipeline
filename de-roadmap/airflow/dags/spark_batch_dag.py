from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'hsm_data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'spark_batch_clean_pipeline',
    default_args=default_args,
    description='파이썬 코드로 S3A 커넥터를 안전하게 자가 다운로드하여 구동하는 파이프라인',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    infra_check = BashOperator(
        task_id='infra_health_check',
        bash_command='echo "Starting Spark Batch Job on Airflow. Python Self-Downloading S3A Jars..."'
    )

    # 💡 wget이 없으므로 파이썬 내장 urllib을 이용해 스파크 jars 폴더에 파일을 정밀 타격하여 다운로드합니다.
    run_spark_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command='''
        python3 -m pip install --no-cache-dir pyspark==3.4.1 && \
        SPARK_JARS_DIR=$(python3 -c "import pyspark, os; print(os.path.join(pyspark.__path__[0], 'jars'))") && \
        python3 -c "
import urllib.request, os
jars_dir = '$SPARK_JARS_DIR'
hadoop_jar = os.path.join(jars_dir, 'hadoop-aws-3.3.4.jar')
aws_jar = os.path.join(jars_dir, 'aws-java-sdk-bundle-1.12.262.jar')

if not os.path.exists(hadoop_jar):
    print('Downloading hadoop-aws...')
    urllib.request.urlretrieve('https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar', hadoop_jar)

if not os.path.exists(aws_jar):
    print('Downloading aws-java-sdk-bundle...')
    urllib.request.urlretrieve('https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar', aws_jar)
" && \
        cd /opt/de-roadmap && \
        python3 spark_batch_clean.py
        '''
    )

    infra_check >> run_spark_job
