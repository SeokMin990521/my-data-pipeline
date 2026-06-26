
except KeyboardInterrupt:
    print("\n🛑 사용자에 의해 컨슈머가 정지되었습니다. 남은 버퍼를 정리합니다.")
    if data_buffer:
        upload_to_minio(data_buffer)
finally:
    consumer.close()
    print("👋 데이터 레이크 적재 시스템이 안전하게 종료되었습니다.")
EOF

python consumer_to_s3.py
# 1. 에러가 나던 import 라인을 정상 규격으로 교체합니다.
sed -i "s/from confluent_kafka import Deserializer, KafkaError/from confluent_kafka import KafkaError/g" consumer_to_s3.py
# 2. 적재 엔진 다시 가동!
python consumer_to_s3.py
# 1. 틀린 변수명을 올바른 boto3 규격(aws_secret_access_key)으로 교체합니다.
sed -i "s/aws_secret_key_id=/aws_secret_access_key=/g" consumer_to_s3.py
# 2. 적재 엔진 다시 가동!
python consumer_to_s3.py
cat << 'EOF' > consumer_to_s3.py
import time
import json
import io
from datetime import datetime
import boto3
from botocore.client import Config
from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

# 1. 환경 설정 주소
SR_URL = "http://127.0.0.1:8081"
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "avro-user-events"

MINIO_URL = "http://127.0.0.1:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET_NAME = "raw-data-lake"

print("📥 [13일차] Kafka to 데이터 레이크(MinIO) 최신형 적재 엔진 가동...")

# 2. MinIO(S3 호환) 클라이언트 연결
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_URL,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

# 3. Schema Registry 클라이언트 및 최신형 역직렬화 도구 세팅
sr_client = SchemaRegistryClient({"url": SR_URL})

with open("user_event.avsc", "r") as f:
    schema_str = f.read()

avro_deserializer = AvroDeserializer(
    schema_registry_client=sr_client,
    schema_str=schema_str
)
string_deserializer = StringDeserializer('utf_8')

# 4. 신형 디시리얼라이징 컨슈머 생성 (호환성 경고 완벽 제거)
consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 's3-sink-connector-group-v2', # 그룹 ID 변경으로 처음부터 다시 읽기
    'auto.offset.reset': 'earliest',
    'key.deserializer': string_deserializer,
    'value.deserializer': avro_deserializer
}
consumer = DeserializingConsumer(consumer_config)
consumer.subscribe([TOPIC_NAME])

# 5. 배치 처리를 위한 버퍼 바구니 및 타이머 세팅
data_buffer = []
BUFFER_SIZE_LIMIT = 50       
TIME_LIMIT_SECONDS = 10      
last_flush_time = time.time()

def upload_to_minio(data_list):
    """버퍼에 쌓인 데이터를 시계열 파티셔닝 경로에 JSON 파일로 덤프"""
    if not data_list:
        return

    now = datetime.now()
    partition_path = f"year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}/hour={now.strftime('%H')}"
    file_name = f"events_{now.strftime('%Y%m%d_%H%M%S')}_{int(time.time())}.json"
    full_s3_key = f"{partition_path}/{file_name}"

    json_lines = "\n".join([json.dumps(record, ensure_ascii=False) for record in data_list])
    
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=full_s3_key,
            Body=json_lines.encode('utf-8')
        )
        print(f"💾 [데이터 레이크 적재 완료] {len(data_list)}건 ➡️ S3://{BUCKET_NAME}/{full_s3_key}")
    except Exception as e:
        print(f"❌ MinIO 업로드 실패 에러: {e}")

# 6. 실시간 스트리밍 적재 루프
print(f"🌟 24시간 감시망 가동... 덤프 조건: {BUFFER_SIZE_LIMIT}건 적재 또는 {TIME_LIMIT_SECONDS}초 경과")
print("-" * 60)

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            if time.time() - last_flush_time >= TIME_LIMIT_SECONDS and data_buffer:
                print("⏱️ [타임아웃] 10초간 유입이 없어 현재 버퍼를 데이터 레이크로 덤프합니다.")
                upload_to_minio(data_buffer)
                data_buffer.clear()
                last_flush_time = time.time()
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"❌ 카프카 컨슈머 에러: {msg.error()}")
                break

        # 순수한 파이썬 딕셔너리 데이터 버퍼에 누적
        user_event


ls
sudo nano consumer_to_s3.py
python consumer_to_s3.py
sudo nano consumer_to_s3.py
python consumer_to_s3.py
sudo nano consumer_to_s3.py
python consumer_to_s3.py
pip install pyspark
cat << 'EOF' > spark_batch_clean.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp

# 1. 환경 설정 변수
MINIO_URL = "http://127.0.0.1:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

RAW_BUCKET = "raw-data-lake"
MART_BUCKET = "analytics-data-mart" # 🌟 최종 목적지

print("⚡ [14일차] Apache Spark 분산 배치 정제 엔진 초기화 중...")

# 2. S3(MinIO) 연동용 하둡 패키지가 탑재된 Spark 세션 생성
spark = SparkSession.builder \
    .appName("Deroadmap-Spark-Batch-Clean") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
    .config("fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .master("local[*]") \
    .getOrCreate()

# 로그 레벨을 WARN으로 낮춰 지저분한 자바 로그 차단
spark.sparkContext.setLogLevel("WARN")
print("🚀 Spark 분산 커널 세션 연결 완수!")

try:
    # 3. 데이터 레이크로부터 하이브 파티셔닝 JSON 데이터를 한 번에 스캔 (분산 로드)
    raw_s3_path = f"s3a://{RAW_BUCKET}/year=*/*/*/*/*.json"
    print(f"📥 [Data Lake 스캔] {raw_s3_path} 로딩 중...")
    df = spark.read.json(raw_s3_path)
    
    print("📋 [Raw 데이터 카운트 및 스키마 분석]")
    total_count = df.count()
    print(f"📊 총 유입 데이터 로우 수: {total_count}건")
    df.printSchema()

    # 4. 🛠️ 데이터 정제 및 12일차 실데이터 동기화 가공
    # - collected_at 문자열을 실제 Spark Timestamp 타입으로 변환
    # - 이메일 도메인 중 'data-lake.org'를 사용하는 유저에게 가중치 1 부여 (비즈니스 로직 모사)
    print("🧹 정제 및 비즈니스 마트 스키마 변환 가동...")
    cleaned_df = df \
        .withColumn("event_time", to_timestamp(col("collected_at"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("is_conversion", when(col("email").contains("data-lake.org"), 1).otherwise(0)) \
        .drop("collected_at") # 구 컬럼 제거

    print("✨ 정제 및 가공 완료 스키마 구조:")
    cleaned_df.printSchema()
    cleaned_df.show(5) # 상위 5건 샘플 대시보드 출력

    # 5. 💾 최적화 압축 포맷(Parquet)으로 데이터 웨어하우스 마트 적재
    mart_s3_path = f"s3a://{MART_BUCKET}/user_conversions"
    print(f"💾 [DW 마트 적재 완료] 최적화 압축 Parquet 포맷 변환 중 ➡️ {mart_s3_path}")
    
    cleaned_df.write \
        .mode("overwrite") \
        .parquet(mart_s3_path)
        
    print("🎯 [14일차 미션 성공] Apache Spark 정제 파이프라인 배치가 완벽하게 끝났습니다!")

except Exception as e:
    print(f"❌ 스파크 배치 연산 중 에러 발생: {e}")
finally:
    spark.stop()
EOF

python spark_batch_clean.py
sudo apt update
sudo apt install -y default-jdk
# 환경변수 임시 등록 (현재 터미널용)
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
export PATH=$PATH:$JAVA_HOME/bin
# 잘 등록되었는지 확인 (경로가 출력되어야 합니다)
echo $JAVA_HOME
python spark_batch_clean.py
sudo apt update
sudo apt install -y openjdk-11-jdk
sudo update-alternatives --config java
sudo update-alternatives --config javac
# 자바 11 전용 경로로 환경변수 갱신
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin
# 11 버전으로 잘 바뀌었는지 검증
java -version
python spark_batch_clean.py
sudo nano docker-compose.yml
docker compose up -d spark
cat << 'EOF' > spark_batch_clean.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp

# 1. 🌟 도커 내부망 전용 주소 튜닝
MINIO_URL = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

RAW_BUCKET = "raw-data-lake"
MART_BUCKET = "analytics-data-mart"

print("⚡ [14일차] 도커 컨테이너 내부에서 Spark 분산 엔진 가동 중...")

# 2. S3(MinIO) 연동 세션 생성
spark = SparkSession.builder \
    .appName("Deroadmap-Spark-Batch-Clean") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
    .config("fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("🚀 Spark 분산 커널 세션 연결 완수!")

try:
    # 3. 데이터 레이크 스캔
    raw_s3_path = f"s3a://{RAW_BUCKET}/year=*/*/*/*/*.json"
    print(f"📥 [Data Lake 스캔] {raw_s3_path} 로딩 중...")
    df = spark.read.json(raw_s3_path)
    
    print("📋 [Raw 데이터 카운트 및 스키마 분석]")
    print(f"📊 총 유입 데이터 로우 수: {df.count()}건")
    df.printSchema()

    # 4. 데이터 정제 가공
    print("🧹 정제 및 비즈니스 마트 스키마 변환 가동...")
    cleaned_df = df \
        .withColumn("event_time", to_timestamp(col("collected_at"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("is_conversion", when(col("email").contains("data-lake.org"), 1).otherwise(0)) \
        .drop("collected_at")

    print("✨ 정제 및 가공 완료 스키마 구조:")
    cleaned_df.printSchema()
    cleaned_df.show(5)

    # 5. Parquet 포맷으로 최종 적재
    mart_s3_path = f"s3a://{MART_BUCKET}/user_conversions"
    print(f"💾 [DW 마트 적재 완료] 최적화 압축 Parquet 포맷 변환 중 ➡️ {mart_s3_path}")
    
    cleaned_df.write \
        .mode("overwrite") \
        .parquet(mart_s3_path)
        
    print("🎯 [14일차 미션 성공] Apache Spark 컨테이너 연산 배치가 완벽하게 성공했습니다!")

except Exception as e:
    print(f"❌ 스파크 배치 연산 중 에러 발생: {e}")
finally:
    spark.stop()
EOF

docker exec -it my-spark spark-submit /opt/bitnami/spark/app/spark_batch_clean.py
[200~# 1. 스파크 이미지를 가장 안정적인 최신 3 버전 규격으로 변경합니다.
sed -i "s/image: bitnami\/spark:3.4.1/image: bitnami\/spark:3/g" docker-compose.yml
# 2. 다시 스파크 컨테이너 기동!
docker compose up -d spark
# 3. 컨테이너가 잘 떴는지 확인
docker compose ps spark~
docker exec -it my-spark spark-submit /opt/bitnami/spark/app/spark_batch_clean.py
# 1. 도커 컴포즈 파일의 스파크 이미지를 검증된 3.5.1 버전으로 강제 교체합니다.
sed -i "s/image: bitnami\/spark:.*/image: bitnami\/spark:3.5.1/g" docker-compose.yml
# 2. 확실하게 인프라 반영 및 빌드 구동
docker compose up -d spark
# 3. 컨테이너가 정상적으로 Up 되었는지 눈으로 확인
docker compose ps spark
# 1. 컴포즈 파일의 스파크 이미지를 공식 apache/spark 규격으로 완벽하게 교체합니다.
sed -i "s/image: bitnami\/spark:.*/image: apache\/spark:3.5.0/g" docker-compose.yml
# 2. 공식 엔진으로 다시 기동!
docker compose up -d spark
# 3. 웅장하게 컨테이너가 떴는지 상태 점검
docker compose ps spark
# 1. 컴포즈 파일에서 충돌 나던 8085 포트를 빈 포트인 8089로 교체합니다.
sed -i "s/\"8085:8080\"/\"8089:8080\"/g" docker-compose.yml
# 2. 깨끗해진 포트로 다시 스파크 기동!
docker compose up -d spark
# 3. 웅장하게 올라왔는지 상태 체크
docker compose ps spark
docker exec -it my-spark spark-submit /opt/bitnami/spark/app/spark_batch_clean.py
cd ~/de-roadmap
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
cat << 'EOF' > spark_batch_clean.py
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp

# 1. 로컬 환경 변수 명시 (127.0.0.1 조준)
MINIO_URL = "http://127.0.0.1:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
RAW_BUCKET = "raw-data-lake"
MART_BUCKET = "analytics-data-mart"

print("⚡ [14일차] 호스트 로컬 자바 11 기반 Spark 엔진 가동 중...")

# 2. Spark 세션 기동
spark = SparkSession.builder \
    .appName("Deroadmap-Spark-Batch-Clean") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
    .config("fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("🚀 Spark 분산 커널 세션 연결 완수!")

try:
    # 3. 데이터 레이크 로드
    raw_s3_path = f"s3a://{RAW_BUCKET}/year=*/*/*/*/*.json"
    print(f"📥 [Data Lake 스캔] {raw_s3_path} 로딩 중...")
    df = spark.read.json(raw_s3_path)
    
    print("📋 [Raw 데이터 카운트 및 스키마 분석]")
    print(f"📊 총 유입 데이터 로우 수: {df.count()}건")
    df.printSchema()

    # 4. 데이터 정제 가공
    print("🧹 정제 및 비즈니스 마트 스키마 변환 가동...")
    cleaned_df = df \
        .withColumn("event_time", to_timestamp(col("collected_at"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("is_conversion", when(col("email").contains("data-lake.org"), 1).otherwise(0)) \
        .drop("collected_at")

    print("✨ 정제 및 가공 완료 스키마 구조:")
    cleaned_df.printSchema()
    cleaned_df.show(5)

    # 5. Parquet 포맷으로 최종 적재
    mart_s3_path = f"s3a://{MART_BUCKET}/user_conversions"
    print(f"💾 [DW 마트 적재 완료] 최적화 압축 Parquet 포맷 변환 중 ➡️ {mart_s3_path}")
    
    cleaned_df.write \
        .mode("overwrite") \
        .parquet(mart_s3_path)
        
    print("🎯 [14일차 미션 성공] Apache Spark 정제 파이프라인 배치가 완벽하게 성공했습니다!")

except Exception as e:
    print(f"❌ 스파크 배치 연산 중 에러 발생: {e}")
finally:
    spark.stop()
EOF

sudo nano spark_batch_clean.py
# 환경 변수 재선언 후 슛
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_HOME=~/de-roadmap/spark-3.5.0-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin
# 대망의 다이렉트 연산 점화
python spark_batch_clean.py
# 1. 기존의 맛이 간 spark 서비스 블록을 도커 컴포즈 파일에서 깔끔하게 도려내고 새로 정의합니다.
cat << 'EOF' > update_spark.py
with open("docker-compose.yml", "r") as f:
    lines = f.readlines()

# spark 관련 내용이 들어갈 자리를 새로 갈아끼우기 위해 기존 spark 블록 위치 찾기
new_lines = []
skip = False
for line in lines:
    if "spark:" in line:
        skip = True
        continue
    if skip and line.startswith("  ") and not line.startswith("    "):
        if "networks:" in line or "volumes:" in line or "services:" in line:
            skip = False
    if not skip:
        new_lines.append(line)

# 맨 끝 services 닫히기 전에 수정한 완벽한 spark 컨테이너 스펙 주입
content = "".join(new_lines)
spark_spec = """
  spark:
    image: apache/spark:3.5.0
    container_name: my-spark
    user: root
    entrypoint: ["/bin/bash", "-c", "tail -f /dev/null"]
    volumes:
      - .:/opt/spark/app
    networks:
      - data-network
"""
# 적절한 위치에 주입 (networks: 위에 넣기)
if "networks:" in content:
    content = content.replace("networks:", spark_spec + "\nnetworks:")
else:
    content += spark_spec

with open("docker-compose.yml", "w") as f:
    f.write(content)
print("✅ docker-compose.yml 스파크 스펙 완벽 수정 완료!")
EOF

python update_spark.py && rm update_spark.py
# 1. 수정된 도커 인프라로 스파크를 백그라운드에 완전히 상주시킵니다.
docker compose up -d spark
# 2. 컨테이너 내부망 통신용 스크립트로 덮어쓰기
cat << 'EOF' > spark_batch_clean.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp

MINIO_URL = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
RAW_BUCKET = "raw-data-lake"
MART_BUCKET = "analytics-data-mart"

print("⚡ [14일차] 스파크 컨테이너 내부(안정환경)에서 분산 배치 엔진 가동 중...")

spark = SparkSession.builder \
    .appName("Deroadmap-Spark-Batch-Clean") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
    .config("fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("🚀 Spark 분산 커널 세션 연결 완수!")

try:
    raw_s3_path = f"s3a://{RAW_BUCKET}/year=*/*/*/*/*.json"
    print(f"📥 [Data Lake 스캔] {raw_s3_path} 로딩 중...")
    df = spark.read.json(raw_s3_path)
    
    print("📋 [Raw 데이터 카운트 및 스키마 분석]")
    print(f"📊 총 유입 데이터 로우 수: {df.count()}건")
    df.printSchema()

    print("🧹 정제 및 비즈니스 마트 스키마 변환 가동...")
    cleaned_df = df \
        .withColumn("event_time", to_timestamp(col("collected_at"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("is_conversion", when(col("email").contains("data-lake.org"), 1).otherwise(0)) \
        .drop("collected_at")

    print("✨ 정제 및 가공 완료 스키마 구조:")
    cleaned_df.printSchema()
    cleaned_df.show(5)

    mart_s3_path = f"s3a://{MART_BUCKET}/user_conversions"
    print(f"💾 [DW 마트 적재 완료] 최적화 압축 Parquet 포맷 변환 중 ➡️ {mart_s3_path}")
    
    cleaned_df.write \
        .mode("overwrite") \
        .parquet(mart_s3_path)
        
    print("🎯 [14일차 미션 성공] Apache Spark 정제 파이프라인 배치가 완벽하게 성공했습니다!")

except Exception as e:
    print(f"❌ 스파크 배치 연산 중 에러 발생: {e}")
finally:
    spark.stop()
EOF

# 1. 만약의 사태를 대비해 기존 컴포즈 파일 백업
cp docker-compose.yml docker-compose.yml.bak
# 2. 파이썬 코드로 services: 키워드 바로 아랫줄에 정밀하게 공백 2칸 들여쓰기로 주입
cat << 'EOF' > fix_yaml.py
with open("docker-compose.yml", "r") as f:
    content = f.read()

# 기존에 꼬여서 잘못 들어간 spark 블록이나 찌꺼기 완벽 도려내기
lines = content.split("\n")
cleaned_lines = [line for line in lines if "spark" not in line and "image: apache/spark" not in line and "container_name: my-spark" not in line and "/opt/spark/app" not in line and "tail -f" not in line]
content = "\n".join(cleaned_lines)

# 올바른 위치(services: 내부)에 띄어쓰기 2칸 규격으로 스파크 엔진 주입
spark_fixed_spec = """services:
  spark:
    image: apache/spark:3.5.0
    container_name: my-spark
    user: root
    entrypoint: ["/bin/bash", "-c", "tail -f /dev/null"]
    volumes:
      - .:/opt/spark/app
    networks:
      - data-network"""

if "services:" in content:
    content = content.replace("services:", spark_fixed_spec)

with open("docker-compose.yml", "w") as f:
    f.write(content)
print("🎯 docker-compose.yml 문법 및 들여쓰기 교정 완료!")
EOF

python fix_yaml.py && rm fix_yaml.py
# 1. 꼬인 엔진 정상 기동
docker compose up -d spark
# 2. 완벽하게 격리된 도커 내부 안정 환경에서 스파크 연산 점화!
docker exec -it my-spark spark-submit /opt/spark/app/spark_batch_clean.py
cat << 'EOF' > docker-compose.yml
version: '3.8'

services:
  # 11일차: 데이터 저장소 및 레지스트리
  zookeeper:
    image: confluentinc/cp-zookeeper:7.3.0
    container_name: my-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - data-network

  kafka:
    image: confluentinc/cp-kafka:7.3.0
    container_name: my-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://127.0.0.1:9092,PLAINTEXT_INTERNAL://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    networks:
      - data-network

  schema-registry:
    image: confluentinc/cp-schema-registry:7.3.0
    container_name: my-schema-registry
    depends_on:
      - kafka
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    networks:
      - data-network

  # 13일차: 오브젝트 스토리지 (데이터 레이크)
  minio:
    image: minio/minio:RELEASE.2023-08-29T23-07-35Z
    container_name: my-minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: password123
    command: server /data --console-address ":9001"
    networks:
      - data-network

  # 14일차: 대용량 분산 연산 엔진 (독립 실행 및 무한 대기 세팅)
  spark:
    image: apache/spark:3.5.0
    container_name: my-spark
    user: root
    entrypoint: ["/bin/bash", "-c", "tail -f /dev/null"]
    volumes:
      - .:/opt/spark/app
    networks:
      - data-network

networks:
  data-network:
    driver: bridge
EOF

# 꼬인 컨테이너들 완전히 내렸다가 깨끗하게 올리기
docker compose down
docker compose up -d
docker exec -it my-spark spark-submit /opt/spark/app/spark_batch_clean.py
sudo nano docker-compose.yml
docker exec -it my-spark /opt/spark/bin/spark-submit /opt/spark/app/spark_batch_clean.py
docker exec -it my-spark /opt/spark/bin/spark-submit   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262   /opt/spark/app/spark_batch_clean.py
cat << 'EOF' > spark_batch_clean.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp

# 1. 환경 설정 변수
MINIO_URL = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
RAW_BUCKET = "raw-data-lake"
MART_BUCKET = "analytics-data-mart"

# 🌟 PostgreSQL DW 연결 정보 (도커 내부망 주소 사용)
PG_URL = "jdbc:postgresql://postgres:5432/analytics_db"  # DB 환경에 맞게 조정 가능
PG_PROPERTIES = {
    "user": "postgres",
    "password": "password123",
    "driver": "org.postgresql.Driver"
}

print("⚡ [14일차] 스파크 컨테이너 내부에서 분산 배치 및 DW 적재 엔진 가동 중...")

spark = SparkSession.builder \
    .appName("Deroadmap-Spark-Batch-Clean") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
    .config("fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

try:
    # 2. 데이터 레이크 로드
    raw_s3_path = f"s3a://{RAW_BUCKET}/year=*/*/*/*/*.json"
    df = spark.read.json(raw_s3_path)
    
    print(f"📊 총 유입 데이터 로우 수: {df.count()}건")

    # 3. 데이터 정제 가공
    cleaned_df = df \
        .withColumn("event_time", to_timestamp(col("collected_at"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("is_conversion", when(col("email").contains("data-lake.org"), 1).otherwise(0)) \
        .drop("collected_at")

    # 4. Parquet 포맷으로 최종 적재 (데이터 레이크 마트)
    mart_s3_path = f"s3a://{MART_BUCKET}/user_conversions"
    print(f"💾 [1/2] 최적화 압축 Parquet 포맷 변환 및 적재 중 ➡️ {mart_s3_path}")
    cleaned_df.write.mode("overwrite").parquet(mart_s3_path)
        
    # 5. 🌟 데이터 웨어하우스(PostgreSQL) 고속 적재
    print("🔮 [2/2] PostgreSQL DW 분석용 테이블 고속 적재 가동...")
    cleaned_df.write.jdbc(
        url=PG_URL,
        table="bi_user_conversions",
        mode="overwrite",
        properties=PG_PROPERTIES
    )
    
    print("🎯 [14일차 미션 최종 완수] Spark 대용량 배치 정제부터 DW 적재까지 완벽하게 성공했습니다!")

except Exception as e:
    print(f"❌ 스파크 배치 연산 중 에러 발생: {e}")
finally:
    spark.stop()
EOF

docker exec -it my-spark /opt/spark/bin/spark-submit   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
# PostgreSQL 컨테이너 내부로 진입하여 테이블 확인 (컨테이너명이 다르면 수정)
docker exec -it my-postgres psql -U postgres -d analytics_db -c "SELECT * FROM bi_user_conversions LIMIT 5;"
# postgres 기본 데이터베이스로 접속해서 analytics_db 생성하기
docker exec -it my-postgres psql -U postgres -d postgres -c "CREATE DATABASE analytics_db;"
docker exec -it my-spark /opt/spark/bin/spark-submit   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
docker exec -it my-postgres psql -U postgres -d analytics_db -c "SELECT * FROM bi_user_conversions LIMIT 5;"
docker exec -it my-spark /opt/spark/bin/spark-submit   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
docker exec -it my-postgres psql -U postgres -d analytics_db -c "SELECT * FROM bi_user_conversions LIMIT 5;"
# raw-data-lake 버킷 생성 (이미 있으면 통과)
curl -X PUT http://127.0.0.1:9000/raw-data-lake      -H "Authorization: AWS4-HMAC-SHA256 ..."      --user "admin:password123"
# analytics-data-mart 버킷 생성
curl -X PUT http://127.0.0.1:9000/analytics-data-mart      -H "Authorization: AWS4-HMAC-SHA256 ..."      --user "admin:password123"
docker exec -it my-spark /opt/spark/bin/spark-submit   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
# 1. MinIO 관리자 계정 정보로 로컬 클라이언트 로그인 세팅
docker exec -it my-minio mc alias set myminio http://localhost:9000 admin password123
# 2. 스파크가 읽을 원시 데이터 레이크 버킷 생성
docker exec -it my-minio mc mb myminio/raw-data-lake
# 3. 스파크가 저장할 최종 데이터 마트 버킷 생성
docker exec -it my-minio mc mb myminio/analytics-data-mart
docker exec -it my-spark /opt/spark/bin/spark-submit   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
# 1. 로컬에 샘플 JSON 파일 생성
cat << 'EOF' > sample_event.json
{"email": "user123@data-lake.org", "collected_at": "2026-06-23 14:00:00", "event_type": "click", "user_id": "DE_TEST_99"}
EOF

# 2. MinIO 내부의 스파크가 원하는 날짜 파티션 경로로 강제 복사·적재
docker exec -it my-minio mc cp /opt/spark/app/sample_event.json myminio/raw-data-lake/year=2026/month=06/day=23/hour=14/sample_event.json
# 3. 사용한 로컬 임시 파일 삭제
rm sample_event.json
# 1. 호스트 PC 터미널에 샘플 데이터 다시 생성
cat << 'EOF' > sample_event.json
{"email": "user123@data-lake.org", "collected_at": "2026-06-23 14:00:00", "event_type": "click", "user_id": "DE_TEST_99"}
EOF

# 2. 호스트 PC에 있는 파일을 MinIO 컨테이너 내부(/tmp)로 직접 복사해서 밀어넣기 🌟
docker cp sample_event.json my-minio:/tmp/sample_event.json
# 3. MinIO 컨테이너 안에서 /tmp에 있는 파일을 mc 명령어로 버킷에 인젝션!
docker exec -it my-minio mc cp /tmp/sample_event.json myminio/raw-data-lake/year=2026/month=06/day=23/hour=14/sample_event.json
# 4. 사용한 흔적 정리
rm sample_event.json
docker exec -it my-spark /opt/spark/bin/spark-submit   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
docker inspect my-postgres | grep POSTGRES_PASSWORD
# 만약 진짜 패스워드가 postgres였다면 'password123'을 'postgres'로 바꿉니다.
# 본인의 실제 패스워드에 맞게 수정 후 슛!
sed -i 's/"password123"/"1234"/g' spark_batch_clean.py
docker exec -it my-spark /opt/spark/bin/spark-submit   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
docker exec -it my-spark /opt/spark/bin/spark-submit   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
# 꼬인 컨테이너들을 완전히 내렸다가 깨끗하게 재기동
docker compose down
docker compose up -d
# 1. 샘플 데이터 생성
cat << 'EOF' > sample_event.json
{"email": "user123@data-lake.org", "collected_at": "2026-06-23 14:00:00", "event_type": "click", "user_id": "DE_TEST_99"}
EOF

# 2. MinIO 내부로 배달
docker cp sample_event.json my-minio:/tmp/sample_event.json
# 3. mc 도구로 버킷에 정확한 날짜 파티션 경로로 주입
docker exec -it my-minio mc cp /tmp/sample_event.json myminio/raw-data-lake/year=2026/month=06/day=23/hour=14/sample_event.json
# 4. 정리
rm sample_event.json
docker exec -it my-spark /opt/spark/bin/spark-submit   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
docker exec -it my-postgres psql -U postgres -d postgres -c "CREATE DATABASE analytics_db;"
docker exec -it my-spark /opt/spark/bin/spark-submit   --driver-memory 1G   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
cat << 'EOF' > spark_batch_clean.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp

# 🌟 컨테이너 내부 가상망 주소 완전 고정
MINIO_URL = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
RAW_BUCKET = "raw-data-lake"
MART_BUCKET = "analytics-data-mart"

PG_URL = "jdbc:postgresql://postgres:5432/analytics_db"
PG_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",  # 💡 만약 inspect 시 다르게 나왔다면 그 패스워드로 수정
    "driver": "org.postgresql.Driver"
}

print("⚡ [Internal Network] 컨테이너 전용 고속 백본망 라우팅 개시...")

spark = SparkSession.builder \
    .appName("Deroadmap-Spark-Batch-Clean") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
    .config("fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .master("local[*]") \
    .getOrCreate()

# 멈춘 것처럼 보이지 않게 진행 로그 가시화 세팅
spark.sparkContext.setLogLevel("INFO")

try:
    raw_s3_path = f"s3a://{RAW_BUCKET}/year=*/*/*/*/*.json"
    print(f"📥 데이터 레이크 로딩 중: {raw_s3_path}")
    df = spark.read.json(raw_s3_path)
    
    print(f"📊 스캔 완료! 총 유입 로우 수: {df.count()}건")

    cleaned_df = df \
        .withColumn("event_time", to_timestamp(col("collected_at"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("is_conversion", when(col("email").contains("data-lake.org"), 1).otherwise(0)) \
        .drop("collected_at")

    mart_s3_path = f"s3a://{MART_BUCKET}/user_conversions"
    print("💾 [1/2] Parquet 고속 스토리지 저장 중...")
    cleaned_df.write.mode("overwrite").parquet(mart_s3_path)
        
    print("🔮 [2/2] PostgreSQL DW 엔진 적재 중...")
    cleaned_df.write.jdbc(url=PG_URL, table="bi_user_conversions", mode="overwrite", properties=PG_PROPERTIES)
    
    print("🎯 [14일차 미션 최종 완수] 인프라가 완벽하게 결합되었습니다!")

except Exception as e:
    print(f"❌ 스파크 배치 연산 중 에러 발생: {e}")
finally:
    spark.stop()
EOF

docker exec -it my-spark /opt/spark/bin/spark-submit   --driver-memory 1G   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
# 1. MinIO 버킷 2개 생성
docker exec -it my-minio mc alias set myminio http://localhost:9000 admin password123
docker exec -it my-minio mc mb myminio/raw-data-lake
docker exec -it my-minio mc mb myminio/analytics-data-mart
# 2. 호스트 PC에 가짜(Mock) JSON 데이터 생성
cat << 'EOF' > sample_event.json
{"email": "user123@data-lake.org", "collected_at": "2026-06-23 14:00:00", "event_type": "click", "user_id": "DE_TEST_99"}
EOF

# 3. 컨테이너 내부로 샘플 파일 복사
docker cp sample_event.json my-minio:/tmp/sample_event.json
# 4. 스파크가 탐색할 파티션 경로(`year=2026/...`)에 맞추어 인젝션
docker exec -it my-minio mc cp /tmp/sample_event.json myminio/raw-data-lake/year=2026/month=06/day=23/hour=14/sample_event.json
# 5. 임시 파일 정리
rm sample_event.json
docker exec -it my-spark /opt/spark/bin/spark-submit   --driver-memory 1G   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
docker exec -it my-postgres psql -U postgres -d postgres -c "CREATE DATABASE analytics_db;"
sed -i 's/"password": "postgres"/"password": "password123"/g' spark_batch_clean.py
sed -i 's/"password": "password123",/"password": "password123",/g' spark_batch_clean.py
docker exec -it my-spark /opt/spark/bin/spark-submit   --driver-memory 1G   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
sudo nano spark_batch_clean.py
docker exec -it my-spark /opt/spark/bin/spark-submit   --driver-memory 1G   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
docker inspect my-postgres | grep -i password
nano spark_batch_clean.py
docker exec -it my-spark /opt/spark/bin/spark-submit   --driver-memory 1G   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0   /opt/spark/app/spark_batch_clean.py
docker exec -it my-postgres psql -U postgres -d analytics_db -c "SELECT * FROM bi_user_conversions LIMIT 5;"
# 1. 실행 중인 도커 컴포즈 서비스 내리기 (컨테이너 메모리 즉시 반환)
docker compose down
# 2. (선택) 사용하지 않는 미사용 도커 볼륨·네트워크까지 일괄 삭제하여 디스크 용량 확보
docker system prune -a --volumes
rm -rf /tmp/spark-*
rm -rf /tmp/hadoop-*
# 현재 터미널에 임시로 잡힌 자바 메모리 및 경로 세팅 초기화
hash -r
java -version
sudo apt-get autoremove
cd
sudo apt-get update
sudo apt-get upgrade
sudo apt-get update
sudo apt-get upgrade
sudo apt-get autoremove
sudo apt-get upgrade
sudo apt-get update
sudo apt-get upgrade
sudo apt-get autoremove
# 1. Airflow 작업 디렉토리 생성 및 이동
cd ~/de-roadmap
mkdir -p airflow/dags airflow/logs airflow/plugins
cd airflow
# 2. 권한 에러 사전 차단 (매우 중요)
chmod -R 777 dags logs plugins
# 3. 경량화된 도커 컴포즈 파일 작성
cat << 'EOF' > docker-compose.yaml
version: '3.8'
services:
  airflow-webserver:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-webserver
    restart: always
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__SECRET_KEY=my_super_secret_key_12345
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ../:/opt/de-roadmap
    ports:
      - "8089:8080"
    command: webserver

  airflow-scheduler:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-scheduler
    restart: always
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ../:/opt/de-roadmap
    user: "${AIRFLOW_UID:-1000}:0"
    command: scheduler
EOF

# 1. 메타 DB 초기화 (초기 구축이라 약간의 시간이 소요됩니다)
docker compose run --rm airflow-webserver airflow db init
# 2. 로그인용 최고 관리자(Admin) 계정 개설 (비밀번호: admin)
docker compose run --rm airflow-webserver airflow users create     --username admin     --firstname HSM     --lastname DE     --role Admin     --email admin@example.com     --password admin
ls
sudo nano docker-compose.yaml
~
..
.
cd
cd de-roadmap
ls
sudo nano docker-compose.yml
cd airflow
docker compose up -d
# 1. Airflow 컨테이너들이 잘 살아있는지(Up), 죽었는지(Exit) 상태 확인
docker compose ps
# 2. 에러 로그가 찍혔는지 웹서버 로그 통째로 확인
docker compose logs airflow-webserver | tail -n 30
# airflow 폴더 내부에서 실행
sudo chown -R 1000:0 dags logs plugins
chmod -R 777 dags logs plugins
# 그 후 다시 기동
docker compose down
docker compose up -d
# 도커 시스템의 찌꺼기 컨테이너/네트워크 캐시 완전 청소 (램 확보)
docker system prune -f
docker compose ps
# 1. 꼬여있는 권한 문제를 해결하기 위해 현재 실행 중인 서비스를 완전히 내립니다.
docker compose down
# 2. 호스트의 디렉토리 소유권을 Airflow 컨테이너 가상 유저(UID 1000)에게 완벽하게 양도합니다.
sudo chown -R 1000:0 dags logs plugins
sudo chmod -R 777 dags logs plugins
# 3. 찌꺼기 파일이나 꼬인 세션이 있을 수 있으니 가볍게 클린업
rm -f airflow.db
# 4. 메타 DB 구조 재구축 및 계정 재생성 (권한이 풀렸으므로 정상적으로 생성됩니다)
docker compose run --rm airflow-webserver airflow db init
docker compose run --rm airflow-webserver airflow users create     --username admin     --firstname HSM     --lastname DE     --role Admin     --email admin@example.com     --password admin
# 5. 이제 다시 백그라운드로 깨끗하게 엔진 점화!
docker compose up -d
docker compose ps
cat << 'EOF' > dags/spark_batch_dag.py
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
    description='MinIO 데이터레이크의 데이터를 스파크로 정제 후 PostgreSQL로 적재하는 배치',
    schedule_interval='@hourly', # 매시 정각 실행 트리거
    catchup=False
) as dag:

    # 1단계: 인프라 상태를 가볍게 체크하는 가상 태스크
    infra_check = BashOperator(
        task_id='infra_health_check',
        bash_command='echo "Starting Spark Batch Job on Airflow. Checking infrastructure..."'
    )

    # 2단계: 볼륨 마운트된 경로 안에서 가상환경 파이썬 커널로 스파크 배치 파일 실행
    run_spark_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command='''
        cd /opt/de-roadmap && \
        source myenv/bin/activate && \
        python3 spark_batch_clean.py
        '''
    )

    infra_check >> run_spark_job
EOF

# 1. 무한 루프로 돌고 있는 컨테이너들을 안전하게 종료
docker compose down
# 2. 강제로 볼륨 연동된 가상 경로에서 메타 DB 초기화 명령 수행
# (이 명령이 정상 완료되면서 컨테이너 내부에 airflow.db 파일이 깨끗하게 생성됩니다)
docker compose run --rm airflow-webserver airflow db init
# 3. 로그인에 사용할 최고 관리자(Admin) 계정 새로 생성 (비밀번호: admin)
docker compose run --rm airflow-webserver airflow users create     --username admin     --firstname HSM     --lastname DE     --role Admin     --email admin@example.com     --password admin
# 4. 이제 완벽하게 세팅되었으니 백그라운드로 전체 인프라 기동!
docker compose up -d
docker compose ps
docker compose logs -f airflow-webserver
# 1. 꼬여서 돌고 있는 에어플로우 서비스 전면 종료
docker compose down
# 2. 메타 DB 파일 위치를 확실하게 동기화하도록 컴포즈 파일 전면 전개
cat << 'EOF' > docker-compose.yaml
version: '3.8'
services:
  airflow-webserver:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-webserver
    restart: always
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      # 💡 메타 DB 위치를 마운트된 볼륨 내부 경로로 강제 지정
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/db/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__SECRET_KEY=my_super_secret_key_12345
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./:/opt/airflow/db
      - ../:/opt/de-roadmap
    ports:
      - "8089:8080"
    command: webserver

  airflow-scheduler:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-scheduler
    restart: always
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/db/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./:/opt/airflow/db
      - ../:/opt/de-roadmap
    user: "${AIRFLOW_UID:-1000}:0"
    command: scheduler
EOF

# 1. 혹시 남아있을지 모르는 꼬인 잔여 파일 삭제
rm -f airflow.db
# 2. 바뀐 영구 볼륨 경로 기준상에서 메타 DB 정식 초기화
docker compose run --rm airflow-webserver airflow db init
# 3. 로그인 마스터 계정 재생성 (비밀번호: admin)
docker compose run --rm airflow-webserver airflow users create     --username admin     --firstname HSM     --lastname DE     --role Admin     --email admin@example.com     --password admin
# 4. 이제 꼬일 건덕지가 전혀 없습니다. 정식 서비스 인프라 기동!
docker compose up -d
docker compose logs -f airflow-webserver
# 1. 실행 중인 인프라 전면 종료
docker compose down
# 2. 컴포즈 파일 수정 (자동 초기화 및 계정 생성 커맨드 주입)
cat << 'EOF' > docker-compose.yaml
services:
  airflow-webserver:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-webserver
    restart: always
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__SECRET_KEY=my_super_secret_key_12345
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ../:/opt/de-roadmap
    ports:
      - "8089:8080"
    # 💡 컨테이너가 뜰 때 자기가 직접 DB 초기화와 계정 생성을 순차적으로 실행하고 웹서버를 켭니다.
    command: >
      bash -c "airflow db init &&
      airflow users create --username admin --firstname HSM --lastname DE --role Admin --email admin@example.com --password admin &&
      exec airflow webserver"

  airflow-scheduler:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-scheduler
    restart: always
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ../:/opt/de-roadmap
    user: "${AIRFLOW_UID:-1000}:0"
    # 💡 스케줄러도 웹서버가 만든 DB가 안착할 때까지 잠시 대기 후 실행되도록 래핑합니다.
    command: >
      bash -c "sleep 5 && exec airflow scheduler"
EOF

docker compose up -d
docker compose logs -f airflow-webserver
# 1. dags 폴더에 파일이 제대로 들어있는지 확인 (파일 이름이 출력되어야 합니다)
ls -l dags/
# 2. 컨테이너 내부의 airflow 유저가 파일을 읽을 수 있도록 권한을 재차 개방
sudo chmod -R 777 dags/
# 1. 안전하게 인프라 잠시 다운
docker compose down
# 2. volumes 매핑 구조를 조금 더 명확하게 수정한 컴포즈 파일 배포
cat << 'EOF' > docker-compose.yaml
services:
  airflow-webserver:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-webserver
    restart: always
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__SECRET_KEY=my_super_secret_key_12345
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ../:/opt/de-roadmap
    ports:
      - "8089:8080"
    command: >
      bash -c "airflow db init &&
      airflow users create --username admin --firstname HSM --lastname DE --role Admin --email admin@example.com --password admin &&
      exec airflow webserver"

  airflow-scheduler:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-scheduler
    restart: always
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ../:/opt/de-roadmap
    # 스케줄러가 외부 python 환경과 dags를 안정적으로 스캔하도록 root-group 권한 동기화
    user: "0:0"
    command: >
      bash -c "sleep 5 && exec airflow scheduler"
EOF

# 1. 수정된 설정으로 다시 엔진 점화
docker compose up -d
# 1. 기존에 꼬여있던 컨테이너 전면 종료
docker compose down
# 2. 초경량 단일 컨테이너 체제로 컴포즈 수정
cat << 'EOF' > docker-compose.yaml
services:
  airflow-all-in-one:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-container
    restart: always
    user: "0:0" # 호스트 가상환경(myenv) 내부 진입을 위해 루트 권한 부여
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__SECRET_KEY=my_super_secret_key_12345
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ../:/opt/de-roadmap
    ports:
      - "8089:8080"
    # 💡 한 컨테이너 안에서 DB 초기화, 계정 생성, 스케줄러, 웹서버를 한 번에 다 세팅하고 띄웁니다.
    command: >
      bash -c "airflow db init &&
      airflow users create --username admin --firstname HSM --lastname DE --role Admin --email admin@example.com --password admin &&
      airflow scheduler &
      exec airflow webserver"
EOF

docker compose up -d
cat << 'EOF' > dags/spark_batch_dag.py
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
    description='Airflow 내부 환경에서 PySpark을 구동하여 데이터 정제 후 PostgreSQL로 적재하는 배치',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    infra_check = BashOperator(
        task_id='infra_health_check',
        bash_command='echo "Starting Spark Batch Job on Airflow. Setting up Airflow Spark runtime..."'
    )

    # 💡 컨테이너가 직접 격리된 환경에서 안전하게 PySpark 의존성을 확보하고 스파크 배치를 구동합니다.
    run_spark_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command='''
        pip install --no-cache-dir pyspark==3.4.1 && \
        cd /opt/de-roadmap && \
        python3 spark_batch_clean.py
        '''
    )

    infra_check >> run_spark_job
EOF

cat << 'EOF' > dags/spark_batch_dag.py
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
    description='Airflow 내부 환경에서 PySpark을 구동하여 데이터 정제 후 PostgreSQL로 적재하는 배치',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    infra_check = BashOperator(
        task_id='infra_health_check',
        bash_command='echo "Starting Spark Batch Job on Airflow. Setting up Airflow Spark runtime..."'
    )

    # 💡 --user 옵션을 추가하여 루트 권한 차단 에러를 완벽하게 우회합니다.
    run_spark_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command='''
        pip install --no-cache-dir --user pyspark==3.4.1 && \
        cd /opt/de-roadmap && \
        python3 spark_batch_clean.py
        '''
    )

    infra_check >> run_spark_job
EOF

cat << 'EOF' > dags/spark_batch_dag.py
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
    description='호스트 가상환경 라이브러리를 바인딩하여 스파크 배치를 구동하는 파이프라인',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    infra_check = BashOperator(
        task_id='infra_health_check',
        bash_command='echo "Starting Spark Batch Job on Airflow. Linking library path..."'
    )

    # 💡 pip를 거치지 않고 호스트 가상환경(myenv) 내부의 site-packages 경로를 파이썬 경로로 직접 지정합니다.
    run_spark_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command='''
        cd /opt/de-roadmap && \
        export PYTHONPATH="/opt/de-roadmap/myenv/lib/python3.10/site-packages:$PYTHONPATH" && \
        python3 spark_batch_clean.py
        '''
    )

    infra_check >> run_spark_job
EOF

# 1. 실행 중인 컨테이너 전면 종료
docker compose down
# 2. 자바 및 내부 라이브러리 자동 빌드가 포함된 컴포즈 파일 전개
cat << 'EOF' > docker-compose.yaml
services:
  airflow-all-in-one:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-container
    restart: always
    user: "0:0" # 자바 설치 및 내부 시스템 제어를 위해 루트 권한 유지
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__SECRET_KEY=my_super_secret_key_12345
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ../:/opt/de-roadmap
    ports:
      - "8089:8080"
    # 💡 [핵심] 컨테이너가 뜨자마자 스파크 필수 인프라(Java)를 깔고, 루트 격리를 우회해 내부 공용 공간에 pyspark을 직접 설치합니다.
    command: >
      bash -c "apt-get update && apt-get install -y default-jre &&
      python3 -m pip install --no-cache-dir pyspark==3.4.1 &&
      airflow db init &&
      airflow users create --username admin --firstname HSM --lastname DE --role Admin --email admin@example.com --password admin &&
      airflow scheduler &
      exec airflow webserver"
EOF

docker compose up -d
cat << 'EOF' > dags/spark_batch_dag.py
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
    description='자바 및 PySpark 인프라가 완비된 컨테이너 내부에서 구동되는 배치 파이프라인',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    infra_check = BashOperator(
        task_id='infra_health_check',
        bash_command='echo "Starting Spark Batch Job on Airflow. Core environment checked."'
    )

    # 💡 내부 공용 환경에 완전히 안착한 python3 명령어로 스파크 스크립트를 직접 가동합니다.
    run_spark_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command='''
        cd /opt/de-roadmap && \
        python3 spark_batch_clean.py
        '''
    )

    infra_check >> run_spark_job
EOF

docker compose down
cat << 'EOF' > docker-compose.yaml
services:
  airflow-all-in-one:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-container
    restart: always
    user: "0:0"
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__SECRET_KEY=my_super_secret_key_12345
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ../:/opt/de-roadmap
    ports:
      - "8089:8080"
    command: >
      bash -c "apt-get update && apt-get install -y default-jre &&
      python3 -m pip install --no-cache-dir pyspark==3.4.1 &&
      airflow db init &&
      airflow users create --username admin --firstname HSM --lastname DE --role Admin --email admin@example.com --password admin &&
      airflow scheduler &
      exec airflow webserver"
EOF

docker compose up -d
cat << 'EOF' > dags/spark_batch_dag.py
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
    description='자바와 PySpark이 완비된 내장 컨테이너 환경 배치 파이프라인',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    infra_check = BashOperator(
        task_id='infra_health_check',
        bash_command='echo "Starting Spark Batch Job on Airflow. Core environment checked."'
    )

    run_spark_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command='''
        cd /opt/de-roadmap && \
        python3 spark_batch_clean.py
        '''
    )

    infra_check >> run_spark_job
EOF

docker compose logs -f airflow-all-in-one
# 1. 꼬여서 돌고 있는 컨테이너 전면 종료
docker compose down
# 2. 치트키 entrypoint 설정을 주입한 컴포즈 파일 배포
cat << 'EOF' > docker-compose.yaml
services:
  airflow-all-in-one:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-container
    restart: always
    user: "0:0"
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__SECRET_KEY=my_super_secret_key_12345
      - AIRFLOW_HOME=/opt/airflow
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ../:/opt/de-roadmap
    ports:
      - "8089:8080"
    # 💡 [핵심] 에어플로우 자체 차단 스크립트를 무시하고, 배시 쉘로 다이렉트 진입하여 순차 실행합니다.
    entrypoint: /bin/bash
    command: >
      -c "airflow db init &&
      airflow users create --username admin --firstname HSM --lastname DE --role Admin --email admin@example.com --password admin &&
      apt-get update && apt-get install -y default-jre &&
      python3 -m pip install --no-cache-dir pyspark==3.4.1 &&
      airflow scheduler &
      exec airflow webserver"
EOF

docker compose up -d
docker compose logs -f airflow-all-in-one
# 1. 꼬인 컨테이너 전면 종료
docker compose down
# 2. 에어플로우 친화형 정석 컴포즈 파일 배포
cat << 'EOF' > docker-compose.yaml
services:
  airflow-all-in-one:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-container
    restart: always
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__SECRET_KEY=my_super_secret_key_12345
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ../:/opt/de-roadmap
    ports:
      - "8089:8080"
    # 💡 에어플로우 순정 환경을 유지하면서, 기동 직전 스파크 패키지만 유저 영역에 슬쩍 심어줍니다.
    command: >
      bash -c "pip install --no-cache-dir --user pyspark==3.4.1 &&
      airflow db init &&
      airflow users create --username admin --firstname HSM --lastname DE --role Admin --email admin@example.com --password admin &&
      airflow scheduler &
      exec airflow webserver"
EOF

docker compose up -d
docker compose logs -f airflow-all-in-one
docker compose down
cat << 'EOF' > docker-compose.yaml
services:
  airflow-all-in-one:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-container
    restart: always
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__SECRET_KEY=my_super_secret_key_12345
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ../:/opt/de-roadmap
    ports:
      - "8089:8080"
    command: >
      bash -c "airflow db init &&
      airflow users create --username admin --firstname HSM --lastname DE --role Admin --email admin@example.com --password admin &&
      airflow scheduler &
      exec airflow webserver"
EOF

docker compose up -d
cat << 'EOF' > dags/spark_batch_dag.py
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
    description='호스트 패키지 경로를 동적 바인딩하여 구동하는 최종 안정화 파이프라인',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    infra_check = BashOperator(
        task_id='infra_health_check',
        bash_command='echo "Starting Spark Batch Job on Airflow. Core environment checked."'
    )

    # 💡 파이썬 실행 시 호스트 가상환경의 site-packages 경로를 동적으로 강제 주입합니다.
    run_spark_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command='''
        cd /opt/de-roadmap && \
        python3 -c "import sys; sys.path.insert(0, '/opt/de-roadmap/myenv/lib/python3.10/site-packages'); exec(open('spark_batch_clean.py').read())"
        '''
    )

    infra_check >> run_spark_job
EOF

ls
cd de-roadmap
ls
cd airflow
docker compose down
cat << 'EOF' > docker-compose.yaml
services:
  airflow-all-in-one:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-container
    restart: always
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__SECRET_KEY=my_super_secret_key_12345
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ../:/opt/de-roadmap
    ports:
      - "8089:8080"
    command: >
      bash -c "airflow db init &&
      airflow users create --username admin --firstname HSM --lastname DE --role Admin --email admin@example.com --password admin &&
      airflow scheduler &
      exec airflow webserver"
EOF

docker compose up -d
cat << 'EOF' > dags/spark_batch_dag.py
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
    description='호스트 패키지 경로를 동적 바인딩하여 구동하는 최종 안정화 파이프라인',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    infra_check = BashOperator(
        task_id='infra_health_check',
        bash_command='echo "Starting Spark Batch Job on Airflow. Core environment checked."'
    )

    # 💡 파이썬 실행 시 호스트 가상환경의 site-packages 경로를 동적으로 강제 주입합니다.
    run_spark_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command='''
        cd /opt/de-roadmap && \
        python3 -c "import sys; sys.path.insert(0, '/opt/de-roadmap/myenv/lib/python3.10/site-packages'); exec(open('spark_batch_clean.py').read())"
        '''
    )

    infra_check >> run_spark_job
EOF

cat << 'EOF' > dags/spark_batch_dag.py
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
    description='에어플로우 유저 로컬 영역에 PySpark을 안전하게 구성하여 구동하는 파이프라인',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    infra_check = BashOperator(
        task_id='infra_health_check',
        bash_command='echo "Starting Spark Batch Job on Airflow. Local environment provisioning..."'
    )

    # 💡 순정 에어플로우 유저 공간에 패키지를 점진적으로 설치하여 보안 가드레일과 타임아웃을 모두 회피합니다.
    run_spark_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command='''
        python3 -m pip install --user --no-cache-dir pyspark==3.4.1 && \
        cd /opt/de-roadmap && \
        python3 spark_batch_clean.py
        '''
    )

    infra_check >> run_spark_job
EOF

# 1. 기존 컨테이너 종료
docker compose down
# 2. 백그라운드 Java 자동 설치 로직을 심은 컴포즈 배포
cat << 'EOF' > docker-compose.yaml
services:
  airflow-all-in-one:
    image: apache/airflow:2.7.2-python3.10
    container_name: my-airflow-container
    restart: always
    user: "0:0" # 컨테이너 내부 자바(apt-get) 주입을 위해 루트 권한 설정
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__SECRET_KEY=my_super_secret_key_12345
      - JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
    logging:
      options:
        max-size: "10m"
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ../:/opt/de-roadmap
    ports:
      - "8089:8080"
    # 💡 DB 초기화를 최우선으로 진행하여 타임아웃 셧다운을 원천 봉쇄한 뒤, 자바를 안전하게 주입합니다.
    command: >
      bash -c "airflow db init &&
      airflow users create --username admin --firstname HSM --lastname DE --role Admin --email admin@example.com --password admin &&
      apt-get update && apt-get install -y default-jre &&
      airflow scheduler &
      exec airflow webserver"
EOF

docker compose up -d
cat << 'EOF' > dags/spark_batch_dag.py
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
    description='자바와 스파크 인프라가 완비된 컨테이너 내부 배시 실행형 파이프라인',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    infra_check = BashOperator(
        task_id='infra_health_check',
        bash_command='echo "Starting Spark Batch Job on Airflow. Java & PySpark Infrastructure Checked."'
    )

    run_spark_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command='''
        cd /opt/de-roadmap && \
        python3 spark_batch_clean.py
        '''
    )

    infra_check >> run_spark_job
EOF

cat << 'EOF' > dags/spark_batch_dag.py
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
    description='루트 전역 환경에 PySpark을 바인딩하여 실행하는 파이프라인',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    infra_check = BashOperator(
        task_id='infra_health_check',
        bash_command='echo "Starting Spark Batch Job on Airflow. Global environment sync..."'
    )

    # 💡 현재 root 권한이므로 --user 없이 전역에 깔아버려 권한 분리를 해결합니다.
    run_spark_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command='''
        python3 -m pip install --no-cache-dir pyspark==3.4.1 && \
        cd /opt/de-roadmap && \
        python3 spark_batch_clean.py
        '''
    )

    infra_check >> run_spark_job
EOF

cat << 'EOF' > dags/spark_batch_dag.py
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
    description='하둡 S3A 커넥터 라이브러리를 내장하여 물리적으로 구동하는 파이프라인',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    infra_check = BashOperator(
        task_id='infra_health_check',
        bash_command='echo "Starting Spark Batch Job on Airflow. Injecting AWS S3A Jars..."'
    )

    # 💡 스파크 전역 jar 폴더를 찾아 커넥터 2종을 물리적으로 꽂아 넣은 뒤 배치를 가동합니다.
    run_spark_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command='''
        python3 -m pip install --no-cache-dir pyspark==3.4.1 && \
        SPARK_JARS_DIR=$(python3 -c "import pyspark, os; print(os.path.join(pyspark.__path__[0], 'jars'))") && \
        if [ ! -f "$SPARK_JARS_DIR/hadoop-aws-3.3.4.jar" ]; then \
          wget -P "$SPARK_JARS_DIR" https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
          wget -P "$SPARK_JARS_DIR" https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar; \
        fi && \
        cd /opt/de-roadmap && \
        python3 spark_batch_clean.py
        '''
    )

    infra_check >> run_spark_job
EOF

cat << 'EOF' > dags/spark_batch_dag.py
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
EOF

cd
cd de-roadmap
ls
sudo nano spark_batch_clean.py
docker compose ps
sudo nano spark_batch_clean.py
# 1. 컨테이너 잠시 종료
docker compose down
# 2. 볼륨 마운트된 logs나 기존 db가 꼬이지 않도록 깔끔하게 초기화 모드로 재기동
# (command 내부의 'airflow db init'이 실행되면서 이전 기록이 전부 완벽히 포맷됩니다.)
docker compose up -d
cd de-roadmap
# 1. 컨테이너 잠시 종료
docker compose down
# 2. 볼륨 마운트된 logs나 기존 db가 꼬이지 않도록 깔끔하게 초기화 모드로 재기동
# (command 내부의 'airflow db init'이 실행되면서 이전 기록이 전부 완벽히 포맷됩니다.)
docker compose up -d
cd /opt/de-roadmap
nano spark_batch_clean.py
ls
sudo nano docker-compose.yml
hostname -I
sudo nano spark_batch_clean.py
docker compose down && docker compose up -d
cd
d
git init
git add .
cat << 'EOF' > .gitignore
# DB 및 오브젝트 스토리지 데이터 폴더 제외
*_data/
mariadb_data/
postgres_data/
minio_data/

# 파이썬 및 스파크 임시 파일 제외
__pycache__/
.ipynb_checkpoints/
*.pyc
EOF

