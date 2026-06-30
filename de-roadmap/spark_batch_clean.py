# ~/de-roadmap/spark_batch_clean.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp
import os

MINIO_URL = "http://127.0.0.1:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
RAW_BUCKET = "raw-data-lake"

print("⚡ [15일차] Apache Spark 분산 배치 엔진 가동 및 PostgreSQL 적재 준비...")

# S3 커넥터와 함께 PostgreSQL JDBC 드라이버 패키지를 함께 로드합니다.
spark = (SparkSession.builder
    .appName("Deroadmap-Spark-Airflow-PostgreSQL")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL)
    .config("fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    # 14일차 버그 박멸 밀리초 강제 변환 옵션
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")
    .config("spark.hadoop.fs.s3a.multipart.purge.init.time", "86400000")
    .master("local[*]")
    .getOrCreate())

try:
    # 1. 데이터 레이크 원시 JSON 데이터 로드
    raw_s3_path = f"s3a://{RAW_BUCKET}/year=*/*/*/*/*.json"
    df = spark.read.json(raw_s3_path)
    
    # 2. 데이터 가공 및 비즈니스 마트 스키마 변환
    cleaned_df = df \
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("is_conversion", when(col("action") == "purchase", 1).otherwise(0)) \
        .select("user_id", "username", "action", "event_time", "is_conversion")

    print(f"🧹 정제 완료 데이터 수: {cleaned_df.count()}건")

    # 3. [15일차 핵심] PostgreSQL 분석용 마트 테이블(mart_user_conversions)로 적재
    print("💾 PostgreSQL 데이터 웨어하우스 마트 테이블 적재 시작...")
    
    # 도커 컴포즈 상의 PostgreSQL 포트가 5432로 호스트에 포워딩되어 있으므로 localhost 통신 가능
    pg_url = "jdbc:postgresql://127.0.0.1:5432/de_db" 
    
    cleaned_df.write \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "mart_user_conversions") \
        .option("user", "postgres") \
        .option("password", "password123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("🎯 [Spark 작업 완료] 데이터 레이크 정제 및 PostgreSQL 마트 이관 완수!")

except Exception as e:
    print(f"❌ 스파크 배치 연산 중 오류 발생: {e}")
finally:
    spark.stop()
