from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp

# 🌟 컨테이너 내부 가상망 주소 완전 고정
MINIO_URL = "http://host.docker.internal:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
RAW_BUCKET = "raw-data-lake"
MART_BUCKET = "analytics-data-mart"

PG_URL = "jdbc:postgresql://host.docker.internal:5432/analytics_db"
PG_PROPERTIES = {
    "user": "postgres",
    "password": "1234",  # 💡 만약 inspect 시 다르게 나왔다면 그 패스워드로 수정
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
