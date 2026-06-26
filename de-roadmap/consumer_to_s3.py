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
        user_event_data = msg.value()
        data_buffer.append(user_event_data)

        # 버퍼 조건 충족 시 업로드
        if len(data_buffer) >= BUFFER_SIZE_LIMIT or (time.time() - last_flush_time >= TIME_LIMIT_SECONDS):
            upload_to_minio(data_buffer)
            data_buffer.clear()
            last_flush_time = time.time()

except KeyboardInterrupt:
    print("\n🛑 사용자에 의해 컨슈머가 정지되었습니다. 남은 버퍼를 정리합니다.")
    if data_buffer:
        upload_to_minio(data_buffer)
finally:
    consumer.close()
    print("👋 데이터 레이크 적재 시스템이 안전하게 종료되었습니다.")
