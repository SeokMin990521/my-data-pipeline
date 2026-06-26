import time
import random
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

# 1. 스키마 레지스트리 및 카프카 설정
SR_URL = "http://localhost:8081"
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "avro-user-events"  # 11일차와 동일한 토픽 사용

print("📡 [12일차] 스트리밍 데이터 레이크 파이프라인 가동...")

# 2. Schema Registry 클라이언트 세팅 및 기존 avsc 파일 로드
sr_client = SchemaRegistryClient({"url": SR_URL})

with open("user_event.avsc", "r") as f:
    schema_str = f.read()

# Avro 및 String 직렬화 도구 준비
avro_serializer = AvroSerializer(
    schema_registry_client=sr_client,
    schema_str=schema_str
)
string_serializer = StringSerializer('utf_8')

# 3. 카프카 프로듀서 생성
producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'python-avro-streaming-producer'
}
producer = Producer(producer_config)

# 전송 결과 확인 콜백 함수 (성공 로그는 간소화)
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ 메시지 전송 실패: {err}")
    else:
        pass

# 4. 실시간 가상 유저 데이터 풀 구성 (무작위 조합용 데이터)
first_names = ["Kim", "Lee", "Park", "Choi", "Jung", "Kang", "Cho", "Yoon"]
last_names = ["Data", "Eng", "Cloud", "DBA", "Spark", "Kafka", "Docker", "Dev"]
domains = ["example.com", "korea.com", "data-lake.org", "gmail.com"]

print("🚀 실시간 가상 데이터 생성 및 카프카 전송을 시작합니다. (중단하려면 Ctrl+C)")
print("-" * 60)

sent_count = 0

try:
    while True:
        # 11일차 스키마 규격에 맞는 가상 데이터 무작위 동적 조립
        rand_id = random.randint(1000, 9999)
        rand_name = f"{random.choice(first_names)}-{random.choice(last_names)}"
        rand_email = f"{rand_name.lower().replace('-', '_')}@{random.choice(domains)}"
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        streaming_data = {
            "user_id": rand_id,
            "user_name": rand_name,
            "email": rand_email,
            "collected_at": current_time
        }

        # 데이터를 아브로(Avro) 바이너리로 압축 변환하여 카프카로 연속 슛!
        producer.produce(
            topic=TOPIC_NAME,
            key=string_serializer(str(streaming_data["user_id"])),
            value=avro_serializer(streaming_data, SerializationContext(TOPIC_NAME, MessageField.VALUE)),
            on_delivery=delivery_report
        )
        
        # 비동기 전송 큐의 이벤트를 주기적으로 처리
        producer.poll(0)
        
        sent_count += 1
        if sent_count % 10 == 0:
            print(f"📡 [스트리밍 누적 {sent_count}건] 전송 완료 ➡️ ID: {rand_id} | Name: {rand_name} | Time: {current_time}")
            producer.flush() # 10건마다 버퍼 강제 전송으로 엇박자 방지

        # 실시간 웹 서비스 로그의 유입 속도 모사 (0.05초~0.2초 사이 무작위 난사)
        time.sleep(random.uniform(0.05, 0.2))

except KeyboardInterrupt:
    print("\n" + "="*60)
    print("🛑 사용자가 시스템을 수동 중지했습니다. 남은 버퍼를 전송하고 종료합니다.")
    producer.flush()
    print(f"📊 총 전송된 실시간 데이터 스트림: {sent_count} 건")
    print("="*60)

except Exception as e:
    print(f"💥 예기치 못한 에러 발생: {e}")
