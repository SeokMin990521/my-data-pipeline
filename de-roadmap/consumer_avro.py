import time
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

SR_URL = "http://localhost:8081"
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "avro-user-events"

print("📥 [11일차] Avro Consumer 기지 가동... Schema Registry 연결 중...")

sr_client = SchemaRegistryClient({"url": SR_URL})

with open("user_event.avsc", "r") as f:
    schema_str = f.read()

avro_deserializer = AvroDeserializer(
    schema_registry_client=sr_client,
    schema_str=schema_str
)
string_deserializer = StringDeserializer('utf_8')

consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'python-avro-group',
    'auto.offset.reset': 'earliest',
    'key.deserializer': string_deserializer,
    'value.deserializer': avro_deserializer
}

consumer = DeserializingConsumer(consumer_config)
consumer.subscribe([TOPIC_NAME])

print(f"🌟 24시간 감시망 작동 시작... 토픽 [{TOPIC_NAME}]을 구독합니다. (종료: Ctrl + C)")
print("-" * 60)

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            print(f"❌ 컨슈머 에러 발생: {msg.error()}")
            continue

        user_key = msg.key()
        user_data = msg.value()
        
        print(f"🔔 [실시간 데이터 포획] Key(ID): {user_key}")
        print(f"   📊 해독된 데이터 상세: {user_data}")
        print(f"   📍 파티션: {msg.partition()}, 오프셋: {msg.offset()}")
        print("-" * 60)

except KeyboardInterrupt:
    print("\n🛑 사용자에 의해 컨슈머가 안전하게 종료되었습니다.")
finally:
    consumer.close()
