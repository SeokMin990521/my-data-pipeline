import time
import json
import requests
import mysql.connector
import psycopg2

# 11일차: Confluent Kafka 및 Avro 관련 모듈 로드
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

def connect_db():
    while True:
        try:
            print("[인프라 연결] MariaDB 및 PostgreSQL 연결 시도 중...")
            m_conn = mysql.connector.connect(host="mariadb", user="root", password="1234")
            p_conn = psycopg2.connect(host="postgres", user="postgres", password="1234")
            return m_conn, p_conn
        except Exception as e:
            print(f"[대기] DB 부팅 중... 5초 후 재시도합니다. 에러: {e}")
            time.sleep(5)

def init_infrastructure(m_conn, p_conn):
    m_cursor = m_conn.cursor()
    m_cursor.execute("CREATE DATABASE IF NOT EXISTS de_db;")
    m_cursor.execute("USE de_db;")
    m_cursor.execute("DROP TABLE IF EXISTS users;")
    m_cursor.execute("""
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) UNIQUE,
            company VARCHAR(100)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    m_conn.commit()
    m_cursor.close()

    p_cursor = p_conn.cursor()
    p_cursor.execute("DROP TABLE IF EXISTS analytic_users_dw;")
    p_cursor.execute("DROP TABLE IF EXISTS kafka_dlq;")
    p_cursor.execute("""
        CREATE TABLE analytic_users_dw (
            id SERIAL PRIMARY KEY,
            user_id INT,
            name VARCHAR(100),
            email VARCHAR(100),
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    p_cursor.execute("""
        CREATE TABLE kafka_dlq (
            id SERIAL PRIMARY KEY,
            raw_message TEXT,
            error_message TEXT,
            failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    p_conn.commit()
    p_cursor.close()
    print("[초기화] 모든 데이터베이스 스키마 및 DLQ 방어선 구축 완료!")

def run_automated_etl(m_conn):
    print("[ETL] 외부 API 데이터 수집 및 적재 시작...")
    try:
        res = requests.get("https://jsonplaceholder.typicode.com/users")
        users_data = res.json()
        
        m_cursor = m_conn.cursor()
        for user in users_data:
            try:
                sql = "INSERT INTO users (id, name, email, company) VALUES (%s, %s, %s, %s)"
                val = (user['id'], user['name'], user['email'], user['company']['name'])
                m_cursor.execute(sql, val)
            except mysql.connector.Error as err:
                if err.errno == 1062:
                    continue
                raise err
        m_conn.commit()
        m_cursor.close()
        print("[ETL] 배치 데이터 적재 성공!")
    except Exception as e:
        print(f"[ETL 에러] 배치 처리 중 오류 발생: {e}")

# 11일차 고도화: Avro 역직렬화 도구 세팅
def init_avro_deserializer():
    while True:
        try:
            print("[인프라 연결] Schema Registry 연결 시도 중...")
            # 중앙 스키마 저장소 클라이언트 생성
            sr_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})
            
            # 로컬에 정의한 avsc 스키마 파일 읽기
            with open('user.avsc', 'r') as f:
                schema_str = f.read()
            
            # Avro 역직렬화기(Deserializer) 빌드
            avro_deserializer = AvroDeserializer(sr_client, schema_str)
            return avro_deserializer
        except Exception as e:
            print(f"[대기] Schema Registry 대기 중... 5초 후 재시도: {e}")
            time.sleep(5)

def main():
    m_conn, p_conn = connect_db()
    init_infrastructure(m_conn, p_conn)
    run_automated_etl(m_conn)
    
    # Avro 디코더 초기화
    avro_deserializer = init_avro_deserializer()
    
    # Confluent-Kafka 컨슈머 설정 (안정적인 고성능 C기반 클라이언트)
    consumer_config = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'de-avro-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe(['user-events'])
    
    p_cursor = p_conn.cursor()
    print("[스트리밍] Avro 기반 Schema Registry 동기화 완료. 실시간 모니터링 시작 (24/7 강제 생존)...")
    
    while True:
        try:
            # 1초 간격으로 메시지 폴링
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[카프카 에러] {msg.error()}")
                    continue

            # 11일차 핵심: 들어온 바이너리 데이터를 Avro 스키마 기반으로 안전하게 복원
            try:
                data = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                print(f"[수신] Avro 디코딩 성공 데이터: {data}")
                
                # PostgreSQL DW에 누적 적재
                p_cursor.execute(
                    "INSERT INTO analytic_users_dw (user_id, name, email) VALUES (%s, %s, %s);",
                    (data["user_id"], data["name"], data.get("email", "N/A"))
                )
                p_conn.commit()
                print(f"[적재] PostgreSQL DW에 유저 {data['name']} 누적 완료.")
                
            except Exception as avro_err:
                # 스키마에 위배되거나 디코딩에 실패한 불량 가짜 데이터는 DLQ로 격리
                print(f"[경고] 스키마 변환 실패! 악성 데이터 감지. DLQ 격리 조치.")
                raw_bytes_str = str(msg.value())
                p_cursor.execute(
                    "INSERT INTO kafka_dlq (raw_message, error_message) VALUES (%s, %s);",
                    (raw_bytes_str, f"Avro De-serialization failed: {avro_err}")
                )
                p_conn.commit()

        except Exception as e:
            print(f"[시스템 예외 발생] 무한 루프 방어선 가동 중: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()
