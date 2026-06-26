FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc libpq-dev librdkafka-dev && rm -rf /var/lib/apt/lists/*

# confluent-kafka[avro] 확장 스펙 지정 및 누락된 의존성 일괄 주입
RUN pip install --no-cache-dir mysql-connector-python psycopg2-binary requests "confluent-kafka[avro]" fastavro httpx authlib cachetools

COPY . .

CMD ["python", "-u", "main.py"]
