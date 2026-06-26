# 🚀 분산 환경 기반 엔터프라이즈 데이터 배치 파이프라인

Docker 환경에서 Apache Airflow와 Spark를 연동하여 대용량 배치 정제 및 PostgreSQL 데이터 마트 Upsert 파이프라인을 구축하고 Grafana로 관제한 프로젝트입니다.

## 🏗️ Architecture
- **Orchestration:** Apache Airflow (`0 0 * * *` 매일 자정 자동화)
- **Processing Engine:** Apache Spark (DataFrame API)
- **Storage & DB:** MinIO (Object Storage), PostgreSQL (Data Mart)
- **Monitoring:** Grafana Alerting

## 🛠️ Key Troubleshooting
1. **Upsert 중복 Key 충돌 제어**
   - Spark 단에서 `.dropDuplicates(["id"])` 전처리를 선행하고 DB 단에서 `ON CONFLICT DO UPDATE`를 연동하여 파이프라인의 멱등성(Idempotency) 확보.
2. **Grafana 타임존 시차 교정**
   - UTC/KST 시차 문제를 해결하기 위해 관제 쿼리에 `timezone('utc', now())` 수식을 적용하여 실시간 경보 시스템의 신뢰성 구축.
