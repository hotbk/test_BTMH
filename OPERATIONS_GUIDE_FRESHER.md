# Huong Dan Van Hanh Moi Truong Airflow + DBT

Tai lieu nay danh cho fresher de khoi dong, kiem tra, debug va cap nhat moi truong Airflow + DBT trong project.

## 1. Tong Quan Dich Vu

Moi truong nay chay bang Docker Compose, gom cac thanh phan chinh:

- Airflow UI: http://localhost:8080
- Flower Celery monitor: http://localhost:5555
- DBT Docs: http://localhost:8090
- Postgres: metadata database cua Airflow
- Redis: broker cho CeleryExecutor
- Airflow scheduler, worker, triggerer, dag processor
- DBT project: `run_env/dags/dbt/btmh_dmt`

## 2. Cac File Quan Trong

- `Dockerfile`: build image Airflow custom.
- `requirements.txt`: dependency Python cho Airflow.
- `.env`: bien moi truong cho Airflow, BigQuery, GCP.
- `airflow-postgres-compose.yaml`: service Postgres.
- `airflow-redis-compose.yaml`: service Redis.
- `airflow-core-compose.yaml`: Airflow API server, scheduler, triggerer, dag processor, init.
- `airflow-workers-compose.yaml`: Airflow worker, Flower, Docker proxy.
- `airflow-dbt-docs-compose.yaml`: Nginx serve DBT docs.
- `run_env/dags`: DAGs Airflow.
- `run_env/dags/dbt/btmh_dmt`: DBT project.
- `run_env/data/dbt_docs`: output HTML cua DBT docs.

## 3. Khoi Dong Moi Truong

Chay tu thu muc goc project:

```bash
docker compose \
  -f airflow-postgres-compose.yaml \
  -f airflow-redis-compose.yaml \
  -f airflow-core-compose.yaml \
  -f airflow-workers-compose.yaml \
  up -d
```

Kiem tra container:

```bash
docker ps -a
```

Trang thai mong doi:

- `airflow-apiserver`: healthy
- `airflow-scheduler`: healthy
- `airflow-worker`: healthy
- `airflow-dag-processor`: healthy
- `airflow-triggerer`: healthy
- `airflow-flower`: healthy
- `airflow-postgres`: healthy
- `airflow-redis`: healthy
- `airflow-init`: Exited (0), day la binh thuong

## 4. Rebuild Image Khi Sua Dockerfile Hoac Requirements

Neu sua `Dockerfile` hoac `requirements.txt`, phai rebuild image:

```bash
docker compose \
  -f airflow-postgres-compose.yaml \
  -f airflow-redis-compose.yaml \
  -f airflow-core-compose.yaml \
  -f airflow-workers-compose.yaml \
  build
```

Sau do recreate container:

```bash
docker compose \
  -f airflow-postgres-compose.yaml \
  -f airflow-redis-compose.yaml \
  -f airflow-core-compose.yaml \
  -f airflow-workers-compose.yaml \
  up -d
```

Kiem tra lai DBT CLI trong worker:

```bash
docker exec airflow-worker sh -c 'which dbt && dbt --version'
```

## 5. Van Hanh DBT

DBT project nam tai:

```text
run_env/dags/dbt/btmh_dmt
```

Trong container Airflow, duong dan tuong ung la:

```text
/opt/airflow/dags/dbt/btmh_dmt
```

Kiem tra cau hinh DBT:

```bash
docker exec airflow-worker sh -c \
'cd /opt/airflow/dags/dbt/btmh_dmt && dbt debug --profiles-dir . --no-version-check'
```

Parse DBT project:

```bash
docker exec airflow-worker sh -c \
'cd /opt/airflow/dags/dbt/btmh_dmt && dbt parse --profiles-dir . --no-version-check'
```

Generate DBT docs:

```bash
docker exec airflow-worker sh -c \
'mkdir -p /opt/airflow/data/dbt_docs && cd /opt/airflow/dags/dbt/btmh_dmt && dbt docs generate --profiles-dir . --target-path /opt/airflow/data/dbt_docs --no-version-check'
```

## 6. Chay DBT Docs Web

Start container serve DBT docs:

```bash
docker compose -f airflow-dbt-docs-compose.yaml up -d
```

Kiem tra:

```bash
docker ps -a --filter name=airflow-dbt-docs
curl -I http://localhost:8090
```

Neu thanh cong, mo:

```text
http://localhost:8090
```

## 7. Cac Lenh Kiem Tra Thuong Dung

Xem tat ca container:

```bash
docker ps -a
```

Xem log API server:

```bash
docker logs --tail 100 airflow-apiserver
```

Xem log scheduler:

```bash
docker logs --tail 100 airflow-scheduler
```

Xem log worker:

```bash
docker logs --tail 100 airflow-worker
```

Xem log triggerer:

```bash
docker logs --tail 100 airflow-triggerer
```

Xem healthcheck chi tiet:

```bash
docker inspect --format '{{json .State.Health}}' airflow-worker
```

Xem tai nguyen container:

```bash
docker stats --no-stream
```

## 8. Cach Debug Khi Container Loi

Thu tu xu ly khi thay container `unhealthy` hoac `exited`:

1. Kiem tra trang thai:

```bash
docker ps -a
```

2. Xem log container loi:

```bash
docker logs --tail 150 <container_name>
```

3. Xem healthcheck:

```bash
docker inspect --format '{{json .State.Health}}' <container_name>
```

4. Neu vua sua compose hoac image, recreate:

```bash
docker compose \
  -f airflow-postgres-compose.yaml \
  -f airflow-redis-compose.yaml \
  -f airflow-core-compose.yaml \
  -f airflow-workers-compose.yaml \
  up -d --force-recreate
```

## 9. Cac Loi Thuong Gap

### DBT docs khong hien thi

Kiem tra container:

```bash
docker ps -a --filter name=airflow-dbt-docs
```

Kiem tra output docs:

```bash
find run_env/data/dbt_docs -maxdepth 1 -type f
```

Neu thieu `index.html`, generate lai:

```bash
docker exec airflow-worker sh -c \
'cd /opt/airflow/dags/dbt/btmh_dmt && dbt docs generate --profiles-dir . --target-path /opt/airflow/data/dbt_docs --no-version-check'
```

### DBT bao thieu dbt_project.yml

Kiem tra file:

```bash
ls -la run_env/dags/dbt/btmh_dmt/dbt_project.yml
```

Neu file mat, can khoi phuc lai `dbt_project.yml`.

### Airflow worker unhealthy

Xem log:

```bash
docker logs --tail 150 airflow-worker
```

Xem health:

```bash
docker inspect --format '{{json .State.Health}}' airflow-worker
```

Worker co the can vai phut de healthy sau khi recreate image lon.

### GCP credential loi

Kiem tra `.env`:

```text
GCP_SERVICE_ACCOUNT_JSON_B64
GOOGLE_APPLICATION_CREDENTIALS
```

Neu `GCP_SERVICE_ACCOUNT_JSON_B64` van la placeholder hoac base64 sai, Airflow init co the log loi decode credential.

## 10. Luong Lam Viec Hang Ngay

Moi ngay nen kiem tra:

```bash
docker ps -a
docker logs --tail 100 airflow-scheduler
docker logs --tail 100 airflow-worker
```

Neu sua DBT model/macro:

```bash
docker exec airflow-worker sh -c \
'cd /opt/airflow/dags/dbt/btmh_dmt && dbt parse --profiles-dir . --no-version-check'
```

Neu muon cap nhat DBT docs:

```bash
docker exec airflow-worker sh -c \
'cd /opt/airflow/dags/dbt/btmh_dmt && dbt docs generate --profiles-dir . --target-path /opt/airflow/data/dbt_docs --no-version-check'
```

## 11. Nguyen Tac Cho Fresher

- Khong sua truc tiep file ben trong container.
- Sua code tren host trong `run_env/dags` hoac cac file project.
- Sau khi sua DAG, cho scheduler parse lai trong vai chuc giay.
- Sau khi sua `requirements.txt` hoac `Dockerfile`, phai rebuild image.
- Sau khi sua DBT model/macro, chay `dbt parse`.
- Sau khi sua DBT docs/model va can cap nhat trang docs, chay `dbt docs generate`.
- Khong xoa volume/container neu chua hieu ro tac dong.
- Khi gap loi, doc log truoc khi restart hang loat.

