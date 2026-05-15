# Huong Dan Dung Zabbix Monitor Airflow DAG Health

Tai lieu nay huong dan dung Zabbix de monitor DAG cho project Airflow nay, dua tren bai viet:

```text
https://quirky-guy.com/zabbix-airflow-monitoring-dag-health-en/
```

Y tuong chinh cua bai viet:

```text
Zabbix
  -> goi Airflow REST API lay danh sach DAG
  -> dung Low Level Discovery tao item theo tung DAG
  -> goi API lay latest DAG run
  -> trigger alert neu latest DAG run failed
  -> monitor them health cua scheduler / metadata DB / triggerer
```

Project nay dang dung Airflow `3.1.6`, nen phai dung Airflow REST API v2:

```text
/api/v2/...
```

Khong dung `/api/v1/...` nhu Airflow 2 hoac mot so vi du cu.

## 1. Kien truc ap dung cho project nay

Airflow service trong Docker Compose:

```text
airflow-apiserver  http://localhost:8080
airflow-scheduler  healthcheck noi bo port 8974
airflow-triggerer
airflow-postgres
airflow-redis
airflow-worker
```

Zabbix se monitor qua Airflow API server:

```text
http://<AIRFLOW_HOST>:8080/api/v2
```

Neu Zabbix server chay cung may Docker host, dung:

```text
http://127.0.0.1:8080
```

Neu Zabbix server nam may khac, thay bang IP/domain cua may dang expose Airflow:

```text
http://<airflow-server-ip>:8080
```

## 2. Dieu kien truoc khi lam

Airflow phai dang chay:

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

```text
airflow-apiserver      healthy
airflow-scheduler      healthy
airflow-dag-processor  healthy
airflow-triggerer      healthy
airflow-worker         healthy
airflow-postgres       healthy
airflow-redis          healthy
```

Airflow user mac dinh duoc tao trong compose:

```text
username: airflow
password: airflow
```

Neu production, tao user rieng cho Zabbix, khong dung admin mac dinh.

## 3. Test Airflow API bang curl

Tren may Zabbix server, cai `curl` va `jq`:

```bash
sudo apt-get update
sudo apt-get install -y curl jq
```

Kiem tra API version:

```bash
curl -s -u airflow:airflow http://127.0.0.1:8080/api/v2/version | jq .
```

Kiem tra danh sach DAG:

```bash
curl -s -u airflow:airflow \
  "http://127.0.0.1:8080/api/v2/dags?limit=100&offset=0" | jq .
```

Neu Airflow nam may khac:

```bash
curl -s -u airflow:airflow \
  "http://<airflow-server-ip>:8080/api/v2/dags?limit=100&offset=0" | jq .
```

Neu tra ve `401` hoac `403`, sai user/password hoac user khong co quyen doc DAG.

## 4. Tao script lay danh sach DAG ID tren Zabbix server

Tao file:

```text
/usr/local/bin/airflow_daginfo.sh
```

Noi dung:

```bash
#!/bin/bash
set -euo pipefail

API_BASE_URL="${AIRFLOW_API_BASE_URL:-http://127.0.0.1:8080/api/v2}"
USERNAME="${AIRFLOW_API_USERNAME:-airflow}"
PASSWORD="${AIRFLOW_API_PASSWORD:-airflow}"
LIMIT=100
OFFSET=0

OUT_FILE="/tmp/airflow_dag_ids.txt"
RESP_FILE="/tmp/airflow_dags_response.json"

> "$OUT_FILE"

while true; do
  HTTP_STATUS=$(
    curl -s -u "${USERNAME}:${PASSWORD}" \
      -o "$RESP_FILE" \
      -w "%{http_code}" \
      "${API_BASE_URL}/dags?limit=${LIMIT}&offset=${OFFSET}"
  )

  if [ "$HTTP_STATUS" -ne 200 ]; then
    echo "Error: Airflow API returned HTTP $HTTP_STATUS" >&2
    cat "$RESP_FILE" >&2 || true
    exit 1
  fi

  jq -r '.dags[].dag_id' "$RESP_FILE" >> "$OUT_FILE"

  DAG_COUNT=$(jq '.dags | length' "$RESP_FILE")
  if [ "$DAG_COUNT" -eq 0 ]; then
    break
  fi

  OFFSET=$((OFFSET + LIMIT))
done

sort -u "$OUT_FILE" -o "$OUT_FILE"
echo "Saved DAG IDs to $OUT_FILE"
```

Cap quyen:

```bash
sudo chmod +x /usr/local/bin/airflow_daginfo.sh
```

Chay thu:

```bash
AIRFLOW_API_BASE_URL="http://127.0.0.1:8080/api/v2" \
AIRFLOW_API_USERNAME="airflow" \
AIRFLOW_API_PASSWORD="airflow" \
/usr/local/bin/airflow_daginfo.sh
```

Kiem tra output:

```bash
cat /tmp/airflow_dag_ids.txt
```

Nen thay cac DAG cua project, vi du:

```text
mssql_to_bigquery_dbt
test_mssql_connection
```

## 5. Dat cron cap nhat DAG ID

Mo crontab:

```bash
crontab -e
```

Them dong sau de cap nhat moi 30 phut:

```cron
*/30 * * * * AIRFLOW_API_BASE_URL="http://127.0.0.1:8080/api/v2" AIRFLOW_API_USERNAME="airflow" AIRFLOW_API_PASSWORD="airflow" /usr/local/bin/airflow_daginfo.sh >/tmp/airflow_daginfo.log 2>&1
```

Production nen de credential trong file rieng chi user `zabbix` doc duoc, khong ghi plain text trong crontab dung chung.

## 6. Tao Host tren Zabbix UI

Vao Zabbix UI:

```text
Data collection -> Hosts -> Create host
```

Gia tri goi y:

```text
Host name: Airflow - BTMH
Visible name: Airflow - BTMH
Groups: Airflow
Interfaces: Agent 127.0.0.1:10050
```

Neu Zabbix agent nam tren may Airflow host, interface la IP cua may do.

## 7. Tao Master Item doc file DAG ID

Trong host `Airflow - BTMH`, tao item:

```text
Name: Airflow DAG ID master
Type: Zabbix agent
Key: vfs.file.contents[/tmp/airflow_dag_ids.txt]
Type of information: Text
Update interval: 30m
```

Item nay chi doc file `/tmp/airflow_dag_ids.txt` do script tao ra.

## 8. Tao Discovery Rule cho DAG

Trong host `Airflow - BTMH`, tao Discovery rule:

```text
Name: Airflow DAG Discovery
Type: Dependent item
Key: airflow.discovery.dags
Master item: Airflow DAG ID master
```

Preprocessing:

```text
Type: JavaScript
```

JavaScript:

```javascript
try {
    if (!value) {
        return JSON.stringify({ "data": [] });
    }

    var lines = value.split(/\r?\n/);
    var data = [];

    for (var i = 0; i < lines.length; i++) {
        var dagId = lines[i].trim();
        if (dagId) {
            data.push({ "{#DAG_ID}": dagId });
        }
    }

    return JSON.stringify({ "data": data });
} catch (error) {
    return JSON.stringify({ "data": [] });
}
```

Sau do bam `Test` voi input:

```text
mssql_to_bigquery_dbt
test_mssql_connection
```

Output dung:

```json
{"data":[{"{#DAG_ID}":"mssql_to_bigquery_dbt"},{"{#DAG_ID}":"test_mssql_connection"}]}
```

## 9. Tao Item Prototype lay latest DAG run status

Trong Discovery rule, tao Item prototype:

```text
Name: DAG {#DAG_ID} latest run failed
Type: HTTP agent
Key: airflow.dagrun.failed[{#DAG_ID}]
Type of information: Numeric (unsigned)
URL: http://127.0.0.1:8080/api/v2/dags/{#DAG_ID}/dagRuns?order_by=-logical_date&limit=1
Request method: GET
Update interval: 5m
HTTP authentication: Basic
User name: airflow
Password: airflow
Required status codes: 200
```

Neu Airflow khong nam cung may Zabbix, doi URL:

```text
http://<airflow-server-ip>:8080/api/v2/dags/{#DAG_ID}/dagRuns?order_by=-logical_date&limit=1
```

Preprocessing:

```text
Type: JavaScript
```

JavaScript:

```javascript
var parsedData;

try {
    parsedData = JSON.parse(value);
} catch (e) {
    return 2;
}

if (!parsedData.dag_runs || parsedData.dag_runs.length === 0) {
    return 0;
}

var latestRun = parsedData.dag_runs[0];
var state = latestRun.state;

if (state === "failed") {
    return 1;
}

if (state === "success" || state === "running" || state === "queued") {
    return 0;
}

return 0;
```

Quy uoc gia tri:

```text
0 = normal
1 = latest DAG run failed
2 = API/JSON parse error
```

## 10. Tao Trigger Prototype cho DAG failed

Trong Discovery rule, tao Trigger prototype:

```text
Name: DAG {#DAG_ID} latest run failed
Severity: Average
```

Problem expression:

```text
last(/Airflow - BTMH/airflow.dagrun.failed[{#DAG_ID}])=1
```

Recovery expression:

```text
last(/Airflow - BTMH/airflow.dagrun.failed[{#DAG_ID}])=0
```

Tao them trigger cho loi API neu muon:

```text
Name: DAG {#DAG_ID} Airflow API parse/check error
Severity: Warning
Problem expression:
last(/Airflow - BTMH/airflow.dagrun.failed[{#DAG_ID}])=2
```

## 11. Monitor Airflow component health

Theo bai viet goc, chi alert DAG failed la chua du. Neu scheduler hoac metadata DB chet, DAG co the khong chay va cung khong co failed run moi.

Tao 3 item HTTP agent doc endpoint:

```text
http://127.0.0.1:8080/health
```

Neu endpoint nay khong ton tai trong version dang chay, dung endpoint component-level ben duoi:

```text
API server: http://127.0.0.1:8080/api/v2/version
Scheduler:  http://<docker-host>:8974/health
```

Luu y: trong compose hien tai scheduler port `8974` chi la healthcheck noi bo container, chua expose ra host. Neu can Zabbix goi truc tiep tu ngoai Docker, them ports cho `airflow-scheduler`:

```yaml
ports:
  - "8974:8974"
```

Sau do recreate container.

### 11.1 API server health

Item:

```text
Name: Airflow API server health
Type: HTTP agent
Key: airflow.api.health
Type of information: Numeric (unsigned)
URL: http://127.0.0.1:8080/api/v2/version
Required status codes: 200
Update interval: 1m
```

Preprocessing JavaScript:

```javascript
try {
    var parsedData = JSON.parse(value);
    if (parsedData.version) {
        return 0;
    }
    return 1;
} catch (e) {
    return 1;
}
```

Trigger:

```text
Name: Airflow API server unhealthy
Problem expression:
last(/Airflow - BTMH/airflow.api.health)=1
Severity: High
```

### 11.2 Scheduler health

Neu da expose port `8974`, tao item:

```text
Name: Airflow scheduler health
Type: HTTP agent
Key: airflow.scheduler.health
Type of information: Numeric (unsigned)
URL: http://127.0.0.1:8974/health
Required status codes: 200
Update interval: 1m
```

Preprocessing JavaScript:

```javascript
try {
    var parsedData = JSON.parse(value);
    if (parsedData.status === "healthy") {
        return 0;
    }
    return 1;
} catch (e) {
    return 1;
}
```

Trigger:

```text
Name: Airflow scheduler unhealthy
Problem expression:
last(/Airflow - BTMH/airflow.scheduler.health)=1
Severity: High
```

### 11.3 Metadata DB va triggerer health

Neu endpoint `/health` cua Airflow API server tra ve JSON co field `metadatabase` va `triggerer`, co the tao item theo bai viet goc:

```text
URL: http://127.0.0.1:8080/health
```

Metadata DB preprocessing:

```javascript
try {
    var parsedData = JSON.parse(value);
    if (parsedData.metadatabase && parsedData.metadatabase.status === "healthy") {
        return 0;
    }
    return 1;
} catch (e) {
    return 1;
}
```

Triggerer preprocessing:

```javascript
try {
    var parsedData = JSON.parse(value);
    if (parsedData.triggerer && parsedData.triggerer.status === "healthy") {
        return 0;
    }
    return 1;
} catch (e) {
    return 1;
}
```

Neu `/health` khong co cac field nay tren Airflow 3.1.6, monitor container health thay the bang Docker/Zabbix agent hoac `docker ps` item rieng.

## 12. Test canh bao DAG failed

Tao mot DAG test rieng trong `run_env/dags`:

```python
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def always_fail():
    raise RuntimeError("Zabbix alert test")


with DAG(
    dag_id="zabbix_alert_test",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(
        task_id="always_fail",
        python_callable=always_fail,
    )
```

Chay DAG `zabbix_alert_test` tren Airflow UI.

Sau 5 phut, Zabbix phai tao problem:

```text
DAG zabbix_alert_test latest run failed
```

Sau khi test xong, co the xoa DAG test.

## 13. Checklist van hanh

Kiem tra sau khi dung xong:

- `/tmp/airflow_dag_ids.txt` co danh sach DAG.
- Master item doc duoc file DAG ID.
- Discovery rule tao ra item cho tung DAG.
- Item prototype tra ve `0` voi DAG success/running.
- Item prototype tra ve `1` voi DAG failed.
- Trigger prototype tao problem khi DAG failed.
- API server health item tra ve `0`.
- Scheduler health item tra ve `0` neu da expose port `8974`.
- Zabbix action da cau hinh gui Slack/Email/Telegram theo kenh team dung.

## 14. Loi thuong gap

Sai API version:

```text
Dung /api/v1 voi Airflow 3
```

Cach sua:

```text
Dung /api/v2.
```

Sai endpoint order field:

```text
order_by=-execution_date
```

Cach sua cho Airflow 3:

```text
order_by=-logical_date
```

Zabbix khong doc duoc DAG ID:

```text
Permission denied: /tmp/airflow_dag_ids.txt
```

Cach sua:

```bash
ls -l /tmp/airflow_dag_ids.txt
sudo chown zabbix:zabbix /tmp/airflow_dag_ids.txt
```

HTTP item bi 401/403:

```text
Sai credential hoac user khong co quyen doc DAG.
```

Cach sua:

```text
Tao Airflow user rieng cho Zabbix va gan role co quyen read DAG/DAG Runs.
```

Khong alert khi scheduler chet:

```text
Chi monitor latest DAG run failed.
```

Cach sua:

```text
Them item health cho API server, scheduler, metadata DB, triggerer/container health.
```

## 15. Nguon tham khao

- Bai viet goc: https://quirky-guy.com/zabbix-airflow-monitoring-dag-health-en/
- Airflow 3 dung REST API v2: https://docs.cloud.google.com/composer/docs/access-airflow-api
- Airflow REST API v2 example: https://www.astronomer.io/docs/astro/airflow-api

