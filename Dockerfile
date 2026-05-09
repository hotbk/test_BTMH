FROM apache/airflow:3.1.6

ARG AIRFLOW_VERSION=3.1.6

USER root
RUN apt-get update && \
        apt-get install -y --no-install-recommends \
            git \
            libgomp1 \
            ca-certificates \
            curl \
            unixodbc \
            unixodbc-dev \
            && \
        (ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
            || (echo 'msodbcsql18 not available on this platform; falling back to FreeTDS ODBC' && apt-get install -y --no-install-recommends tdsodbc freetds-bin)) && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Install pinned, compatible deps using the official constraints for this Airflow release.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"

# Keep dbt isolated from Airflow's constrained dependency set.
RUN python -m venv /home/airflow/.local/dbt-venv && \
    /home/airflow/.local/dbt-venv/bin/pip install --no-cache-dir dbt-bigquery==1.10.3 && \
    ln -sf /home/airflow/.local/dbt-venv/bin/dbt /home/airflow/.local/bin/dbt
