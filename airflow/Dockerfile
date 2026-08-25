FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/cloudscale-requirements.txt
RUN pip install --no-cache-dir -r /tmp/cloudscale-requirements.txt
