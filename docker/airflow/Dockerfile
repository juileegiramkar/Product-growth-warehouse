FROM apache/airflow:2.10.5-python3.11

USER root
# Java for PySpark (local mode inside the worker)
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Pipeline tools live in their own venv so they can't break Airflow's pins
COPY docker/requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN python -m venv /opt/pulse-venv \
 && /opt/pulse-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/pulse-venv/bin/pip install --no-cache-dir -r /tmp/requirements-pipeline.txt \
 && chown -R airflow:0 /opt/pulse-venv
ENV PATH="/opt/pulse-venv/bin:${PATH}" \
    PYSPARK_PYTHON=/opt/pulse-venv/bin/python

USER airflow
