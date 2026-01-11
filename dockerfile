ARG AIRFLOW_VERSION=3.1.5
ARG PYTHON_VERSION=3.13

#FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
FROM apache/airflow:${AIRFLOW_VERSION}

ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt /

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
