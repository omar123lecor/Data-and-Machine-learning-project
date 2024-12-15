FROM apache/airflow:2.10.3
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  chromium \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir --upgrade pip "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt