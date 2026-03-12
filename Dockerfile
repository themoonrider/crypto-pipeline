FROM apache/airflow:2.8.1-python3.11

USER airflow

RUN pip install --no-cache-dir \
    massive \
    coingecko-sdk \
    apache-airflow-providers-google \
    dbt-bigquery
