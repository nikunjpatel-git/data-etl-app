FROM apache/airflow:2.7.3

USER airflow

# Install Databricks provider and AWS provider (optional for secrets)
RUN pip install --no-cache-dir apache-airflow-providers-databricks==6.4.0
