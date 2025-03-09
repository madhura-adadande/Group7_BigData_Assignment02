# Extend the official Airflow image
FROM apache/airflow:2.10.4

# Install required dependencies before switching to a user
RUN pip install --no-cache-dir \
    selenium requests boto3 webdriver-manager python-dotenv pandas \
    dbt-snowflake apache-airflow-providers-amazon apache-airflow-providers-snowflake \
    apache-airflow[celery] redis apache-airflow-providers-redis

# Switch to the airflow user
USER airflow