# Start from the official Airflow image
FROM apache/airflow:2.10.4-python3.11

# Install the necessary Airflow provider for Spark
RUN pip install apache-airflow-providers-apache-spark

# Install pyspark for the Airflow worker/scheduler to submit jobs
# Ensure the pyspark version is compatible with the Spark image you choose (e.g., bitnami/spark:latest)
RUN pip install pyspark

# Add your local requirements if needed (e.g., redis-py)
RUN pip install redis