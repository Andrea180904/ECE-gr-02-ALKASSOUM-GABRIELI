# Same base image as the course labs, plus the Python libraries used by the
# Kafka producer and the enrichment collectors.
FROM quay.io/jupyter/pyspark-notebook:latest

RUN pip install --no-cache-dir \
    "confluent-kafka>=2.4" \
    "requests>=2.31"
