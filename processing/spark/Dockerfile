FROM bitnami/spark:3.5

USER root

# Iceberg runtime cho Spark 3.5
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    -O /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar

# AWS bundle — S3FileIO để ghi/đọc MinIO
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar \
    -O /opt/bitnami/spark/jars/iceberg-aws-bundle-1.5.2.jar

COPY jobs/ /opt/spark/jobs/

USER 1001
