FROM flink:1.17-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip wget && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install apache-flink==1.17.2 --no-cache-dir

# Kafka SQL connector
COPY flink-sql-connector-kafka-1.17.2.jar /opt/flink/lib/

# Iceberg Flink runtime 1.5.2 — phiên bản cuối hỗ trợ Flink 1.17 (1.6.x đã drop)
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.17/1.5.2/iceberg-flink-runtime-1.17-1.5.2.jar \
    -O /opt/flink/lib/iceberg-flink-runtime-1.17-1.5.2.jar

# AWS bundle — dùng S3FileIO thay vì HadoopFileIO, tránh classloader conflict với S3A plugin
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar \
    -O /opt/flink/lib/iceberg-aws-bundle-1.5.2.jar

# Hadoop shaded uber JAR — cần cho Iceberg FlinkCatalogFactory (dùng main classloader, không phải plugin)
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-3-uber/3.1.1.7.2.9.0-173-9.0/flink-shaded-hadoop-3-uber-3.1.1.7.2.9.0-173-9.0.jar \
    -O /opt/flink/lib/flink-shaded-hadoop-3-uber.jar

# S3/MinIO filesystem plugin
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

COPY . /opt/flink/jobs/
