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

# hadoop-common + hadoop-hdfs-client — Iceberg 1.5.2 FlinkCatalogFactory trực tiếp dùng HdfsConfiguration
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar \
    -O /opt/flink/lib/hadoop-common.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.4/hadoop-hdfs-client-3.3.4.jar \
    -O /opt/flink/lib/hadoop-hdfs-client.jar

# Transitive deps thiếu của hadoop-common (Flink không cung cấp vì shade nội bộ):
#
# ROOT CAUSE đã xác định từ client log:
#   HadoopUtils.<clinit>(line 47) → new Text(...) → Text.<clinit> → WritableComparator
#   → WritableComparator gọi new Configuration() rồi parse XML → cần Woodstox XML parser
#   → ClassNotFoundException: com.ctc.wstx.io.InputBootstrapper (class trong woodstox-core)
#
# - woodstox-core: fast XML parser, hadoop-common dùng khi parse core-default.xml
# - stax2-api: StAX2 API mà woodstox-core implement
# - guava + commons-lang3: đã có sẵn từ Flink base image layer trước
RUN wget -q https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/6.2.4/woodstox-core-6.2.4.jar \
    -O /opt/flink/lib/woodstox-core.jar && \
    wget -q https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar \
    -O /opt/flink/lib/stax2-api.jar

# S3/MinIO filesystem plugin
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

COPY . /opt/flink/jobs/
