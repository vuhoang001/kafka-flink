FROM flink:1.17-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip wget && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install apache-flink==1.17.2 --no-cache-dir

# Kafka SQL connector
COPY flink-sql-connector-kafka-1.17.2.jar /opt/flink/lib/

# Iceberg Flink runtime 1.5.2 (phiên bản cuối hỗ trợ Flink 1.17)
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.17/1.5.2/iceberg-flink-runtime-1.17-1.5.2.jar \
    -O /opt/flink/lib/iceberg-flink-runtime-1.17-1.5.2.jar

# Iceberg AWS bundle — S3FileIO, tránh dùng HadoopFileIO
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar \
    -O /opt/flink/lib/iceberg-aws-bundle-1.5.2.jar

# Hadoop deps — Iceberg FlinkCatalogFactory cần HdfsConfiguration + transitive deps
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar \
        -O /opt/flink/lib/hadoop-common.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.4/hadoop-hdfs-client-3.3.4.jar \
        -O /opt/flink/lib/hadoop-hdfs-client.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.4/hadoop-auth-3.3.4.jar \
        -O /opt/flink/lib/hadoop-auth.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar \
        -O /opt/flink/lib/hadoop-shaded-guava.jar && \
    wget -q https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/6.2.4/woodstox-core-6.2.4.jar \
        -O /opt/flink/lib/woodstox-core.jar && \
    wget -q https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar \
        -O /opt/flink/lib/stax2-api.jar

# Tắt Hadoop security module và delegation token manager
# security.delegation.tokens.enabled: false — tắt DefaultDelegationTokenManager hoàn toàn,
# HadoopFSDelegationTokenProvider sẽ không bao giờ được gọi → UGI không bị khởi động
RUN echo "" >> /opt/flink/conf/flink-conf.yaml && \
    echo "security.module.factory.classes: org.apache.flink.runtime.security.modules.JaasModuleFactory" >> /opt/flink/conf/flink-conf.yaml && \
    echo "security.delegation.tokens.enabled: false" >> /opt/flink/conf/flink-conf.yaml

# S3/MinIO filesystem plugin
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

COPY . /opt/flink/jobs/
