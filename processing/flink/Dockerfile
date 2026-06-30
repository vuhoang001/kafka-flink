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

# Transitive deps thiếu của hadoop-common 3.3.4:
#
# hadoop-shaded-guava: Hadoop 3.3.x dùng org.apache.hadoop.thirdparty.com.google.common.*
#   thay vì com.google.common.* (Guava chuẩn). Configuration.<clinit> cần ngay lập tức.
# woodstox-core + stax2-api: Woodstox XML parser cho core-default.xml
#
# KHÔNG thêm hadoop-auth: nó cho phép UGI init tiến xa hơn rồi crash bằng Error không bị catch
# → HadoopModule.install() ném NoClassDefFoundError (Error, không phải Exception)
# → Flink không catch → JobManager/client process chết.
# Thay vào đó, ta disable HadoopModuleFactory hoàn toàn bên dưới.
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar \
    -O /opt/flink/lib/hadoop-shaded-guava.jar && \
    wget -q https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/6.2.4/woodstox-core-6.2.4.jar \
    -O /opt/flink/lib/woodstox-core.jar && \
    wget -q https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar \
    -O /opt/flink/lib/stax2-api.jar

# Có HAI nơi trong Flink 1.17 cố gọi UserGroupInformation (UGI) khi startup:
#
# 1. HadoopModule.install() — tắt bằng security.module.factory.classes
# 2. HadoopFSDelegationTokenProvider.delegationTokensRequired() — được gọi trong
#    DefaultDelegationTokenManager.obtainDelegationTokens() khi JobManager init services
#    Tắt bằng security.kerberos.token.provider.hadoopfs.enabled: false
#
# Không thêm hadoop-auth vì nó cho phép UGI init tiến xa hơn nhưng vẫn crash,
# và error đó (NoClassDefFoundError) là Error không bị Flink catch → process chết.
# Ta không cần UGI/Kerberos: dùng REST catalog + S3FileIO với credentials thuần túy.
RUN echo "" >> /opt/flink/conf/flink-conf.yaml && \
    echo "security.module.factory.classes: org.apache.flink.runtime.security.modules.JaasModuleFactory" >> /opt/flink/conf/flink-conf.yaml && \
    echo "security.kerberos.token.provider.hadoopfs.enabled: false" >> /opt/flink/conf/flink-conf.yaml

# S3/MinIO filesystem plugin
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

COPY . /opt/flink/jobs/
