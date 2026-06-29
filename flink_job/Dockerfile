FROM flink:1.17-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip wget && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install apache-flink==1.17.2 --no-cache-dir

# Kafka SQL connector
RUN wget -q -P /opt/flink/lib/ \
    "https://repo1.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar"

# S3/MinIO filesystem plugin (JAR đã có sẵn trong image, chỉ cần kích hoạt)
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

COPY . /opt/flink/jobs/
