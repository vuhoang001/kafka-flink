FROM flink:1.17-scala_2.12-java11

# Cài Python để chạy PyFlink
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip wget && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# PyFlink phải cùng version với Flink cluster
RUN pip3 install apache-flink==1.17.2 --no-cache-dir

# Kafka SQL connector: cho phép Flink đọc/ghi Kafka bằng Table API / SQL
RUN wget -q -P /opt/flink/lib/ \
    "https://repo1.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar"

COPY . /opt/flink/jobs/
