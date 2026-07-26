# =============================================================================
# Flink image for stream-processing jobs on top of the Debezium CDC pipeline.
# Adds PyFlink (Python Table API) and the Kafka SQL connector to the official
# Flink runtime. Job scripts are bind-mounted (see docker-compose.yml), so
# this image only provides the runtime + connector jar.
# =============================================================================
FROM flink:2.2.1-scala_2.12-java17

# The base image runs as the unprivileged `flink` user; installs need root.
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 \
      python3-pip \
      python3-dev \
      gcc \
      curl \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && pip3 install --no-cache-dir --break-system-packages apache-flink==2.2.1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Kafka SQL connector (bundles Kafka clients) + the debezium-json/json table
# formats, needed by the Table API source/sink defined in flink/jobs/.
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar \
      https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar

USER flink
