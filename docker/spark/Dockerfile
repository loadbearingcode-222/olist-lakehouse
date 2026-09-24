# Spark for the streaming ingest (M2): Spark 4.1.3 + Iceberg 1.11.0 + the Kafka source + S3A.
# Versions are pinned together (ADR-0003): Iceberg has no Spark 4.2 runtime yet, hadoop-aws must
# equal the image's Hadoop (3.4.2), and the AWS SDK bundle is the one hadoop-aws 3.4.2 declares.
FROM apache/spark:4.1.3-scala2.13-java21-python3-ubuntu

USER root

# Extra jars, each verified against Maven Central's published SHA-1 before it is kept.
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eu; cd /opt/spark/jars; \
    for path in \
      org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
      org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar \
      org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar \
      org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
      org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar \
      org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar \
      software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar \
    ; do \
      file=$(basename "$path"); \
      curl -fsSL --retry 3 -o "$file" "$MAVEN/$path"; \
      echo "$(curl -fsSL --retry 3 "$MAVEN/$path.sha1" | cut -c1-40)  $file" | sha1sum -c -; \
    done

# The project's own Python (3.12, from uv.lock), so the job runs on the same interpreter and
# dependencies as everything else. PySpark itself comes from the image (/opt/spark/python).
COPY --from=ghcr.io/astral-sh/uv:0.11.25 /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python \
    PYTHONUNBUFFERED=1
WORKDIR /opt/app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --group quality --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev --group quality && chown -R spark:spark /opt/app

COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
