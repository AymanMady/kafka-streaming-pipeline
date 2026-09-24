# =============================================================================
#  Spark image for this project (phase 8).
#
#  One image for the three roles (master, worker, driver): the driver and the
#  executors MUST run the same Spark and Python version, otherwise Spark
#  refuses to start ("Python in worker has different version").
#
#  What this image adds to the official one:
#    - the Kafka connector for Structured Streaming;
#    - the PostgreSQL JDBC driver (phase 15);
#    - the project's Python dependencies.
# =============================================================================

FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu

USER root

ARG HOST_UID=1000
ARG HOST_GID=1000

RUN groupadd -g "${HOST_GID}" sparkuser 2>/dev/null || true && \
    useradd -m -u "${HOST_UID}" -g "${HOST_GID}" -s /bin/bash sparkuser 2>/dev/null || true

# ---------------------------------------------------------------------------
#  JARs
#
#  We bake them into /opt/spark/jars rather than using `--packages` at submit
#  time: --packages resolves from Maven on EVERY launch, which needs the
#  network and adds ~20 s. Baked in, they are on the classpath of the driver
#  AND of every executor, with no resolution step.
#
#  These four versions go together. spark-sql-kafka-0-10 3.5.6 is built
#  against kafka-clients 3.4.1 and commons-pool2 2.11.1; mixing versions gives
#  NoSuchMethodError at the first micro-batch, not at startup.
# ---------------------------------------------------------------------------
ARG SPARK_VERSION=3.5.6
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG POSTGRES_JDBC_VERSION=42.7.4

ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar /opt/spark/jars/

RUN chmod 644 /opt/spark/jars/*.jar

# ---------------------------------------------------------------------------
#  Python dependencies
#  PySpark is NOT reinstalled: it already ships with the image.
# ---------------------------------------------------------------------------
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

WORKDIR /opt/workspace

# /opt/workspace on the path so "from src.common import config" resolves;
# /opt/spark/python + py4j so a plain "python3" can "import pyspark"
# (spark-submit sets this up on its own, pytest does not).
ENV PYTHONPATH=/opt/workspace:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PATH="${PATH}:/opt/spark/bin:/opt/spark/sbin"
ENV HOME=/home/sparkuser

RUN mkdir -p /opt/spark/logs /opt/spark/work && \
    chown -R "${HOST_UID}:${HOST_GID}" /opt/spark/logs /opt/spark/work /opt/workspace /home/sparkuser

USER sparkuser
