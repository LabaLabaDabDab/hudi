FROM spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root

ARG HADOOP_VERSION=3.3.6
ENV HADOOP_VERSION=${HADOOP_VERSION}
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=$PATH:/opt/hadoop/bin:/opt/hadoop/sbin
ENV SPARK_LOG_DIR=/opt/spark/logs

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl wget tar bash tini procps netcat-openbsd git maven xmlstarlet gettext-base && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt && \
    curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
      | tar -xz -C /opt && \
    ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop && \
    mkdir -p /data/dfs/name /data/dfs/data /workspace/hudi /workspace/hudi-bench /workspace/spark-events /opt/spark/logs

COPY conf/core-site.xml.template /opt/templates/core-site.xml.template
COPY conf/hdfs-site.xml.template /opt/templates/hdfs-site.xml.template
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY scripts/ /opt/scripts/
RUN chmod +x /opt/scripts/*.sh

WORKDIR /workspace/hudi
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/opt/scripts/run-hudi-dev.sh"]