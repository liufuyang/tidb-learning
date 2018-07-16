FROM jupyter/pyspark-notebook:8e15d329f1e9
# 8e15d329f1e9 has spark version 2.1.1

ENV TISPARK_VERSION=1.0
USER root 
RUN wget https://github.com/pingcap/tispark/releases/download/${TISPARK_VERSION}/tispark-core-${TISPARK_VERSION}-jar-with-dependencies.jar -P /usr/local/spark/jars

ADD spark-defaults.conf /usr/local/spark/conf/spark-defaults.conf

USER 1000

RUN mkdir -p /home/jovyan/work \
  && pip2 install pytispark \
  && pip install pytispark \
  && rm -rf /home/jovyan/work
