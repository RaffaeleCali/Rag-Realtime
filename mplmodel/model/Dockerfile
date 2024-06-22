FROM apache/spark:3.4.2

USER 0

# Installare pip3 e altre dipendenze di sistema necessarie
RUN apt-get update && apt-get install -y \
    curl \
    python3-pip

# Installare le librerie Python necessarie
RUN pip3 install numpy \
    elasticsearch \
    spark-nlp==5.3.3

ENV SPARK_OPTS="--conf spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp"

RUN mkdir -p /home/spark && chown -R spark:spark /home/spark && chmod -R 755 /home/spark

RUN chmod -R 777 /tmp

COPY ./model /home/spark
WORKDIR /home/spark

USER spark