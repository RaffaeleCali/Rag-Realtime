FROM apache/spark:3.4.2

USER 0

# Installare pip3 e altre dipendenze di sistema necessarie
RUN apt-get update && apt-get install -y \
    curl \
    python3-pip

# Installare le librerie Python necessarie
RUN pip3 install numpy \
    elasticsearch \
    langchain_elasticsearch \
    spark-nlp==5.3.3

# Configurare Ivy per memorizzare la cache delle dipendenze
ENV SPARK_OPTS="--conf spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp"

# Creare una directory di lavoro e assegnare i permessi
RUN mkdir -p /home/spark && chown -R spark:spark /home/spark && chmod -R 755 /home/spark

# Impostare i permessi sulla directory /tmp
RUN chmod -R 777 /tmp

# Copiare il codice Python nella directory di lavoro
COPY ./streaming/code /home/spark
WORKDIR /home/spark

# Impostare l'utente per l'esecuzione
USER spark

# Impostare il comando di avvio
CMD ["/opt/spark/bin/spark-submit", "--conf", "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4,com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3", "/home/spark/streaming.py"]
