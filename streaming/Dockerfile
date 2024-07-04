FROM apache/spark:3.4.2

USER 0

RUN apt-get update && apt-get install -y \
    curl \
    python3-pip

RUN pip3 install numpy \
    spark-nlp==5.3.3


ENV SPARK_OPTS="--conf spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp"

RUN mkdir -p /home/spark && chown -R spark:spark /home/spark && chmod -R 755 /home/spark

RUN chmod -R 777 /tmp

COPY ./streaming/code /home/spark
#modello mlp 
COPY ./mlpmodel/tmp/string_indexer_model /home/spark/string_indexer_model
COPY ./mlpmodel/tmp/mod_mlp/spark_mlp_modelv /home/spark/spark_mlp_modelv 
WORKDIR /home/spark

USER spark
