FROM jupyter/pyspark-notebook:spark-3.1.1

RUN pip3 install pyspark numpy  elasticsearch

WORKDIR /app


RUN pip install chromadb langchain langchain_community langchain_chroma


RUN pip install einops nomic numpy
RUN pip install chromadb 
RUN pip install langchain_chroma
RUN pip install sentence_transformers
COPY ./code .

ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1", "--master", "local[*]", "streaming.py"]