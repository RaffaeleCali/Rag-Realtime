FROM apache/spark:3.4.2

USER 0
RUN pip3 install pyspark numpy  elasticsearch

WORKDIR /app


RUN pip install chromadb langchain langchain_community langchain_chroma


RUN pip install einops nomic numpy
RUN pip install chromadb 
RUN pip install langchain_chroma
RUN pip install sentence_transformers
RUN pip3 install chromadb
RUN pip3 install  langchain_elasticsearch
RUN pip install langchain_nomic
COPY ./code .
USER spark
