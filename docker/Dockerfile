# From https://towardsdatascience.com/how-to-run-jupyter-notebook-on-docker-7c9748ed209f
ARG BASE_CONTAINER=jupyter/pyspark-notebook
FROM $BASE_CONTAINER
USER root
RUN pip install findspark confluent_kafka pyspark flask nltk 
RUN pip install transformers torch
# Switch back to jovyan to avoid accidental container runs as root
USER $NB_UID

ENV JUPYTER_ENABLE_LAB="yes"
RUN pip install chromadb
RUN pip install ollama langchain
#Assume current directory is the folder where this Dockerfile resides
#FROM ollama/ollama:0.1.37
RUN pip install langchain_community
#COPY ./.ollama/ /root/.ollama/
RUN pip install sentence_transformers
RUN pip install einops
RUN pip install nomic
RUN pip install numpy
RUN mkdir data
RUN pip install langchain_chroma
#RUN pip install hashlib
#RUN  ollama run gemma:2b
COPY Untitled.ipynb prova.ipynb
COPY  app.py app.py
COPY  spark-sql-kafka-0-10_2.12-3.0.0.jar  /home/raffaelecali/anaconda3/lib/python3.9/site-packages/pyspark/jars



