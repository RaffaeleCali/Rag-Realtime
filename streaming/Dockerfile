FROM apache/spark:3.4.2

USER 0
RUN pip3 install numpy  elasticsearch

RUN pip3 install langchain langchain_community 


RUN pip3 install einops nomic numpy
#RUN pip install chromadb 
#RUN pip install langchain_chroma
#RUN pip install sentence_transformers
#RUN pip3 install chromadb
RUN pip3 install  langchain_elasticsearch
RUN pip3 install sentence_transformers
RUN pip install spark-nlp==5.3.3
#RUN pip install sparknlp.annotator.embeddings.nomic_embeddings
RUN apt-get update && apt-get install -y curl
RUN mkdir -p /home/spark && chmod -R 777 /home/spark
COPY ./streaming/code /opt/spark/work-dir
WORKDIR /opt/spark/work-dir
USER spark
