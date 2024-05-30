FROM ollama/ollama:latest

USER root
RUN apt-get update && apt-get install -y python3 python3-pip && ln -s /usr/bin/python3 /usr/bin/python

RUN apt-get install nano
 
RUN pip install flask 
RUN pip install sentence_transformers

# Switch back to jovyan to avoid accidental container runs as root


RUN pip install chromadb langchain langchain_community langchain_chroma


RUN pip install einops nomic numpy

RUN mkdir data
RUN pip install langchain_chroma
#RUN pip install hashlib transformers torch 

#COPY ollama/ollama /root/.ollama
