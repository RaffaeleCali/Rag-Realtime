# Usa un'immagine base di Ubuntu
FROM ubuntu:20.04

# Evita di dover selezionare manualmente l'opzione di timezone durante l'installazione di pacchetti
ENV DEBIAN_FRONTEND=noninteractive

# Aggiorna i pacchetti di sistema e installa Python e altri strumenti necessari
RUN apt-get update && apt-get install -y \
    python3.8 \
    python3-pip \
    curl \
    openjdk-11-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Imposta Python 3.8 come python predefinito
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.8 1 \
    && update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1

# Imposta JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

# Copia il file di requisiti prima, per migliorare la cache dei layer di Docker
COPY ragui/requirements.txt /app/requirements.txt

# Cambia la directory di lavoro
WORKDIR /app

# Installa le dipendenze Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia il resto del codice dell'applicazione nella directory /app
COPY ./ragui/code .

# Espone la porta su cui gira Streamlit
EXPOSE 8501

# Installa ulteriori pacchetti Python
RUN pip install sparknlp huggingface-cli
RUN pip install --upgrade --quiet  johnsnowlabs
# Comando per eseguire l'applicazione Streamlit
CMD ["streamlit", "run", "Rag.py"]
