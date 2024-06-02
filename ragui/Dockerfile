# Usa un'immagine base di Python con Streamlit
FROM python:3.8-slim

# Installa le dipendenze
RUN pip install --upgrade pip
COPY ragui/requirements.txt .
RUN pip install -r requirements.txt

# Copia il codice dell'applicazione
RUN mkdir -p /app 
COPY ./ragui/code /app
# Install curl
RUN apt-get update && apt-get install -y curl
# Imposta la directory di lavoro
WORKDIR /app
# Make port 8501 available to the world outside this container
EXPOSE 8501
# Comando per eseguire l'applicazione Streamlit
CMD ["streamlit", "run", "Rag.py"]
