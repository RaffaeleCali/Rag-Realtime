# Usa un'immagine base di Python con Streamlit
FROM python:3.8-slim

# Installa le dipendenze
RUN pip install --upgrade pip
COPY ragui/requirements.txt .
RUN pip install -r requirements.txt

# Copia il codice dell'applicazione
RUN mkdir -p /app 
COPY ./ragui/code /app

# Imposta la directory di lavoro
WORKDIR /app

# Comando per eseguire l'applicazione Streamlit
CMD ["streamlit", "run", "Rag.py"]
