### README.md del Progetto "RAG Real Time"

---

# RAG Real Time

## Introduzione
"RAG Real Time" è un sistema di data processing in tempo reale progettato per raccogliere, elaborare, classificare e visualizzare dati provenienti da Arxiv e Google News. Utilizza tecnologie avanzate come Docker, Kafka, Spark, Elasticsearch e Kibana per creare un sistema robusto e scalabile.

## Struttura del Progetto
Il progetto è organizzato in più container Docker che interagiscono tra loro per implementare il flusso di dati. Di seguito sono riportati i componenti principali e le operazioni preliminari necessarie per avviare il sistema.

## Requisiti
- Docker
- Docker Compose

## Clonazione del Repository

Prima di tutto, clonare il repository del progetto:

```bash
git clone <URL_DEL_REPOSITORY>
cd <NOME_DEL_REPOSITORY>
```

## Operazioni Preliminari

Prima di avviare il sistema, è necessario costruire alcune immagini Docker e configurare i container. Seguire i passaggi seguenti per preparare l'ambiente.

### 1. Build delle Immagini Docker

#### Server Python (`server_data`)

```bash
cd server_data
docker build -t server_data .
cd ..
```

#### Spark (`sparkrc`)

```bash
cd progetto_sp
docker build -t sparkrc -f streaming/Dockerfile .
cd ..
```

#### Streamlit RAG (`streamlit_rag`)

```bash
cd ragstramlit
docker build -t streamlit_rag .
cd ..
```

### 2. Download del Modello Gemma in Ollama

Entrare nel container `ollama` ed effettuare il download del modello `gemma:2b`.

```bash
docker exec -it ollama bash
ollama pull gemma:2b
exit
```

### 3. Avvio del Sistema

Una volta completate le operazioni preliminari, avviare il sistema utilizzando Docker Compose.

```bash
docker-compose up -d
```

### 4. Arresto del Sistema

Per arrestare il sistema, utilizzare il comando:

```bash
docker-compose down
```

## Componenti del Sistema

1. **Server Python** (`flask_server`): Raccolta dei dati da Arxiv e Google News.
2. **Logstash** (`logstash`): Ingestion dei dati e invio a Kafka.
3. **Zookeeper** (`zookeeper`): Coordinazione per Kafka.
4. **Kafka** (`broker`): Broker di messaggi per il trasferimento dei dati.
5. **Spark** (`spark`): Processing dei dati in tempo reale.
6. **Elasticsearch** (`elasticsearch`): Storage e ricerca dei dati.
7. **Kibana** (`kibana`): Visualizzazione dei dati in tempo reale.
8. **Streamlit RAG** (`rag`): Implementazione della chat RAG.
9. **Ollama** (`ollama`): Modelli LLM per la chat RAG.

## Utilizzo del Sistema

### Avvio del Sistema

Per avviare il sistema, eseguire il comando:

```bash
docker-compose up -d
```

### Arresto del Sistema

Per arrestare il sistema, eseguire il comando:

```bash
docker-compose down
```

## Conclusioni

"RAG Real Time" è progettato per gestire grandi volumi di dati in tempo reale utilizzando tecnologie avanzate. La scelta di Docker, Kafka, Spark, Elasticsearch e Kibana permette di creare un sistema scalabile e efficiente per il processamento e la visualizzazione dei dati. Seguendo le operazioni preliminari e utilizzando Docker Compose, è possibile configurare e avviare facilmente il sistema.

---
