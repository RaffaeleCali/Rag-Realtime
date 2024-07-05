### README.md for "RAG Real Time" Project

---

# RAG Real Time

## Introduction
"RAG Real Time" is a real-time data processing system designed to collect, process, classify, and visualize data from Arxiv and Google News. This project leverages advanced technologies such as Docker, Kafka, Spark, Elasticsearch, and Kibana to build a robust and scalable system. Additionally, it enables the real-time updating of a vector database, which can be utilized via LongChain to support language models.

## Project Structure
The project is organized into multiple Docker containers that interact with each other to implement the data flow. Below are the main components and the preliminary steps needed to set up the system.

## Requirements
- Docker
- Docker Compose

## Clone the Repository

First, clone the project repository:

```bash
git clone <REPOSITORY_URL>
cd <REPOSITORY_NAME>
```

## Preliminary Steps

Before starting the system, you need to build some Docker images and configure the containers. Follow these steps to set up the environment.

### 1. Build Docker Images

#### Python Server (`server_data`)

```bash
cd server_data
docker build -t server_data .
cd ..
```

#### Spark (`sparkrc`)

```bash
docker build -t sparkrc -f streaming/Dockerfile .
```

#### Streamlit RAG (`streamlit_rag`)

```bash
cd ragstramlit
docker build -t streamlit_rag .
cd ..
```

### 2. Download the Gemma Model in Ollama

Enter the `ollama` container and download the `gemma:2b` model.

```bash
docker-compose up -d ollama
```
```bash
docker exec -it ollama bash
ollama pull gemma:2b
exit
```

### 3. Start the System

Once the preliminary steps are complete, start the system using Docker Compose.

```bash
docker-compose up 
```

### 4. Stop the System

To stop the system, use the following command:

```bash
docker-compose down
```

## System Components

1. **Python Server** (`flask_server`): Collects data from Arxiv and Google News.
2. **Logstash** (`logstash`): Ingests data and sends it to Kafka.
3. **Zookeeper** (`zookeeper`): Coordination service for Kafka.
4. **Kafka** (`broker`): Message broker for data transfer.
5. **Spark** (`spark`): Real-time data processing.
6. **Elasticsearch** (`elasticsearch`): Data storage and search.
7. **Kibana** (`kibana`): Real-time data visualization.
8. **Streamlit RAG** (`rag`): Implements the RAG chat interface.
9. **Ollama** (`ollama`): Hosts the LLM models for the RAG chat.

## Using the System

Access the system components via the following ports:

- **Kibana**: `http://localhost:5601`
- **Chat RAG with Streamlit**: `http://localhost:8501`
- **Manual File Upload**: `http://localhost:5000`
- **Elasticsearch**: `http://localhost:9200`

## Conclusion

"RAG Real Time" is designed to handle large volumes of data in real-time using advanced technologies. By leveraging Docker, Kafka, Spark, Elasticsearch, and Kibana, we've built a scalable and efficient system for data processing and visualization. Following the preliminary steps and using Docker Compose, you can easily set up and run the system.

---