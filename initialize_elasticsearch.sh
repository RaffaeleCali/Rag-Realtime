#!/bin/bash

# Aspetta che Elasticsearch sia avviato
until curl -s http://elasticsearch:9200 > /dev/null; do
  echo "Waiting for Elasticsearch to be up..."
  sleep 5
done
#curl -X DELETE "http://elasticsearch:9200/def"

# URL del server Elasticsearch
ES_URL="http://elasticsearch:9200"

# Nome dell'indice
INDEX_NAME="def"

# Configurazione dell'indice
INDEX_CONFIG='{
  "mappings": {
    "properties": {
      "url": { "type": "keyword" },
      "publishedAt": { "type": "date" },
      "description": { "type": "text" },
      "source": {
        "type": "nested",
        "properties": {
          "name": { "type": "keyword" },
          "id": { "type": "keyword" }
        }
      },
      "title": { "type": "text" },
      "urlToImage": { "type": "keyword" },
      "content": { "type": "text" },
      "author": { "type": "keyword" },
      "metadata": { "type": "object" },
      "vector": {
        "type": "dense_vector",
        "dims": 384,
        "index": true,
        "similarity": "cosine"
      }
    }
  }
}'


# Controlla se l'indice esiste già
if curl -s --head --fail "$ES_URL/$INDEX_NAME"; then
  echo "Index $INDEX_NAME already exists."
else
  # Creazione dell'indice
  curl -X PUT "$ES_URL/$INDEX_NAME" -H 'Content-Type: application/json' -d"$INDEX_CONFIG"
  echo "Index $INDEX_NAME created."
fi
