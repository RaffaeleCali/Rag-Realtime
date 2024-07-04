#!/bin/bash

until curl -s http://elasticsearch:9200 > /dev/null; do
  echo "Waiting for Elasticsearch to be up..."
  sleep 5
done
#curl -X DELETE "http://elasticsearch:9200/def"

ES_URL="http://elasticsearch:9200"

INDEX_NAME="def"


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



if curl -s --head --fail "$ES_URL/$INDEX_NAME"; then
  echo "Index $INDEX_NAME already exists."
else
  curl -X PUT "$ES_URL/$INDEX_NAME" -H 'Content-Type: application/json' -d"$INDEX_CONFIG"
  echo "Index $INDEX_NAME created."
fi
