from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import types as st
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import create_map, lit
from pyspark.sql.types import StructType, StructField, StringType
import sys 
from elasticsearch import Elasticsearch

import hashlib
def generate_sha256_hash_from_text(text) -> str:
    # Create a SHA256 hash object
    sha256_hash = hashlib.sha256()
    # Update the hash object with the text encoded to bytes
    sha256_hash.update(text.encode('utf-8'))
    # Return the hexadecimal representation of the hash
    return sha256_hash.hexdigest()
spark = SparkSession.builder.appName("kafkatospark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
kafkaServer="broker:9092"
topic = "datapipe"
es_host = "http://elasticsearch:9200"
es_index = "spark-index"
es = Elasticsearch(
    es_host,
)
#from sentence_transformers import SentenceTransformer

#model = SentenceTransformer("nomic-ai/nomic-embed-text-v1", trust_remote_code=True)

class CustomEmbeddingFunction:
    def __init__(self,model ):
        self.model = model

    def __call__(self, input):
        if isinstance(input, list):
            return [self.generate_embeddings(text) for text in input]
        else:
            return [self.generate_embeddings(input)]

    def generate_embeddings(self, text):
        if text:
            embeddings = self.model.encode([text], convert_to_tensor=False)
            return embeddings.tolist()[0]
        else:
            return []

# Creazione dell'istanza della classe di funzione di embedding
#embedding_function = CustomEmbeddingFunction(model)


article_schema = StructType([
        StructField("url", StringType(), True),
        StructField("publishedAt", StringType(), True),
        StructField("description", StringType(), True),
        StructField("source", StructType([
            StructField("name", StringType(), True),
            StructField("id", StringType(), True)
        ]), True),
        StructField("title", StringType(), True),
        StructField("urlToImage", StringType(), True),
        StructField("content", StringType(), True),
        StructField("author", StringType(), True)
    ])

    # Definisci lo schema per l'intero JSON
schema = StructType([
    StructField("@timestamp", StringType(), True),
    StructField("articles", article_schema, True),
    StructField("@version", StringType(), True),
    StructField("status", StringType(), True),
    StructField("totalResults", StringType(), True)
    ])

df = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', kafkaServer) \
        .option('subscribe', topic) \
        .option("startingOffsets", "latest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .selectExpr("data.articles.content")
#.select(from_json(df.json, schema).alias('rowdata')) \
        
def process_batch(batch_df, batch_id):
    for row in batch_df.collect():
        document = row['content']  
        hash = generate_sha256_hash_from_text(document)
        try:
            
            resp = es.index(index=es_index, id=hash, document=document)
            print(resp)
        except Exception as e:
            print(f"\rErrore nell'invio del documento a Elastic: {e}",end = "")




df.writeStream \
    .foreachBatch(process_batch) \
    .start() \
    .awaitTermination()

#df.writeStream \
#.format("console") \
#.start() \
#.awaitTermination()