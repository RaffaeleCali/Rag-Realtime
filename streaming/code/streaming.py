
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import types as st
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import sys 

import hashlib
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_elasticsearch import ElasticsearchStore
import elasticsearch
from langchain_community.embeddings import HuggingFaceEmbeddings
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch
from pyspark.conf import SparkConf

def generate_sha256_hash_from_text(text) -> str:
    sha256_hash = hashlib.sha256()
    sha256_hash.update(text.encode('utf-8'))
    return sha256_hash.hexdigest()



def process_batch(batch_df, batch_id):
    for row in batch_df.collect():
        document = row['content'] 
        if document is None:
            print("non cè ninete")
            return
        splits = text_splitter.split_text(document)
        
        for split in splits:
            hash = generate_sha256_hash_from_text(split)
            # Controlla l'esistenza di ciascun hash/documento
            prova = es_cli.exists(index=es_index, id=hash)
            print(prova)
            if not prova:
                try:
                    ids = [hash]
                    texts = [split]
                    resp = es.add_texts(index=es_index, ids=ids, texts=texts)
                    print(resp)
                except Exception as e:
                    print(f"\rErrore nell'invio del documento a Elastic: {e}", end="")
            else:
                print("Documento già presente")



text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)


sparkConf = SparkConf().set("es.nodes", "elasticsearch") \
                        .set("es.port", "9200")

spark = SparkSession.builder.appName("kafkatospark").config(conf=sparkConf).getOrCreate()



kafkaServer="broker:9092"
topic = "datapipe"
            

es_index = "spark-index"

text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
spark.sparkContext.setLogLevel("ERROR")


#es_cli = Elasticsearch("http://elasticsearch:9200")

#es = ElasticsearchStore(
#    index_name= es_index,
#    es_url="http://elasticsearch:9200",
#    embedding= HuggingFaceEmbeddings(
#        model_name="all-MiniLM-L6-v2", model_kwargs={"device": "cpu"}
#    ),
    #distance_strategy="COSINE",
#)


#strategy=ElasticsearchStore.ApproxRetrievalStrategy(
#        hybrid=True,
#    )



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
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) 

#dfall = df.selectExpr   #("data.articles.content")

df.writeStream \
    .option("checkpointLocation", "/tmp/") \
    .format("es") \
    .start("prova_tutti") \
    .awaitTermination()




#df.writeStream \
#.format("console") \
#.start() \
#.awaitTermination()