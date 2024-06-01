
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import types as st
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import sys 
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer
import hashlib
from langchain_text_splitters import RecursiveCharacterTextSplitter
from elasticsearch.helpers import bulk
from langchain_elasticsearch import ElasticsearchStore
import elasticsearch
from langchain_community.embeddings import HuggingFaceEmbeddings

def generate_sha256_hash_from_text(text) -> str:
    sha256_hash = hashlib.sha256()
    sha256_hash.update(text.encode('utf-8'))
    return sha256_hash.hexdigest()


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
            #if not es.exists(index=es_index, id=hash):
            try:
                ids = [hash]
                texts = [split]
                resp = es.add_texts(index=es_index, ids=ids,texts=texts )
                print(resp)
            except Exception as e:
                print(f"\rErrore nell'invio del documento a Elastic: {e}",end = "")

#            else: 
#                print("gia presente")



#model = SentenceTransformer("nomic-ai/nomic-embed-text-v1", trust_remote_code=True)

text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)

#embedding_function = CustomEmbeddingFunction(model)

#model = SentenceTransformer("all-MiniLM-L6-v2")

spark = SparkSession.builder.appName("kafkatospark").getOrCreate()

kafkaServer="broker:9092"
topic = "datapipe"
            

es_index = "spark-index"
#embedding_function = CustomEmbeddingFunction(model)
text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
spark.sparkContext.setLogLevel("ERROR")
#es_host = Elasticsearch(es_host="http://elasticsearch:9200",index_name= es_index)

es = ElasticsearchStore(
    index_name= es_index,
    es_url="http://elasticsearch:9200",
    embedding= HuggingFaceEmbeddings(
        model_name="all-MiniLM-L6-v2", model_kwargs={"device": "cpu"}
    ),
)




# Creazione dell'istanza della classe di funzione di embedding

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
        




df.writeStream \
    .foreachBatch(process_batch) \
    .start() \
    .awaitTermination()

#df.writeStream \
#.format("console") \
#.start() \
#.awaitTermination()