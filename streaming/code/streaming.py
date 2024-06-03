from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import hashlib
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_elasticsearch import ElasticsearchStore
from langchain_community.embeddings import HuggingFaceEmbeddings
from elasticsearch import Elasticsearch

def generate_sha256_hash_from_text(text) -> str:
    sha256_hash = hashlib.sha256()
    sha256_hash.update(text.encode('utf-8'))
    return sha256_hash.hexdigest()

def process_batch(batch_df, batch_id):
    for row in batch_df.collect():
        document = row['content']
        if document is None:
            print("non c'è niente")
            continue
        splits = text_splitter.split_text(document)
        
        for split in splits:
            hash = generate_sha256_hash_from_text(split)
            prova = es_cli.exists(index=es_index, id=hash)
            if not prova:
                try:
                    ids = [hash]
                    texts = [split]
                    resp = es.add_texts(index=es_index, ids=ids, texts=texts)
                    print(f"Document added with id: {hash}")
                except Exception as e:
                    print(f"Errore nell'invio del documento a Elastic: {e}")
            else:
                print("Documento già presente")

# Initialize text splitter
text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)

# Initialize Spark session
spark = SparkSession.builder.appName("kafkatospark").getOrCreate()

kafkaServer = "broker:9092"
topic = "datapipe"

es_index = "spark-index"
es_cli = Elasticsearch("http://elasticsearch:9200")

es = ElasticsearchStore(
    index_name=es_index,
    es_url="http://elasticsearch:9200",
    embedding=HuggingFaceEmbeddings(
        model_name="all-MiniLM-L6-v2", model_kwargs={"device": "cpu"}
    ),
)

# Define the schema for the articles
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

# Define the schema for the entire JSON
schema = StructType([
    StructField("@timestamp", StringType(), True),
    StructField("articles", article_schema, True),
    StructField("@version", StringType(), True),
    StructField("status", StringType(), True),
    StructField("totalResults", StringType(), True)
])

# Read data from Kafka
df = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', kafkaServer) \
    .option('subscribe', topic) \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.articles.content")

# Add logging to check the content being processed
df.writeStream \
    .foreachBatch(lambda df, epoch_id: df.show(truncate=False)) \
    .start() \
    .awaitTermination()

df.writeStream \
    .foreachBatch(process_batch) \
    .start() \
    .awaitTermination()
