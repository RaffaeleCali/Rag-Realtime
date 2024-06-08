from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, lit, create_map
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.conf import SparkConf
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
import os

# Configurazione Spark
sparkConf = SparkConf() \
    .set("es.nodes", "elasticsearch") \
    .set("es.port", "9200") \
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.kryoserializer.buffer.max", "2000M") \
    .set("spark.driver.memory", "10g") \
    .set("spark.executor.memory", "10g") \
    .set("spark.driver.maxResultSize", "4g") \
    .set("spark.network.timeout", "800s") \
    .set("spark.executor.heartbeatInterval", "200s") \
    .set("spark.rpc.message.maxSize", "512")

spark = SparkSession.builder.appName("kafkatospark").config(conf=sparkConf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Kafka e ElasticSearch Config
kafkaServer = "broker:9092"
topic = "datapipe"
elastic_index = "test5"

# Schema
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

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("articles", article_schema, True),
    StructField("@version", StringType(), True),
    StructField("status", StringType(), True),
    StructField("totalResults", StringType(), True)
])

# Lettura dal topic Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .selectExpr("data.timestamp as timestamp", "data.articles.*")

df1 = df.filter(df["content"].isNotNull()) \
        .withColumnRenamed("content", "text")

# Pipeline NLP
document_assembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

bert_tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")

bert_model_path = "/tmp/bert_base_cased_model"

if not os.path.exists(bert_model_path):
    bert_embeddings = BertEmbeddings.pretrained("gte_small", "en") \
        .setInputCols(["document", "token"]) \
        .setOutputCol("embeddings")
    bert_embeddings.write().overwrite().save(bert_model_path)
else:
    bert_embeddings = BertEmbeddings.load(bert_model_path)

pipeline = Pipeline(stages=[
    document_assembler,
    bert_tokenizer,
    bert_embeddings
])

model = pipeline.fit(df1)
result = model.transform(df1)

result.printSchema()

# Selezione dei campi e preparazione per Elasticsearch
result_df = result.select("url", "publishedAt", "description", "source", "title", "urlToImage", "text", "author", "timestamp", explode(result.embeddings).alias("embedding_struct")) \
    .select("url", "publishedAt", "description", "source", "title", "urlToImage", "text", "author", "timestamp", col("embedding_struct.embeddings").alias("vector")) \
    .withColumn("metadata", create_map(lit("key"), lit("value")))

query = result_df.writeStream \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", elastic_index) \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .start(elastic_index) \
    .awaitTermination()
