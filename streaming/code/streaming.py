from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, create_map, lit, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
from pyspark.conf import SparkConf
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline

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
elastic_index = "tes"

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
    .option("failOnDataLoss", "false") \
    .load()

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .selectExpr("data.timestamp as timestamp", "data.articles.*")

df1 = df.filter(df["content"].isNotNull()) \
        .withColumnRenamed("content", "text")

# Pipeline NLP con BertSentenceEmbeddings
document_assembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

sentence_detector = SentenceDetector() \
    .setInputCols(["document"]) \
    .setOutputCol("sentence")

bert_sentence_embeddings = BertSentenceEmbeddings.pretrained("sent_small_bert_L4_512", "en") \
    .setInputCols(["sentence"]) \
    .setOutputCol("sentence_embeddings") \
    .setCaseSensitive(True) \
    .setMaxSentenceLength(512)

pipeline = Pipeline(stages=[
    document_assembler,
    sentence_detector,
    bert_sentence_embeddings
])

model = pipeline.fit(df1)
result = model.transform(df1)

# Funzione per estrarre il primo elemento degli embeddings
def extract_cls_embedding(embeddings):
    if len(embeddings) > 0:
        return embeddings[0]
    else:
        return []

extract_cls_udf = udf(extract_cls_embedding, ArrayType(FloatType()))

# Creazione delle colonne per testo e vettore di embedding
result_df = result.withColumn("vector", extract_cls_udf(col("sentence_embeddings.embeddings")))

# Creazione del campo metadata come mappa
result_df = result_df.withColumn("metadata", create_map(
    lit("text"), col("text")
))

# Selezione dei campi e preparazione per Elasticsearch
result_df = result_df.select("url", "publishedAt", "description", "source", "title", "urlToImage", "author", "timestamp","text" ,"metadata", "vector")

query = result_df.writeStream \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", elastic_index) \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .start(elastic_index) \
    .awaitTermination()
