from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import from_json, col, create_map, lit
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.conf import SparkConf
import sparknlp
from sparknlp.base import DocumentAssembler, Pipeline, EmbeddingsFinisher
from sparknlp.annotator import Tokenizer, BertEmbeddings, SentenceEmbeddings, SentenceDetector, YakeKeywordExtraction
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

# Schema per gli articoli
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

df = df.selectExpr("CAST(value AS STRING)", "timestamp AS kafka_timestamp") \
    .select(from_json("value", schema).alias("data"), "kafka_timestamp") \
    .selectExpr("data.timestamp as timestamp", "data.articles.*", "kafka_timestamp")

# Filtrare e rinominare le colonne necessarie
df1 = df.filter(df["content"].isNotNull()) \
        .withColumnRenamed("content", "text")

# Configurazione della prima pipeline con BertEmbeddings
document_assembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")

bert_model_path = "/tmp/bert_base_cased_model"
import os
if not os.path.exists(bert_model_path):
    bert_embeddings = BertEmbeddings.pretrained("gte_small", "en") \
        .setInputCols(["document", "token"]) \
        .setOutputCol("embeddings")
    bert_embeddings.write().overwrite().save(bert_model_path)
else:
    bert_embeddings = BertEmbeddings.load(bert_model_path)

sentence_embeddings = SentenceEmbeddings() \
    .setInputCols(["document", "embeddings"]) \
    .setOutputCol("sentence_embeddings")

embeddings_finisher = EmbeddingsFinisher() \
    .setInputCols(["sentence_embeddings"]) \
    .setOutputCols(["finished_sentence_embeddings"])

pipeline_bert = Pipeline(stages=[
    document_assembler,
    tokenizer,
    bert_embeddings,
    sentence_embeddings,
    embeddings_finisher
])

model_bert = pipeline_bert.fit(df1)
result_bert = model_bert.transform(df1)

# Estrazione delle colonne necessarie
result_bert = result_bert.withColumn("sentence_embeddings", col("finished_sentence_embeddings")[0])

# Configurazione della seconda pipeline con YakeKeywordExtraction
sentenceDetector = SentenceDetector() \
    .setInputCols(["document"]) \
    .setOutputCol("sentence")

token = Tokenizer() \
    .setInputCols(["sentence"]) \
    .setOutputCol("token") \
    .setContextChars(["(", ")", "?", "!", ".", ","])

keywords = YakeKeywordExtraction() \
    .setInputCols(["token"]) \
    .setOutputCol("keywords") 
 #   .setNKeywords(8)

yake_pipeline = Pipeline(stages=[
    document_assembler,
    sentenceDetector,
    token,
    keywords
])

yake_model = yake_pipeline.fit(df1)
result_yake = yake_model.transform(df1)

result_yake = result_yake.withColumn('unique_keywords', F.array_distinct("keywords.result"))

result_yake = result_yake.select("text", 'unique_keywords')

# Combinazione dei risultati delle due pipeline
final_df = result_bert.join(result_yake, "text")

# Rinomina della colonna `sentence_embeddings` in `vector`
final_df = final_df.withColumnRenamed("sentence_embeddings", "vector")

# Creazione della colonna `metadata`
final_df = final_df.withColumn("metadata", create_map(
    lit("text"), col("text")
))

# Filtrare duplicati
final_df = final_df.dropDuplicates(["text", "vector"])
final_df = final_df.withColumn("page_content", col("text"))

# Selezione delle colonne finali
final_df = final_df.select(
    "url", "publishedAt", "description", "source", "kafka_timestamp","page_content" ,"title", "urlToImage", "author", "timestamp", "text", "vector", "unique_keywords", "metadata"
)

# Scrittura su Elasticsearch
query = final_df.writeStream \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", elastic_index) \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .start(elastic_index) \
    .awaitTermination()
