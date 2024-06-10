from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from pyspark.sql.functions import from_json, col, create_map, lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
from pyspark.conf import SparkConf
import sparknlp
#from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.base import DocumentAssembler, Pipeline, EmbeddingsFinisher
from sparknlp.annotator import Tokenizer, BertEmbeddings, SentenceEmbeddings 
from sparknlp.pretrained import PretrainedPipeline
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


df1 = df.filter(df["content"].isNotNull()) \
        .withColumnRenamed("content", "text")

# Configurazione del Pipeline con BertEmbeddings usando il modello gte_small

# Configurazione degli annotatori
document_assembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")

# Sostituisci "bert_base_cased" con "gte_small" se disponibile
bert_embeddings = BertEmbeddings.pretrained("gte_small", "en") \
    .setInputCols(["document", "token"]) \
    .setOutputCol("embeddings")

sentence_embeddings = SentenceEmbeddings() \
      .setInputCols(["document", "embeddings"]) \
      .setOutputCol("sentence_embeddings")

embeddings_finisher = EmbeddingsFinisher() \
      .setInputCols(["sentence_embeddings"]) \
      .setOutputCols(["finished_sentence_embeddings"])

# Creazione del pipeline
pipeline = Pipeline(stages=[
    document_assembler,
    tokenizer,
    bert_embeddings,
    sentence_embeddings,
    embeddings_finisher
])


model = pipeline.fit(df1)
result = model.transform(df1)

# Creazione delle colonne per testo e vettore di embedding
#result_df = result.withColumn("vector", col("embeddings.embeddings"))
# Estrarre frasi e embeddings associati
result_df = result.select("kafka_timestamp", F.explode(F.arrays_zip(result.sentence_embeddings.result, 
       result.sentence_embeddings.embeddings)).alias("cols")) \
                  .select(
                      col("kafka_timestamp"),
                      F.expr("cols['0']").alias("sentence"),
                      F.expr("cols['1']").alias("sentence_embeddings")
                  )
result_df = result_df.withColumn("metadata", create_map(
    lit("text"), col("sentence"),  # Qui usi 'sentence' invece di 'text' se vuoi la frase esatta
))
result_df = result_df.withColumn("vector", col("sentence_embeddings"))
#result_df = result_df.withColumn("sente", col("sentence"))
# Selezione dei campi e preparazione per Elasticsearch
#result_df = result_df.select(
#    "url", "publishedAt", "description", "source", "title", "urlToImage", "author", "timestamp", "text", "metadata", "vector"
#)
result_df = result_df.join(df1, "kafka_timestamp")
result_df = result_df.select(
    "url", "publishedAt", "description", "source", "kafka_timestamp","title", "urlToImage", "author", "timestamp", "text", "metadata", "vector"
)
query = result_df.writeStream \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", elastic_index) \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .start(elastic_index) \
    .awaitTermination()
