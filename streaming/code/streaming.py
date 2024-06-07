from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, arrays_zip
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
from pyspark.conf import SparkConf
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *

# Spark Configuration
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

# Kafka Configuration
kafkaServer = "broker:9092"
topic = "datapipe"
elastic_index = "spark-index"

# Define the schema for Kafka messages
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

print("Reading stream from kafka...")
# Read the stream from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Cast the message received from Kafka with the provided schema
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .selectExpr("data.timestamp as timestamp", "data.articles.*")

# Filter for non-null content
df1 = df.filter(df["content"].isNotNull())

# Define Spark NLP pipeline
document_assembler = DocumentAssembler() \
    .setInputCol("content") \
    .setOutputCol("document")

tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")

# Using a smaller pre-trained model instead of BERT
small_bert_embeddings = BertEmbeddings.pretrained('small_bert_L2_128', 'en') \
    .setInputCols(["document", "token"]) \
    .setOutputCol("embeddings")

pipeline = Pipeline(stages=[
    document_assembler,
    tokenizer,
    small_bert_embeddings
])

# Apply the pipeline
model = pipeline.fit(df1)
result = model.transform(df1)

# Print the schema to verify the presence of the embeddings field
result.printSchema()

# Extract and structure embeddings
result_df = result.select("url", "publishedAt", "description", "source", "title", "urlToImage", "content", "author", "timestamp", explode(result.embeddings).alias("embedding_struct")) \
    .select("url", "publishedAt", "description", "source", "title", "urlToImage", "content", "author", "timestamp", col("embedding_struct.embeddings").alias("embedding"))

# Write the resulting DataFrame to Elasticsearch
query = result_df.writeStream \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .format("es") \
    .start(elastic_index) \
    .awaitTermination()
