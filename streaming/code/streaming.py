from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_json, col, create_map, lit
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.conf import SparkConf
#import sparknlp
from sparknlp.base import DocumentAssembler, Pipeline, EmbeddingsFinisher
from sparknlp.annotator import Tokenizer, BertEmbeddings, SentenceEmbeddings, SentenceDetector, YakeKeywordExtraction
from pyspark.ml import Pipeline
from pyspark.ml.classification import MultilayerPerceptronClassificationModel
from pyspark.ml.feature import StringIndexerModel
from pyspark.ml.linalg import Vectors, VectorUDT
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
#    .set("spark.network.timeout", "800s") \
#    .set("spark.executor.heartbeatInterval", "200s") \
#    .set("spark.rpc.message.maxSize", "512") \

spark = SparkSession.builder.appName("kafkatospark").config(conf=sparkConf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

kafkaServer = "broker:9092"
topic = "datapipe"
elastic_index = "def"

# Schema per i dati in arrivo
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

# Lettura dal topic
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

# ml_pipe_1: estazione embeddings e classificazione con mlp usando gli embeddings 
document_assembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

tokenizer = Tokenizer() \
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

#nella posizione 0 risiede l'embeddings della frase (cls)
result_bert = result_bert.withColumn("sentence_embeddings", col("finished_sentence_embeddings")[0])

# Caricamento MLP (allenato in mlpmodel)
mlp_model_path = "spark_mlp_modelv/"
mlp_model = MultilayerPerceptronClassificationModel.load(mlp_model_path)

#  StringIndexer per convertire da numerico a categoria testuale (es 2.0 = sport)
indexer_model_path = "string_indexer_model"
indexer_model = StringIndexerModel.load(indexer_model_path)

# preprocessing per inferenza mlp 
def to_vector_udf(features):
    if features and isinstance(features[0], list):
        flat_list = [item for sublist in features for item in sublist]
        return Vectors.dense([float(x) for x in flat_list])
    else:
        return Vectors.dense([float(x) for x in features])

to_vector = udf(to_vector_udf, VectorUDT())

df_for_prediction = result_bert.withColumn("features", to_vector(col("finished_sentence_embeddings"))).select("text", "features")

# inferenza mlp
predictions = mlp_model.transform(df_for_prediction)


predictions = predictions.withColumnRenamed("prediction", "classpredict")

# rimappatura delle classi numeriche alle categorie testuali
labels = indexer_model.labels
class_mapping = {float(i): label for i, label in enumerate(labels)}

def map_class_to_category(class_index):
    return class_mapping.get(class_index, 'unknown')

map_class_to_category_udf = udf(map_class_to_category, StringType())
predictions = predictions.withColumn("category", map_class_to_category_udf(col("classpredict")))

#  join con text
final_df_with_predictions = result_bert.join(predictions.select("text", "category"), "text")

# ml_pipe2 con YakeKeywordExtraction estrazione parole chiavi
sentenceDetector = SentenceDetector() \
    .setInputCols(["document"]) \
    .setOutputCol("sentence")

token = Tokenizer() \
    .setInputCols(["sentence"]) \
    .setOutputCol("token") \
    .setContextChars(["(", ")", "?", "!", ".", ","])

keywords = YakeKeywordExtraction() \
    .setInputCols(["token"]) \
    .setNKeywords(6) \
    .setOutputCol("keywords")\
    

yake_pipeline = Pipeline(stages=[
    document_assembler,
    sentenceDetector,
    token,
    keywords
])

yake_model = yake_pipeline.fit(df1)
result_yake = yake_model.transform(df1)

# in questo modo vengono selezionate solo le parole uniche nel testo (yake ritorna altre tre colonne ... da capire)
result_yake = result_yake.withColumn('unique_keywords', F.array_distinct("keywords.result"))
result_yake = result_yake.select("text", 'unique_keywords')

# Combinazione dei risultati delle due pipeline
final_df = final_df_with_predictions.join(result_yake, "text")


final_df = final_df.withColumnRenamed("sentence_embeddings", "vector")

# Creazione della colonna metadata serve per longchain dopo (è obbligatoria non so il perche) 
final_df = final_df.withColumn("metadata", create_map(
    lit("text"), col("text")
))


final_df = final_df.dropDuplicates(["text", "vector"])
final_df = final_df.withColumn("page_content", col("text"))

final_df = final_df.select(
    "url", "publishedAt", "description", "source", "kafka_timestamp", "page_content", "title", "urlToImage", "author", "timestamp", "text", "vector", "unique_keywords", "metadata", "category"
)

# Scrittura su Es
query = final_df.writeStream \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", elastic_index) \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .start(elastic_index) \
    .awaitTermination()
