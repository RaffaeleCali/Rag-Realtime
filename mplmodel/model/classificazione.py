from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, monotonically_increasing_id
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from sparknlp.base import DocumentAssembler, EmbeddingsFinisher
from sparknlp.annotator import Tokenizer, BertEmbeddings, SentenceEmbeddings
import sparknlp
from pyspark.ml import Pipeline
from pyspark.conf import SparkConf
import os

def to_vector_udf(features):
    if features and isinstance(features[0], list):
        flat_list = [item for sublist in features for item in sublist]
        return Vectors.dense([float(x) for x in flat_list])
    else:
        return Vectors.dense([float(x) for x in features])

to_vector = udf(to_vector_udf, VectorUDT())

sparkConf = SparkConf() \
    .set("es.nodes", "elasticsearch") \
    .set("es.port", "9200") \
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.kryoserializer.buffer.max", "2000M")

spark = SparkSession.builder.appName("MLP with Spark NLP").config(conf=sparkConf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sparknlp.start()
elastic_index = "modelmlp"

#dataset
print("Caricamento del dataset...")
df = spark.read.csv("./dataset_k_neighbour.csv", header=True, inferSchema=True)
df = df.drop('df_index', 'lunghezza_testo')

# Filtra solo le categorie valide
valid_categories = ['business', 'tech', 'sport', 'other', 'politics']
df = df.filter(col("Category").isin(valid_categories))

df = df.withColumn("id", monotonically_increasing_id())

print("Utilizzo di un campione casuale del dataset...")
df = df.sample(withReplacement=False, fraction=0.50, seed=42)  # Usa l'10% del dataset

# Indicizzazione delle categorie con StringIndexer
print("Indicizzazione delle categorie...")
stringIndexer = StringIndexer(inputCol="Category", outputCol="label", handleInvalid="keep")
indexer_model = stringIndexer.fit(df)
df_indexed = indexer_model.transform(df)

indexer_model_save_path = "/tmp/string_indexer_model"
os.makedirs(os.path.dirname(indexer_model_save_path), exist_ok=True)
indexer_model.write().overwrite().save(indexer_model_save_path)


print("Controllo delle etichette generate...")
df_indexed.groupBy("label").count().show()
df_indexed.select("Category", "label").distinct().show()

print("Definizione della pipeline di Spark NLP...")
document_assembler = DocumentAssembler().setInputCol("Text").setOutputCol("document")
tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol("token")
bert_model_path = "/tmp/bert_base_cased_model"
if not os.path.exists(bert_model_path):
    print("Scaricamento del modello BERT...")
    bert_embeddings = BertEmbeddings.pretrained("gte_small", "en") \
        .setInputCols(["document", "token"]) \
        .setOutputCol("embeddings")
    bert_embeddings.write().overwrite().save(bert_model_path)
else:
    print("Caricamento del modello BERT da percorso locale...")
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

print("Applicazione della pipeline di Spark NLP...")
df_transformed = pipeline_bert.fit(df_indexed).transform(df_indexed)

print("Preparazione dei dati per Spark MLlib...")
df_ml = df_transformed.withColumn("features", to_vector(col("finished_sentence_embeddings"))).select("id", "features", "label")

print("Controllo delle etichette prima dell'addestramento...")
df_ml.groupBy("label").count().show()

print("Addestramento del modello MLP...")
layers = [384,256,128,64, 5]  # Dimensione embedding, hidden layer, output layer
mlp = MultilayerPerceptronClassifier(maxIter=20, layers=layers, blockSize=128, seed=42)
train, test = df_ml.randomSplit([0.7, 0.3], seed=42)
model = mlp.fit(train)

print("Valutazione del modello...")
result = model.transform(test)
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
accuracy = evaluator.evaluate(result)
print(f"Accuracy: {accuracy}")

print("Salvataggio del modello...")
model_save_path = "/tmp/mod_mlp/spark_mlp_modelv"
os.makedirs(os.path.dirname(model_save_path), exist_ok=True)
model.write().overwrite().save(model_save_path)

result_with_text = result.join(df.select("id", "Text"), on="id")

print("Risultati:")
result_with_text.select("Text", "label", "prediction").show(truncate=False)
