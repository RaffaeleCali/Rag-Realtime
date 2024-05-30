from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
import findspark


if __name__ == "__main__":
    findspark.init()
# Definisci lo schema per l'oggetto 'articles'
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

    # Configurazione della sessione Spark
    spark = SparkSession.builder \
    .appName("KafkaSpark") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("DEBUG")
    #spark.sparkContext.setLogLevel("ERROR") 



    # Configurazione di Kafka
    kafka_bootstrap_servers = "broker:29092"

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "datipipe") \
        .option("startingOffsets", "latest")\
        .load() 
    
    transformed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("parsed_json")
    )
    
    from pyspark.sql.streaming import StreamingQueryException

    try:
        query = df.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()
        query.awaitTermination()
    except StreamingQueryException as e:
        print("Errore durante lo streaming:", e)
        print("Dettagli dell'errore:", e.desc, "\nCausa dell'errore:", e.stackTrace)

    
    
    print("porco di quello")
        


    # Decodifica e trasformazione dei dati
    #data = df.select(from_json(col("json_string"), schema).alias("parsed_value"))
    #spark.sparkContext.setLogLevel("ERROR") 
    #content = data.select(col("parsed_value.articles.content").alias("content"),
    #                      col("parsed_value.articles.publishedAt").alias("publishedAt"),
    #                      col("parsed_value.articles.title").alias("title"))
    # Estrai il campo 'content' da 'articles'
    #contents = data.select(col("parsed_value.articles.content").alias("content"))

    # Esegui il flusso di dati e salva i contenuti in file di testo
    #query = contents.writeStream \
    #    .outputMode("append") \
    #    .format("text") \
    #    .option("path", "/opt/spark-app/output") \
    #    .option("checkpointLocation", "/tmp/checkpoints") \
    #    .start()

    # Esegui il flusso di dati e mostra i contenuti nella console
    #query = df.writeStream \
    #    .outputMode("append") \
    #    .format("console") \
    #    .start()\
    #    .query.awaitTermination()