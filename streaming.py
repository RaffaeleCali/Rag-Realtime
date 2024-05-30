from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import StreamingQueryException
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import *
import requests
import json

sc = SparkContext(appName="PythonStructuredStreamsKafka")
spark = SparkSession(sc)

sc.setLogLevel("ERROR")

kafkaServer="broker:9092"
topic = "datapipe"


import time 
time.sleep(100)
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

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafkaServer) \
  .option("subscribe", topic) \
  .load() \
  .selectExpr('CAST(value AS STRING) as json') \
  .as[String]

#df = df.selectExpr('CAST(value AS STRING) as json')

df.select(from_json(df.json, schema).alias('rowdata')) \
  .select('rowdata.articles.description') \
  .writeStream \
  .trigger(once=True) \
  .format("console") \
  .start() \
  .awaitTermination()
