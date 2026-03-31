'''

Spark Module to filter ingested JSON data from Kafka

'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path

#Generating path to the raw data file
BASEPATH = Path(__file__).parent.parent.parent
RAWDATA = BASEPATH / "data" / "raw_data.json"

spark = SparkSession.builder\
    .appName("project_transofmations")\
    .getOrCreate()


df = spark.read.json(str(RAWDATA))

#Separating different topics to different df
transactions_df = df.filter(col("Topic") == "transaction_events")

user_events_df = df.filter(col("Topic") == "user_events")



spark.stop()