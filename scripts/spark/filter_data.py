from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path

BASEPATH = Path(__file__).parent.parent.parent
RAWDATA = BASEPATH / "data" / "raw_data.json"

spark = SparkSession.builder\
    .appName("project_transofmations")\
    .getOrCreate()


df = spark.read.json(str(RAWDATA))

#Separating different topics to different df
transactions_df = df.filter(col("Topic") == "transaction_events")

user_events_df = df.filter(col("Topic") == "user_events")

print(user_events_df.count())

spark.stop()