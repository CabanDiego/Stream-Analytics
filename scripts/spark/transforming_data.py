'''

Spark Module to filter ingested JSON data from Kafka

'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pathlib import Path
import time
import os

os.makedirs("./data", exist_ok=True)
file_path = os.path.join("./data")

#Generating path to the raw data file
BASEPATH = Path(__file__).parent.parent.parent
RAWDATA = BASEPATH / "data" / "1775062577.4987094.json"

spark = SparkSession.builder\
    .appName("project_transofmations")\
    .getOrCreate()


df = spark.read.json(str(RAWDATA))

# ========== Transaction Events ========

#Separating transaction events from ingested data
# transactions_df = df.filter(col("Topic") == "transaction_events")

# #Flattening and filtering wanted Transaction data for analyzing
# struct_transactions_df = transactions_df.select(
#     col("Data.products.product_name").cast("string").alias("product_name"),
#     col("Data.billing_address.country").alias("country"),
#     col("Data.total").alias("total"),
#     col("Data.user_id").alias("user_id"),
#     col("Data.timestamp").alias("timestamp")
# )

# #Adding new column for event (purchase if total > 0, otherwise return)
# final_transactions_df = struct_transactions_df\
#     .withColumn("event", when(struct_transactions_df.total > 0, "purchase")\
#                 .otherwise("return"))


# ========== User Events ========

#Separating user events from ingested data
user_events_df = df.filter(col("Topic") == "user_events")

user_events_df.printSchema()

#Flattening and filtering wanted user event data for analyzing
final_user_events_df = user_events_df.select(
    col("Data.user_id").alias("user_id"),
    col("Data.browser").alias("browser"),
    col("Data.device").alias("device"),
    col("Data.session_id").alias("session_id")
)

final_user_events_df.createOrReplaceTempView("u_events")

sql_results = spark.sql("""
    SELECT * FROM u_events
                        LIMIT 20
""").show(20)


# ========== Joins ========

# left_join = final_transactions_df.join(final_user_events_df,
#                                      "user_id", "left").show()

#final_df = ""

#final_df.write.csv(f"{file_path}/transformed_data/{time.time}_transactions.csv", header=True)
#print("Saved transformed data")

spark.stop()