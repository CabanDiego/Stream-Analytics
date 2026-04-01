'''

Spark Module to filter ingested JSON data from Kafka

'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pathlib import Path
import time
import os


#Generating path to the raw data file
landing_path = Path("./data/landing") 
output_path = Path("./data/transformed_data")
os.makedirs(output_path, exist_ok=True)

json_files = sorted(landing_path.glob("*.json"))
if not json_files:
    print(f"No batch files found in {landing_path}. Exiting.")
    exit(0)
latest_file = json_files[-1]

spark = SparkSession.builder\
    .appName("project_transofmations")\
    .getOrCreate()


df = spark.read.json(str(latest_file), multiLine=True)

# ========== Transaction Events ========

#Separating transaction events from ingested data
transactions_df = df.filter(col("Topic") == "transaction_events")

#Flattening and filtering wanted Transaction data for analyzing
struct_transactions_df = transactions_df.select(
    col("Data.products.product_name").cast("string").alias("product_name"),
    col("Data.billing_address.country").alias("country"),
    col("Data.total").alias("total"),
    col("Data.user_id").alias("user_id"),
    col("Data.timestamp").alias("timestamp")
)

#Adding new column for event (purchase if total > 0, otherwise return)
final_transactions_df = struct_transactions_df\
    .withColumn("event", when(struct_transactions_df.total > 0, "purchase")\
                .otherwise("return"))


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

# final_user_events_df.createOrReplaceTempView("u_events")

# sql_results = spark.sql("""
#     SELECT * FROM u_events
#                         LIMIT 20
# """).show(20)


# ========== Joins ========

left_join_df = final_transactions_df.join(final_user_events_df,
                                     "user_id", "left")

#final_df = ""

timestamp_str = time.strftime("%Y%m%d_%H%M%S")
left_join_df.write.csv(
    f"{output_path}/user_events_{timestamp_str}.csv",
    header=True,
    mode="overwrite"
)
print("Saved transformed data")

spark.stop()