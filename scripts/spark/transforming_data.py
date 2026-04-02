'''

Spark Module to filter ingested JSON data from Kafka

'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path
import os


#Generating path to the raw data file and output path of gold layer
landing_path = Path("/opt/spark-data/landing") 
output_path = Path("/opt/spark-data/gold")

os.makedirs(output_path, exist_ok=True)

#Extracting the lastest json file to transform latest batch
json_files = sorted(landing_path.glob("*.json"))
if not json_files:
    print(f"No files found in {landing_path}.")
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
    col("Data.transaction_id").alias("transaction_id"),
    col("Data.user_id").alias("user_id"),
    col("Data.transaction_type").alias("transaction_type"),
    col("Data.products.product_name").cast("string").alias("product_name"),
    col("Data.billing_address.country").alias("country"),
    col("Data.payment_method").alias("payment_method"),
    col("Data.currency").alias("currency"),
    col("Data.total").alias("total"),
    col("Data.timestamp").alias("timestamp")
    
)

#Dropping any rows with any null/nan values
cleaned_transactions_df = struct_transactions_df.dropna(how='any')


# ========== User Events ========

#Separating user events from ingested data
user_events_df = df.filter(col("Topic") == "user_events")

#Flattening and filtering wanted user event data for analyzing
struct_user_events_df = user_events_df.select(
    col("Data.user_id").alias("user_id"),
    col("Data.event_type").alias("event_type"),
    col("Data.browser").alias("browser"),
    col("Data.device").alias("device"),
    col("Data.session_id").alias("session_id")
)


#Dropping any rows with any null/nan values
cleaned_uevents_df = struct_user_events_df.dropna(how='any')


# ========== Saving Data ========


#Saving each df to a seperate file inside a folder named after batch name
file_name = os.path.basename(latest_file).split(".")[0]

cleaned_transactions_df.write.parquet(
    f"{output_path}/batch_{file_name}/transactions.parquet")

cleaned_uevents_df.write.parquet(
    f"{output_path}/batch_{file_name}/user_events.parquet")
print(f"Saved transformed data for batch {file_name}")

spark.stop()