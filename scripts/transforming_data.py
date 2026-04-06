'''

Spark Module to filter ingested JSON data from Kafka

'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, regexp_extract
from pathlib import Path
import os


#Generating path to the raw data file
landing_path = Path("/opt/spark-data/landing")
output_path = Path("/opt/spark-data/gold")
os.makedirs(output_path, exist_ok=True)

spark = SparkSession.builder\
    .appName("project_transofmations")\
    .getOrCreate()

#Verifying to see if there are any json files in landing area
json_files = list(landing_path.glob("*.json"))
if not json_files:
    print(f"No JSON files found in {landing_path}, exiting...")
    spark.stop()
    exit(0)

#Find the last modified file and get its name
latest_file = max(landing_path.glob("*.json"), key=lambda f: f.stat().st_mtime)
landing_name = latest_file.stem
    
    
#Reading found files and making sure they arent empty
df = spark.read.option("multiLine", True)\
    .option("mode", "DROPMALFORMED")\
    .json(str(latest_file))
    
df.cache()

if df.rdd.isEmpty():
    print("No data found")
    spark.stop()
    exit(0)
    

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


# ========== Snowflake Schema Creation for Gold Layer ========

dim_users = cleaned_uevents_df.select("user_id", "browser", "device").dropDuplicates()

dim_products = cleaned_transactions_df.select("product_name").dropDuplicates()

dim_country = cleaned_transactions_df.select("country").dropDuplicates()

fact_transactions = cleaned_transactions_df.select(
    "user_id",
    "product_name",
    "country",
    "total",
    "transaction_type",
    "timestamp"
)

fact_user_events = cleaned_uevents_df.select(
   "user_id",
    "event_type",
    "browser",
    "device",
    "session_id"
)

# ========== Saving Gold Layer ========
fact_transactions.write \
    .mode("append") \
    .parquet(f"{output_path}/fact_transactions/{landing_name}_transformed")

fact_user_events.write \
    .mode("append") \
    .parquet(f"{output_path}/fact_user_events/{landing_name}_transformed")


dim_users.write.mode("append").parquet(f"{output_path}/dim_users/{landing_name}_transformed")
dim_products.write.mode("append").parquet(f"{output_path}/dim_products/{landing_name}_transformed")
dim_country.write.mode("append").parquet(f"{output_path}/dim_country/{landing_name}_transformed")

print("Saved Gold Layer data")

spark.stop()