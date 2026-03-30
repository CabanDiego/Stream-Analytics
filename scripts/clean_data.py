'''Clean incoming data from consumer'''
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Event_Data_Cleaning").getOrCreate()

raw_path = ''
out_path = '/opt/spark-data/cleaned_events.parquet'