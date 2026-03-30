'''Analyze incoming cleaned data'''
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession.builder.appName("Event_Analysis").getOrCreate()

INPUT_PATH = '/opt/spark-data/cleaned_events.parquet'
OUT_PATH = 'opt/spark-data/events_summary.parquet'