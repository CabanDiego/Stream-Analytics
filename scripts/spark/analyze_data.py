'''

Analyze Filtered Dataframes for visualization using streamlit

'''
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession.builder\
    .appName("Event_Analysis")\
    .getOrCreate()
