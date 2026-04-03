'''

Take gold layer data, analyze it, and display it using streamlit

'''
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import glob


st.set_page_config(page_title="Transactions Analysis",layout="wide")
st.title("Transacion Analytics")

tevents_PATH = Path("/opt/spark-data/gold")
#Extracting the lastest gold payer parquet file for analysis

gold_files = sorted(glob.glob(str(tevents_PATH)))
if not gold_files:
    print(f"No files found in {tevents_PATH}.")
    exit(0)
    
latest_file = gold_files[-1]

#Load data and refresh cache 60 seconds
@st.cache_data(ttl=60)
def load_data():
    print(str(latest_file))
    return pd.read_parquet(str(latest_file))

try:
    df = load_data()
except Exception as e:
    st.warning("Waiting for Data")

