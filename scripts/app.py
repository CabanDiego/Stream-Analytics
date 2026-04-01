'''

Take analyzed data and display it using streamlit

'''
import streamlit as st
import pandas as pd
import plotly.express as px
import time
from pathlib import Path

BASEPATH = Path(__file__).parent.parent
SUMMARY_PATH = BASEPATH / "data" / "transformed_data" / str(time.time)

st.set_page_config(page_title="Transactions Analysis",layout="wide")
st.title("Transacion Analytics")

#Load data and refresh cache 60 seconds
@st.cache_data(ttl=60)
def load_data():
    return pd.read_parquet(str(SUMMARY_PATH))

try:
    df = load_data()
except Exception as e:
    st.warning("Waiting for Data")

