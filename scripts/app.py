'''Take analyzed data and display it using streamlit'''
import streamlit as st
import pandas as pd
import plotly.express as px

SUMMARY_PATH = ''

st.set_page_config(page_title="Transactions Information",layout="wide")
st.title("Ream TIme Transacion Analytics")

@st.cache_data(ttl=60)
def load_data():
    return pd.read_parquet(SUMMARY_PATH)

try:
    df = load_data()
except Exception as e:
    st.warning("Waiting for Data")

