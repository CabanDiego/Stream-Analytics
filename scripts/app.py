import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="Transaction Analytics", layout="wide")
st.title("Real-Time Transaction Analytics")

#Paths to gold layer parquet data
GOLD_TRANSACTIONS_PATH = Path("data/gold/fact_transactions")
GOLD_USER_EVENTS_PATH = Path("data/gold/fact_user_events")

#Load transactions data
@st.cache_data(ttl=10)
def load_transactions():
    #Verifying to see if path exists
    if not GOLD_TRANSACTIONS_PATH.exists():
        return pd.DataFrame()

    #Retrieving list of parquet files inside the path, if none exist return empty dataframe
    parquet_files = list(GOLD_TRANSACTIONS_PATH.rglob("*.parquet"))
    if not parquet_files:
        return pd.DataFrame()

    #Storing each dataframe from each file and using concat to unify them
    frames = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        frames.append(df)

    return pd.concat(frames, ignore_index=True)



#Load user events data
@st.cache_data(ttl=10)
def load_user_events():
    #Verifying to see if path exists
    if not GOLD_USER_EVENTS_PATH.exists():
        return pd.DataFrame()

    #Retrieving list of parquet files inside the path, if none exist return empty dataframe
    parquet_files = list(GOLD_USER_EVENTS_PATH.rglob("*.parquet"))
    if not parquet_files:
        return pd.DataFrame()

    #Storing each dataframe from each file and using concat to unify them
    frames = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        frames.append(df)

    return pd.concat(frames, ignore_index=True)


try:
    #Load both datasets
    transactions_df = load_transactions()
    user_events_df = load_user_events()

    #Holder in case there is no data
    if transactions_df.empty and user_events_df.empty:
        st.warning("Waiting for Airflow to generate the first gold data...")
        st.stop()

    #Transaction Events
    if not transactions_df.empty:
        st.subheader("Transaction Events Analysis")
        col1, col2, col3, col4 = st.columns(4)

        total_sales = transactions_df["total"].sum()
        top_user = transactions_df.groupby("user_id")["total"].sum().sort_values(ascending=False).reset_index()
        top_product = transactions_df.groupby("product_name").size().reset_index()
        top_country = transactions_df.groupby("country").size().sort_values(ascending=False).reset_index()

        col1.metric("Total Sales", f"${total_sales:,.2f}")
        col2.metric("Top Spender", top_user.iloc[0]["user_id"], f"Total Spent ${top_user.iloc[0]['total']:.2f}")
        col3.metric("Top Product", top_product.iloc[0]["product_name"], f"{top_product.iloc[0][0]} bought")
        col4.metric("Top Buying Country", top_country.iloc[0]["country"], f"{top_country.iloc[0][0]} purchases")

        purchased_df = transactions_df[transactions_df["transaction_type"] == "purchase"]

        product_sales = purchased_df.groupby("product_name")["total"].sum().sort_values(ascending=False).reset_index()
        fig = px.bar(
            product_sales.head(10),
            x="product_name",
            y="total",
            title="Top 10 Products by Revenue",
            color="total"
        )
        st.plotly_chart(fig, use_container_width=True)

        bot_product_sales = purchased_df.groupby("product_name")["total"].sum().sort_values(ascending=True).reset_index()
        fig2 = px.bar(
            bot_product_sales.head(10),
            x="product_name",
            y="total",
            title="10 Lowest Revenue Products",
            color="total"
        )
        st.plotly_chart(fig2, use_container_width=True)

    #User Events
    if not user_events_df.empty:
        st.subheader("User Events Analysis")
        col1, col2, col3 = st.columns(3)

        most_used_browser = user_events_df.groupby("browser").size().sort_values(ascending=False).reset_index()
        top_event_user = user_events_df.groupby("user_id").size().sort_values(ascending=False).reset_index()
        top_event_type = user_events_df["event_type"].value_counts().reset_index()
        top_event_type.columns = ["event_type", "count"]


        col1.metric("Most Used Browser", most_used_browser.iloc[0]["browser"], f"{most_used_browser.iloc[0][0]} times used")
        col2.metric("Most Active User", top_event_user.iloc[0]["user_id"], f"{top_event_user.iloc[0][0]} events")
        col3.metric("Top Event", top_event_type.iloc[0]["event_type"], f"{top_event_type.iloc[0]['count']} events")

        #Plot top event types
        fig_events = px.bar(
            top_event_type.head(10),
            x="event_type",
            y="count",
            title="Top 10 Event Types",
            color="count"
        )
        st.plotly_chart(fig_events, use_container_width=True)

except Exception as e:
    st.warning("Waiting for Airflow to generate the first gold data...")