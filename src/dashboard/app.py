import os
from datetime import datetime
import pandas as pd
import streamlit as st
import plotly.express as px

DATA_PATH = os.path.abspath(os.path.join(os.getcwd(), "data", "jobs.csv"))

st.set_page_config(page_title="Data Scientist Jobs in Israel", layout="wide")
st.title("Data Scientist Jobs in Israel")
st.caption("Interactive dashboard of open positions over time")

@st.cache_data(ttl=600)
def load_data(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=["source", "job_title", "company", "department", "location", "url", "collected_at"])
    df = pd.read_csv(path)
    if "collected_at" in df.columns:
        df["collected_at"] = pd.to_datetime(df["collected_at"]).dt.date
    return df


def trigger_fetch():
    # Increase temp max to 125 via env and run scraper
    os.environ["LINKEDIN_MAX_JOBS"] = "125"
    exit_code = os.system("python -m src.job_scraper.runner")
    if exit_code == 0:
        st.success("Fetch completed. Reloading data...")
        st.cache_data.clear()
    else:
        st.error("Fetch failed. Check logs.")


with st.sidebar:
    st.header("Filters")
    if st.button("Fetch more now (+100)"):
        trigger_fetch()

    df = load_data(DATA_PATH)
    sources = sorted(df["source"].dropna().unique().tolist())
    selected_sources = st.multiselect("Source", options=sources, default=sources)

    companies = sorted(df["company"].dropna().unique().tolist())
    selected_companies = st.multiselect("Company", options=companies, default=companies)

    title_filter = st.text_input("Title contains", value="")

filtered = df.copy()
if selected_sources:
    filtered = filtered[filtered["source"].isin(selected_sources)]
if selected_companies:
    filtered = filtered[filtered["company"].isin(selected_companies)]
if title_filter:
    filtered = filtered[filtered["job_title"].str.contains(title_filter, case=False, na=False)]

col1, col2, col3 = st.columns(3)
col1.metric("Total postings", len(filtered))
col2.metric("Unique companies", filtered["company"].nunique())
col3.metric("Snapshots", filtered["collected_at"].nunique())

if not filtered.empty:
    by_day = (
        filtered.groupby("collected_at").size().reset_index(name="count").sort_values("collected_at")
    )
    fig = px.line(by_day, x="collected_at", y="count", markers=True, title="Open positions over time")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Latest snapshot")
    latest_date = filtered["collected_at"].max()
    latest = filtered[filtered["collected_at"] == latest_date].sort_values(["company", "job_title"]).reset_index(drop=True)
    st.dataframe(latest, use_container_width=True, hide_index=True)
else:
    st.info("No data yet. Run the scraper to populate jobs.csv.") 