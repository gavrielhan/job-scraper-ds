import os
from datetime import datetime
import pandas as pd
import streamlit as st
import plotly.express as px

API_URL = st.secrets.get("API_URL") or os.getenv("API_URL", "")
DATA_PATH = os.path.abspath(os.path.join(os.getcwd(), "data", "jobs.csv"))
ENRICHED_PATH = os.path.abspath(os.path.join(os.getcwd(), "data", "jobs_enriched.csv"))
# Optional: override with a remote CSV for Streamlit Cloud (e.g., raw GitHub)
DEFAULT_REMOTE_CSV = "https://raw.githubusercontent.com/gavrielhan/job-scraper-ds/main/data/jobs.csv"
REMOTE_CSV = os.environ.get("DASHBOARD_DATA_URL", DEFAULT_REMOTE_CSV)
# Only show local fetch button if explicitly enabled
ENABLE_FETCH = os.environ.get("ENABLE_FETCH_BUTTON", "").strip().lower() in {"1", "true", "yes", "on"}

st.set_page_config(page_title="Data Scientist Jobs in Israel", layout="wide")
st.title("Data Scientist Jobs in Israel")
st.caption("Interactive dashboard of open positions over time")

@st.cache_data(ttl=600)
def load_data(path: str, remote_url: str, enriched_path: str) -> pd.DataFrame:
    # Try local file first
    if os.path.exists(path):
        df = pd.read_csv(path)
    else:
        # Fallback to remote CSV (raw GitHub)
        try:
            df = pd.read_csv(remote_url)
        except Exception:
            return pd.DataFrame(columns=["source", "job_title", "company", "location", "url", "collected_at"])
    if "collected_at" in df.columns:
        df["collected_at"] = pd.to_datetime(df["collected_at"]).dt.date
    # Ensure expected columns exist
    for c in ["source", "job_title", "company", "location", "url", "collected_at"]:
        if c not in df.columns:
            df[c] = None
    # If enriched file exists, merge in normalized columns by URL
    if os.path.exists(enriched_path):
        try:
            df_en = pd.read_csv(enriched_path, usecols=["url", "city_normalized", "title_normalized"]).drop_duplicates("url")
            df = df.merge(df_en, on="url", how="left")
        except Exception:
            pass
    return df[["source", "job_title", "company", "location", "url", "collected_at", "city_normalized", "title_normalized"] if "city_normalized" in df.columns else ["source", "job_title", "company", "location", "url", "collected_at"]]


def trigger_fetch():
    if not API_URL:
        st.error("API_URL not set in Streamlit secrets or env")
        return None
    # no params — backend will use its defaults/config
    return requests.post(API_URL, json={}, timeout=25)


def normalize_city(loc: str) -> str:
    if not isinstance(loc, str) or not loc.strip():
        return "Tel Aviv-Yafo"
    t = " ".join(loc.replace("\n", " ").split()).strip(", ")
    tl = t.lower()
    # If generic country
    if tl == "israel":
        return "Tel Aviv-Yafo"
    # Strip trailing country
    if tl.endswith(", israel"):
        t = t[:-8]
        tl = t.lower()
    # Tel Aviv variants
    tel_variants = ["tel aviv-yafo", "tel-aviv-yafo", "tel aviv yafo", "tel aviv", "tel-aviv"]
    if any(v in tl for v in tel_variants):
        return "Tel Aviv-Yafo"
    mapping = {
        "jerusalem": "Jerusalem",
        "haifa": "Haifa",
        "herzliya": "Herzliya",
        "ra'anana": "Ra'anana",
        "beer sheva": "Beer Sheva",
        "be'er sheva": "Beer Sheva",
        "netanya": "Netanya",
        "ashdod": "Ashdod",
        "ashkelon": "Ashkelon",
        "rishon": "Rishon LeZion",
        "petah tikva": "Petah Tikva",
    }
    for k, v in mapping.items():
        if k in tl:
            return v
    # Default to original (without country)
    return t


with st.sidebar:
    st.header("Filters")
    if ENABLE_FETCH:
        if st.button("Fetch more now", type="primary"):
            with st.spinner("Starting fetch…"):
                resp = trigger_fetch()
            if resp is not None:
                if resp.ok:
                    st.success("Fetch started ✅. Check S3 soon.")
                else:
                    st.error(f"Failed: {resp.status_code}")
                st.code(resp.text, language="json")
    else:
        st.warning("Set API_URL in Streamlit secrets to enable fetching.")


    df = load_data(DATA_PATH, REMOTE_CSV, ENRICHED_PATH)
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

# Distribution charts (locations % and titles pie)
if not filtered.empty:
    dist_col1, dist_col2 = st.columns(2)

    # Location percentages (prefer normalized city)
    if "city_normalized" in filtered.columns:
        city_series = filtered["city_normalized"].fillna("")
        # For empty normalized entries, fall back to normalized-from-location
        city_series = city_series.mask(city_series.eq("") | city_series.isna(), filtered["location"].fillna("").map(normalize_city))
    else:
        city_series = filtered["location"].fillna("").map(normalize_city)
    loc_counts = city_series.value_counts(dropna=False).reset_index()
    loc_counts.columns = ["city", "count"]
    loc_counts["percent"] = (loc_counts["count"] / loc_counts["count"].sum() * 100).round(1)
    loc_counts["percent_label"] = loc_counts["percent"].astype(str) + "%"
    fig_loc = px.bar(loc_counts, x="city", y="percent", text="percent_label", title="Locations (% of filtered)")
    fig_loc.update_yaxes(title="Percent", range=[0, 100])
    fig_loc.update_layout(xaxis_title="Location", yaxis_ticksuffix="%", uniformtext_minsize=10, uniformtext_mode="hide")
    dist_col1.plotly_chart(fig_loc, use_container_width=True)

    # Titles pie (prefer normalized title)
    if "title_normalized" in filtered.columns:
        title_series = filtered["title_normalized"].fillna("")
        title_series = title_series.mask(title_series.eq("") | title_series.isna(), filtered["job_title"].fillna("Unknown"))
    else:
        title_series = filtered["job_title"].fillna("Unknown")
    title_counts = title_series.value_counts().reset_index()
    title_counts.columns = ["job_title", "count"]
    top_n = 12
    if len(title_counts) > top_n:
        top = title_counts.iloc[:top_n].copy()
        other = pd.DataFrame([["Other", title_counts.iloc[top_n:]["count"].sum()]], columns=["job_title", "count"])
        title_counts = pd.concat([top, other], ignore_index=True)
    fig_titles = px.pie(title_counts, names="job_title", values="count", title="Titles distribution (filtered)")
    dist_col2.plotly_chart(fig_titles, use_container_width=True)

# Trend over time
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
    st.info("No data yet. Ensure data/jobs.csv exists in the repo or set DASHBOARD_DATA_URL to a CSV.") 