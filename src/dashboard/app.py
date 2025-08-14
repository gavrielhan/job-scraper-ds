import os
from datetime import datetime, timezone
import pandas as pd
import streamlit as st
import plotly.express as px
import requests

from sentence_transformers import SentenceTransformer, util
from typing import List
import re
import json
import boto3
from zoneinfo import ZoneInfo
try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    def st_autorefresh(*args, **kwargs):
        return None
import time


API_URL = st.secrets.get("API_URL") or os.getenv("API_URL", "")
DATA_PATH = os.path.abspath(os.path.join(os.getcwd(), "data", "jobs.csv"))
ENRICHED_PATH = os.path.abspath(os.path.join(os.getcwd(), "data", "jobs_enriched.csv"))
# Optional: override with a remote CSV for Streamlit Cloud (e.g., raw GitHub)
DEFAULT_REMOTE_CSV = "https://raw.githubusercontent.com/gavrielhan/job-scraper-ds/main/data/jobs.csv"
REMOTE_CSV = os.environ.get("DASHBOARD_DATA_URL", DEFAULT_REMOTE_CSV)
# Only show local fetch button if explicitly enabled
ENABLE_FETCH = os.environ.get("ENABLE_FETCH_BUTTON", "").strip().lower() in {"1", "true", "yes", "on"}
# Optional: read data from S3 directly (private bucket) if enabled
USE_S3 = os.environ.get("USE_S3", "").strip().lower() in {"1", "true", "yes", "on"}
# Prefer OUTPUT_* for consistency with runner/Lambda, then fall back to legacy S3_* secrets
S3_BUCKET = (
    os.getenv("OUTPUT_BUCKET")
    or st.secrets.get("OUTPUT_BUCKET")
    or st.secrets.get("S3_BUCKET", "")
)
S3_PREFIX = (
    os.getenv("OUTPUT_PREFIX")
    or st.secrets.get("OUTPUT_PREFIX")
    or st.secrets.get("S3_PREFIX", "snapshots/")
)
# Countdown config (read next_run.json from S3) — prefer OUTPUT_* then secrets fallbacks
AWS_REGION = os.getenv("AWS_REGION", st.secrets.get("AWS_DEFAULT_REGION", "us-east-1"))
S3_META_BUCKET = (
    os.getenv("OUTPUT_BUCKET")
    or st.secrets.get("OUTPUT_BUCKET")
    or st.secrets.get("S3_BUCKET")
    or S3_BUCKET
    or "job-scraper-ds"
)
S3_META_PREFIX = (
    os.getenv("OUTPUT_PREFIX")
    or st.secrets.get("OUTPUT_PREFIX")
    or st.secrets.get("S3_PREFIX")
    or S3_PREFIX
    or "snapshots"
)
S3_META_PREFIX = S3_META_PREFIX.strip("/")
SCHEDULE_HRS = int(os.getenv("SCHEDULE_HOURS", st.secrets.get("SCHEDULE_HOURS", 12)))
_s3_client = boto3.client("s3", region_name=AWS_REGION)
NEXT_RUN_REFRESH_SECS = int(os.getenv("NEXT_RUN_REFRESH_SECS", st.secrets.get("NEXT_RUN_REFRESH_SECS", 36000)))  # 10h default
DATA_REFRESH_SECS = int(os.getenv("DATA_REFRESH_SECS", 300))  # force refresh every N seconds

def fetch_next_run_from_s3():
    key = f"{S3_META_PREFIX}/meta/next_run.json"
    try:
        obj = _s3_client.get_object(Bucket=S3_META_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
        return datetime.fromisoformat(data.get("next_run_at"))
    except Exception:
        return None

def get_next_run_cached():
    now_utc = datetime.now(timezone.utc)
    last_checked = st.session_state.get("_next_run_last_checked")
    cached = st.session_state.get("_next_run_at")
    should_refresh = (
        cached is None
        or last_checked is None
        or (now_utc - last_checked).total_seconds() >= NEXT_RUN_REFRESH_SECS
    )
    if should_refresh:
        value = fetch_next_run_from_s3()
        # Normalize to aware UTC
        if value is not None and value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        st.session_state["_next_run_at"] = value
        st.session_state["_next_run_last_checked"] = now_utc
        return value
    return cached

# Optional: perform in-memory enrichment instead of relying on jobs_enriched.csv
SELF_ENRICH = os.environ.get("SELF_ENRICH", "").strip().lower() in {"1", "true", "yes", "on"}
SELF_ENRICH_MODE = os.environ.get("SELF_ENRICH_MODE", "embed").strip().lower()  # "embed" or "flan"
ENRICH_THRESHOLD = float(os.environ.get("ENRICH_THRESHOLD", "0.55"))
HF_SENTENCE_MODEL = os.environ.get("HF_SENTENCE_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
HF_T2T_MODEL = os.environ.get("HF_T2T_MODEL", "google/flan-t5-small")
# Canonical label sets used for normalization
CITY_CANON = [
    "Tel Aviv-Yafo",
    "Jerusalem",
    "Haifa",
    "Herzliya",
    "Ramat Gan",
    "Ra'anana",
    "Beer Sheva",
    "Netanya",
    "Ashdod",
    "Ashkelon",
    "Rishon LeZion",
    "Petah Tikva",
    "Other",
]
TITLE_CANON = [
    "Data Scientist",
    "Machine Learning Engineer",
    "AI Engineer",
    "Data Analyst",
    "Data Engineer",
    "Data Architect",
    "Research Scientist",
    "Data Science Manager",
    "Bioinformatics Scientist",
    "Other",
]

st.set_page_config(page_title="Data Scientist Jobs in Israel", layout="wide")
st.title("Data Scientist Jobs in Israel")
st.caption("Interactive dashboard of open positions over time")

@st.cache_data(ttl=0)
def load_data(path: str, remote_url: str, enriched_path: str, s3_version_token: str, local_mtime_token: str, time_bucket_token: str) -> pd.DataFrame:
    # If configured, try S3 first
    if USE_S3 and S3_BUCKET:
        try:
            import boto3
            from botocore.config import Config
            s3 = boto3.client(
                "s3",
                aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
                region_name=st.secrets.get("AWS_DEFAULT_REGION", "us-east-1"),
                config=Config(retries={"max_attempts": 5, "mode": "standard"}),
            )
            # Prefer stable keys; otherwise newest *.csv
            prefix = S3_PREFIX if S3_PREFIX.endswith("/") else (S3_PREFIX + "/")
            stable_candidates = [
                f"{prefix}archive.csv",
                f"{prefix}latest.csv",
                f"{prefix}jobs_latest.csv",
                f"{prefix}latest/jobs.csv",
            ]
            df = None
            for latest_key in stable_candidates:
                try:
                    obj = s3.get_object(Bucket=S3_BUCKET, Key=latest_key)
                    df = pd.read_csv(obj["Body"])
                    break
                except Exception:
                    df = None
            if not df.empty:
                # proceed to post-processing (col parsing, enrichment merge)
                pass
            else:
                # fall through to local/remote
                raise RuntimeError("No CSV objects found in S3")
        except Exception:
            # S3 disabled or failed — fallback to local/remote
            df = None
    else:
        df = None

    if df is None:
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
    # Ensure expected columns exist (add snapshot_id if missing)
    for c in ["source", "job_title", "company", "location", "url", "collected_at"]:
        if c not in df.columns:
            df[c] = None
    if "snapshot_id" not in df.columns:
        df["snapshot_id"] = None
    # If enriched file exists, merge in normalized columns by URL
    if os.path.exists(enriched_path):
        try:
            df_en = pd.read_csv(enriched_path, usecols=["url", "city_normalized", "title_normalized"]).drop_duplicates("url")
            df = df.merge(df_en, on="url", how="left")
        except Exception:
            pass
    base_cols = ["source", "job_title", "company", "location", "url", "collected_at"]
    if "snapshot_id" in df.columns:
        base_cols.append("snapshot_id")
    if "city_normalized" in df.columns:
        base_cols.extend(["city_normalized", "title_normalized"])
    return df[base_cols]


def get_data_version_tokens() -> tuple[str, str, str]:
    # S3 version token: ETag+LastModified of first available stable key
    s3_token = ""
    if USE_S3 and S3_BUCKET:
        try:
            s3c = boto3.client("s3", region_name=AWS_REGION)
            prefix = S3_PREFIX if S3_PREFIX.endswith("/") else (S3_PREFIX + "/")
            for key in [f"{prefix}archive.csv", f"{prefix}latest.csv", f"{prefix}jobs_latest.csv", f"{prefix}latest/jobs.csv"]:
                try:
                    head = s3c.head_object(Bucket=S3_BUCKET, Key=key)
                    s3_token = f"{key}:{head.get('ETag')}:{head.get('LastModified')}"
                    break
                except Exception:
                    continue
        except Exception:
            pass
    # Local mtime token
    try:
        local_token = str(os.path.getmtime(DATA_PATH))
    except Exception:
        local_token = ""
    # Time bucket token to force refresh every N seconds
    time_token = str(int(max(1, DATA_REFRESH_SECS) and time.time() // max(1, DATA_REFRESH_SECS)))
    return s3_token, local_token, time_token


# Cached model loader so the embedding model is initialized once
@st.cache_resource(show_spinner=False)
def get_embed_model(model_name: str) -> SentenceTransformer:
    return SentenceTransformer(model_name)


@st.cache_data(ttl=3600, show_spinner=False)
def canonicalize_titles_cached(titles: List[str], model_name: str, threshold: float) -> List[str]:
    # Deduplicate while preserving order
    unique: List[str] = []
    seen = set()
    for t in titles:
        t0 = (t or "").strip()
        if t0 not in seen:
            seen.add(t0)
            unique.append(t0)
    # Heuristic first
    mapping = {u: (classify_title_heuristic(u) or "") for u in unique}
    missing = [u for u in unique if not mapping[u]]
    if missing:
        try:
            model = get_embed_model(model_name)
            emb_canon = model.encode(TITLE_CANON, convert_to_tensor=True, normalize_embeddings=True)
            emb = model.encode(missing, convert_to_tensor=True, normalize_embeddings=True)
            from sentence_transformers import util as st_util
            sims = st_util.cos_sim(emb, emb_canon)
            for i, u in enumerate(missing):
                idx = int(sims[i].argmax())
                score = float(sims[i][idx])
                mapping[u] = TITLE_CANON[idx] if score >= threshold else "Other"
        except Exception:
            for u in missing:
                mapping[u] = "Other"
    # Build result list preserving original order
    return [mapping.get((t or "").strip(), "Other") for t in titles]


def normalize_strings_embed(values: List[str], canon_list: List[str], model: SentenceTransformer, threshold: float) -> List[str]:
    emb_canon = model.encode(canon_list, convert_to_tensor=True, normalize_embeddings=True)
    out: List[str] = []
    for v in values:
        txt = (v or "").strip()
        if not txt:
            out.append("")
            continue
        emb = model.encode([txt], convert_to_tensor=True, normalize_embeddings=True)
        sim = util.cos_sim(emb, emb_canon)[0]
        idx = int(sim.argmax())
        score = float(sim[idx])
        out.append(canon_list[idx] if score >= threshold else txt)
    return out


def normalize_strings_flan(values: List[str], canon_list: List[str], gen) -> List[str]:
    labels = ", ".join(canon_list)
    out: List[str] = []
    instruction = (
        "Classify the job title into EXACTLY ONE of these labels: [" + labels + "]. "
        "Rules: If the title contains 'research data scientist' choose 'Data Scientist'. "
        "If it contains 'data scientist' choose 'Data Scientist'. "
        "If it contains 'machine learning engineer' or 'ml engineer' choose 'Machine Learning Engineer'. "
        "If it contains 'ai engineer' choose 'AI Engineer'. "
        "If it contains 'data analyst' choose 'Data Analyst'. "
        "If it contains 'data engineer' or 'analytics engineer' choose 'Data Engineer'. "
        "If it contains 'architect' choose 'Data Architect'. "
        "If it contains 'research scientist' (without 'data') choose 'Research Scientist'. "
        "If it contains 'manager', 'lead', 'head' and the role is in data/ai/ml, choose 'Data Science Manager'. "
        "If the role is unrelated to data/ai/ml (e.g., developer advocate, growth, marketing, acquisition, sales, product manager), choose 'Other'. "
        "Output ONLY the label text."
    )
    examples = [
        ("Research Data Scientist, Waze Personalized Experience", "Data Scientist"),
        ("Senior Machine Learning Engineer (GenAI)", "Machine Learning Engineer"),
        ("AI Engineer", "AI Engineer"),
        ("Applied Data Scientist", "Data Scientist"),
        ("Data Engineer II - GenAI", "Data Engineer"),
        ("Head of Data Science", "Data Science Manager"),
        ("Developer Advocate, GenAI", "Other"),
        ("User Acquisition Team Lead - Paid Social", "Other"),
        ("Product AI Lab Team Lead", "Data Science Manager"),
    ]
    shots = "\n".join([f"Title: {t}\nLabel: {y}" for t, y in examples])
    for v in values:
        txt = (v or "").strip()
        if not txt:
            out.append("")
            continue
        prompt = instruction + "\n" + shots + "\nTitle: " + txt + "\nLabel:"
        try:
            resp = gen(prompt, max_new_tokens=8)
            pred = (resp[0]["generated_text"] or "").strip()
            out.append(pred if pred in canon_list else "Other")
        except Exception:
            out.append("Other")
    return out


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
    if tl in {"israel", "il"}:
        return "Tel Aviv-Yafo"
    # Strip trailing country tokens like ', Israel' or ', IL'
    t = re.sub(r"\s*,\s*(israel|il)\s*$", "", t, flags=re.IGNORECASE)
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
        "bnei brak": "Bnei Brak",
        "benei brak": "Bnei Brak",
        "bene brak": "Bnei Brak",
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


def classify_title_heuristic(title: str) -> str:
    if not isinstance(title, str):
        return ""
    t_raw = title.strip()
    t = t_raw.lower()
    if not t:
        return ""
    # Normalize common noise
    t = re.sub(r"\s+\(.*?\)$", "", t)
    # Helper matchers
    def has(*words: str) -> bool:
        return all(w in t for w in words)
    def any_re(patterns: list[str]) -> bool:
        return any(re.search(p, t) for p in patterns)

    # Leadership / management (prioritize before DS catch-alls)
    if any_re([r"\b(head|lead|manager|director|vp)\b"]) and any_re([r"\bdata\b|\bai\b|\bml\b|\banalytic"]):
        return "Data Science Manager"

    # Data Scientist family
    if (
        "data scientist" in t
        or "applied data scientist" in t
        or "machine learning scientist" in t
        or "research data scientist" in t
        or any_re([r"\bapplied scientist\b", r"\bquant\w* scientist\b", r"\bcomput(ational|er) scientist\b"])  # applied/quant/computational
    ):
        return "Data Scientist"

    # Machine Learning Engineer family (incl. deep learning / CV / NLP / MLOps / research engineer)
    if (
        any_re([
            r"\bml\b[^a-zA-Z]*eng", r"machine learning engineer", r"ml engineer", r"ml software engineer",
            r"deep learning engineer", r"computer vision( engineer|)\b|\bcv engineer\b", r"nlp engineer",
            r"ai/ml engineer", r"gen(erative)? ai engineer", r"genai engineer", r"ml developer|ai/ml developer",
            r"mlops\b|ml ops|ml platform|ml infrastructure", r"research engineer\b"
        ])
    ):
        return "Machine Learning Engineer"

    # AI Engineer (general AI that is not clearly ML Eng)
    if any_re([r"\bai engineer\b", r"\bgen(erative)? ai\b", r"\bai specialist\b", r"\bai developer\b"]) and "ml" not in t:
        return "AI Engineer"

    # Data Engineer family
    if any_re([r"\bdata engineer\b", r"\banalytics engineer\b", r"etl engineer\b", r"data platform\b", r"data infra"]):
        return "Data Engineer"

    # Data Analyst family
    if any_re([r"\bdata analyst\b", r"business analyst\b", r"product analyst\b", r"analytics? analyst\b"]):
        return "Data Analyst"

    # Architect
    if "architect" in t and any_re([r"\bdata\b", r"\bai\b", r"\bml\b", r"analytics"]):
        return "Data Architect"

    # Research Scientist
    if "research scientist" in t:
        return "Research Scientist"

    # Default: try lightweight fallbacks
    if "scientist" in t:
        return "Data Scientist"
    if any_re([r"\bml\b", r"machine learning\b"]) and "engineer" in t:
        return "Machine Learning Engineer"
    if "analyst" in t:
        return "Data Analyst"

    return ""


with st.sidebar:
    st.subheader("Next scheduled fetch")
    next_dt = get_next_run_cached()
    if not next_dt:
        st.caption("No schedule found yet. It will appear after the first scheduled run writes metadata.")
    else:
        tz = ZoneInfo("Asia/Jerusalem")
        target = next_dt.astimezone(tz) if next_dt.tzinfo else next_dt.replace(tzinfo=timezone.utc).astimezone(tz)
        now = datetime.now(timezone.utc).astimezone(tz)
        remaining = (target - now).total_seconds()
        # Smooth seconds: refresh every second while the timer is displayed
        st_autorefresh(interval=1000, key="next-fetch-ticker")
        if remaining <= 0:
            st.success(f"Next fetch is due now (scheduled for {target:%Y-%m-%d %H:%M:%S %Z}) 🚀")
            # Force next rerun to fetch fresh schedule from S3
            st.session_state["_next_run_last_checked"] = datetime(1970,1,1,tzinfo=timezone.utc)
        else:
            h = int(remaining // 3600)
            m = int((remaining % 3600) // 60)
            s = int(remaining % 60)
            st.metric("Time to next fetch", f"{h:02d}:{m:02d}:{s:02d}", help=f"Scheduled at {target:%Y-%m-%d %H:%M:%S %Z}")
            total = max(1, SCHEDULE_HRS * 3600)
            elapsed = total - int(remaining)
            st.progress(min(1.0, elapsed / total))

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
    elif ENABLE_FETCH and not API_URL:
        st.warning("Set API_URL in Streamlit secrets to enable fetching.")


    s3_tok, local_tok, time_tok = get_data_version_tokens()
    df = load_data(DATA_PATH, REMOTE_CSV, ENRICHED_PATH, s3_tok, local_tok, time_tok)
    # If no enriched columns present and SELF_ENRICH is enabled, compute in-memory
    if SELF_ENRICH and ("city_normalized" not in df.columns or "title_normalized" not in df.columns):
        with st.spinner("Enriching locations and titles in-memory…"):
            loc_values = df.get("location", pd.Series([""] * len(df))).fillna("").astype(str).tolist()
            title_values = df.get("job_title", pd.Series([""] * len(df))).fillna("").astype(str).tolist()
            if SELF_ENRICH_MODE == "flan":
                try:
                    gen = get_t2t_pipeline(HF_T2T_MODEL)
                    city_llm = normalize_strings_flan(loc_values, CITY_CANON, gen)
                    title_llm = normalize_strings_flan(title_values, TITLE_CANON, gen)
                    df["city_normalized"] = [normalize_city(c if c else lv) for c, lv in zip(city_llm, loc_values)]
                    # Apply heuristic as a final pass to collapse verbose variants
                    df["title_normalized"] = [classify_title_heuristic(t) or t for t in title_llm]
                except Exception as e:
                    st.warning(f"LLM enrich failed ({e}); falling back to embeddings")
                    model = get_embed_model(HF_SENTENCE_MODEL)
                    city_embed = normalize_strings_embed(loc_values, CITY_CANON, model, ENRICH_THRESHOLD)
                    df["city_normalized"] = [normalize_city(c if c else lv) for c, lv in zip(city_embed, loc_values)]
                    heurs = [classify_title_heuristic(t) for t in title_values]
                    embed_titles = normalize_strings_embed(title_values, TITLE_CANON, model, ENRICH_THRESHOLD)
                    df["title_normalized"] = [h or e or tv for h, e, tv in zip(heurs, embed_titles, title_values)]
            else:
                model = get_embed_model(HF_SENTENCE_MODEL)
                city_embed = normalize_strings_embed(loc_values, CITY_CANON, model, ENRICH_THRESHOLD)
                df["city_normalized"] = [normalize_city(c if c else lv) for c, lv in zip(city_embed, loc_values)]
                heurs = [classify_title_heuristic(t) for t in title_values]
                embed_titles = normalize_strings_embed(title_values, TITLE_CANON, model, ENRICH_THRESHOLD)
                df["title_normalized"] = [h or e or tv for h, e, tv in zip(heurs, embed_titles, title_values)]
        mode_label = "FLAN-T5" if SELF_ENRICH_MODE == "flan" else "embeddings"
        st.caption(f"Using self-enrichment ({mode_label}) for normalized city/title (no CSV saved)")
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
# Snapshot count: unique snapshot_id (non-empty) + unique collected_at for rows missing snapshot_id
if "snapshot_id" in filtered.columns:
    sid_series = filtered["snapshot_id"].astype(str).str.strip()
    mask_has_sid = sid_series.ne("") & ~sid_series.isna()
    count_sid = sid_series[mask_has_sid].nunique()
    count_fallback = filtered.loc[~mask_has_sid, "collected_at"].nunique() if "collected_at" in filtered.columns else 0
    snapshots_count = int(count_sid + count_fallback)
else:
    snapshots_count = filtered["collected_at"].nunique()
col3.metric("Snapshots", snapshots_count)

# Distribution charts (locations % and titles pie)
if not filtered.empty:
    dist_col1, dist_col2 = st.columns(2)

    # Location percentages (always normalize with normalize_city)
    base_city = filtered["city_normalized"] if "city_normalized" in filtered.columns else filtered["location"]
    city_series = base_city.fillna("").map(normalize_city)
    loc_counts = city_series.value_counts(dropna=False).reset_index()
    loc_counts.columns = ["city", "count"]
    loc_counts["percent"] = (loc_counts["count"] / loc_counts["count"].sum() * 100).round(1)
    loc_counts["percent_label"] = loc_counts["percent"].astype(str) + "%"
    fig_loc = px.bar(loc_counts, x="city", y="percent", text="percent_label", title="Locations (% of filtered)")
    fig_loc.update_yaxes(title="Percent", range=[0, 100])
    fig_loc.update_layout(xaxis_title="Location", yaxis_ticksuffix="%", uniformtext_minsize=10, uniformtext_mode="hide")
    dist_col1.plotly_chart(fig_loc, use_container_width=True)

    # Titles pie (apply canonical mapping so categories are stable) — batch + cached
    raw_titles = filtered["job_title"].fillna("").astype(str).tolist()
    title_series = pd.Series(canonicalize_titles_cached(raw_titles, HF_SENTENCE_MODEL, ENRICH_THRESHOLD))
    title_counts = title_series.value_counts().reset_index()
    title_counts.columns = ["job_title", "count"]
    top_n = 12
    if len(title_counts) > top_n:
        top = title_counts.iloc[:top_n].copy()
        other = pd.DataFrame([["Other", title_counts.iloc[top_n:]["count"].sum()]], columns=["job_title", "count"])
        title_counts = pd.concat([top, other], ignore_index=True)
    fig_titles = px.pie(title_counts, names="job_title", values="count", title="Titles distribution (filtered)")
    dist_col2.plotly_chart(fig_titles, use_container_width=True)

# Trend over time — group by snapshot_id when present, else collected_at
if not filtered.empty:
    if "snapshot_id" in filtered.columns:
        sid = pd.to_datetime(filtered["snapshot_id"], format="%Y%m%dT%H%M%SZ", errors="coerce")
    else:
        sid = pd.to_datetime(pd.Series([pd.NaT] * len(filtered)))
    cat = pd.to_datetime(filtered["collected_at"], errors="coerce")
    key = sid.fillna(cat)
    trend = pd.DataFrame({"snapshot_ts": key})
    by_snap = (
        trend.groupby("snapshot_ts").size().reset_index(name="count").dropna(subset=["snapshot_ts"]).sort_values("snapshot_ts")
    )
    fig = px.line(by_snap, x="snapshot_ts", y="count", markers=True, title="Open positions over time")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("All postings (newest first)")
    all_posts = (
        filtered.sort_values(["collected_at", "company", "job_title"], ascending=[False, True, True])
        .reset_index(drop=True)
    )
    # Hide normalized helper columns in the table view
    hide_cols = ["title_normalized", "city_normalized"]
    visible_cols = [c for c in all_posts.columns if c not in hide_cols]
    st.dataframe(all_posts[visible_cols], use_container_width=True, hide_index=True)
else:
    st.info("No data yet. Ensure data/jobs.csv exists in the repo or set DASHBOARD_DATA_URL to a CSV.") 