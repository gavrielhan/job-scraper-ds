from __future__ import annotations
import argparse
from datetime import date, datetime
from typing import List
import os
import io
import pandas as pd
import boto3
from botocore.exceptions import ClientError

from .config import AppConfig, ensure_dirs, load_sources_config
from .models import JobPosting
from .storage import append_postings_to_csv, COLUMNS
from .scrapers import (
    GreenhouseScraper,
    LeverScraper,
    SerpapiLinkedInScraper,
    LinkedInPlaywrightScraper,
    SearchApiLinkedInScraper,
)


def append_to_s3_archive(df_run: pd.DataFrame) -> None:
    bucket = os.getenv("OUTPUT_BUCKET")
    prefix = os.getenv("OUTPUT_PREFIX", "snapshots/")
    if not bucket:
        return
    # Normalize prefix
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"
    key = f"{prefix}archive.csv"

    s3 = boto3.client("s3")
    # Load existing aggregate if present
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        df_prev = pd.read_csv(io.BytesIO(obj["Body"].read()))  # type: ignore[arg-type]
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            df_prev = pd.DataFrame(columns=df_run.columns)
        else:
            raise

    # Append and deduplicate on URL (keep latest row)
    df_all = pd.concat([df_prev, df_run], ignore_index=True)
    if "url" in df_all.columns:
        df_all.drop_duplicates(subset=["url"], keep="last", inplace=True)
    else:
        df_all.drop_duplicates(keep="last", inplace=True)

    # Write back
    buf = io.StringIO()
    df_all.to_csv(buf, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(), ContentType="text/csv", CacheControl="no-cache")
    print(f"[s3] appended+dedup to s3://{bucket}/{key} rows={len(df_all)}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run job scrapers and append to CSV")
    parser.add_argument("--as-of", dest="as_of", type=str, default=None, help="ISO date to stamp collection (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--sources", dest="sources_config", type=str, default=None, help="Path to sources.yaml. Defaults to config/sources.yaml")
    return parser.parse_args()


def _parse_date(s: str | None) -> date:
    if not s:
        return date.today()
    return datetime.fromisoformat(s).date()


def run_once(as_of: date, cfg: AppConfig) -> int:
    sources_cfg = load_sources_config(cfg)

    all_postings: List[JobPosting] = []
    # Unique snapshot id per run (UTC timestamp)
    snapshot_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    # LinkedIn via SerpAPI (optional)
    linkedin_cfg = (sources_cfg.get("linkedin_serpapi") or {})
    if linkedin_cfg.get("enabled"):
        scraper = SerpapiLinkedInScraper(
            api_key=cfg.serpapi_api_key,
            query=linkedin_cfg.get("query", "Data Scientist"),
            location=linkedin_cfg.get("location", "Israel"),
        )
        all_postings.extend(scraper.fetch(as_of=as_of))

    # LinkedIn via SearchApi.io (optional)
    searchapi_cfg = (sources_cfg.get("searchapi_linkedin") or {})
    if searchapi_cfg.get("enabled"):
        sa_scraper = SearchApiLinkedInScraper(
            api_key=os.getenv("SEARCHAPI_API_KEY"),
            query=searchapi_cfg.get("query", "Data Scientist"),
            location=searchapi_cfg.get("location", "Israel"),
        )
        all_postings.extend(sa_scraper.fetch(as_of=as_of))

    # LinkedIn via Playwright (optional, requires credentials)
    li_pw_cfg = (sources_cfg.get("linkedin_playwright") or {})
    if li_pw_cfg.get("enabled"):
        headless = str(li_pw_cfg.get("headless", os.getenv("LINKEDIN_HEADLESS", "true"))).lower() == "true"
        max_jobs = int(li_pw_cfg.get("max_jobs", os.getenv("LINKEDIN_MAX_JOBS", 60)))
        li_pw = LinkedInPlaywrightScraper(
            query=li_pw_cfg.get("query", "Data Scientist"),
            location=li_pw_cfg.get("location", "Israel"),
            headless=headless,
            max_jobs=max_jobs,
        )
        all_postings.extend(li_pw.fetch(as_of=as_of))

    # Greenhouse
    greenhouse_cfg = (sources_cfg.get("greenhouse") or {})
    if greenhouse_cfg.get("enabled"):
        boards = greenhouse_cfg.get("companies") or []
        gh_scraper = GreenhouseScraper(boards=boards, title_keywords=greenhouse_cfg.get("title_keywords"))
        all_postings.extend(gh_scraper.fetch(as_of=as_of))

    # Lever
    lever_cfg = (sources_cfg.get("lever") or {})
    if lever_cfg.get("enabled"):
        companies = lever_cfg.get("companies") or []
        lv_scraper = LeverScraper(companies=companies, title_keywords=lever_cfg.get("title_keywords"))
        all_postings.extend(lv_scraper.fetch(as_of=as_of))

    append_postings_to_csv(all_postings, cfg.csv_path, snapshot_id=snapshot_id)
    # Append this run to S3 archive.csv (if configured)
    try:
        df_run = pd.DataFrame([p.to_row() for p in all_postings], columns=COLUMNS)
        df_run["snapshot_id"] = snapshot_id
        if not df_run.empty:
            append_to_s3_archive(df_run)
    except Exception as e:
        print(f"[s3] archive append failed: {e}")
    return len(all_postings)


def main() -> None:
    args = _parse_args()
    cfg = AppConfig()
    ensure_dirs(cfg)
    as_of = _parse_date(args.as_of)
    count = run_once(as_of=as_of, cfg=cfg)
    print(f"Collected {count} postings for {as_of.isoformat()} → {cfg.csv_path}")


if __name__ == "__main__":
    main() 