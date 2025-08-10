from __future__ import annotations
import os
import pandas as pd
from typing import List
from .models import JobPosting


COLUMNS = ["source", "job_title", "company", "location", "url", "collected_at"]


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Drop any legacy columns not in COLUMNS
    for col in list(df.columns):
        if col not in COLUMNS:
            df = df.drop(columns=[col])
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[COLUMNS]


def append_postings_to_csv(postings: List[JobPosting], csv_path: str) -> None:
    if not postings:
        return
    new_df = pd.DataFrame([p.to_row() for p in postings], columns=COLUMNS)
    if os.path.exists(csv_path):
        existing = pd.read_csv(csv_path)
        existing = _ensure_columns(existing)
        combined = pd.concat([existing, new_df], ignore_index=True)
        # Prefer rows that have a non-empty company for the same URL
        combined["company_len"] = combined["company"].fillna("").astype(str).str.len()
        combined = (
            combined.sort_values(["url", "company_len", "collected_at"]).drop_duplicates(subset=["url"], keep="last")
            .drop(columns=["company_len"])  # tidy
            .sort_values(["collected_at", "company", "job_title"])  # final order
            .reset_index(drop=True)
        )
        combined.to_csv(csv_path, index=False)
    else:
        new_df.to_csv(csv_path, index=False) 