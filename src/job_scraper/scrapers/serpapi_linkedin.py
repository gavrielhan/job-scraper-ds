from __future__ import annotations
from datetime import date
from typing import List, Optional
import os
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from ..models import JobPosting
from .base import ScraperBase


def _canonical_linkedin_url(url: str) -> Optional[str]:
    if not url:
        return None
    u = url.strip()
    if u.startswith("/"):
        u = f"https://www.linkedin.com{u}"
    if "linkedin.com" not in u.lower():
        return None
    if "?" in u:
        u = u.split("?")[0]
    return u


def _canonical_url(url: str) -> Optional[str]:
    if not url:
        return None
    u = url.strip()
    if u.startswith("/"):
        u = f"https://www.linkedin.com{u}"
    if "?" in u:
        u = u.split("?")[0]
    return u


class SerpapiLinkedInScraper(ScraperBase):
    def __init__(self, *, api_key: Optional[str], query: str = "Data Scientist", location: str = "Israel"):
        self.api_key = api_key or os.getenv("SERPAPI_API_KEY")
        # Build combined OR query for DS/DE/AI/MLE roles
        if query:
            base = query
        else:
            base = "Data Scientist"
        self.query = f"(\"data scientist\" OR \"data engineer\" OR \"ai engineer\" OR \"machine learning engineer\")"
        self.location = location
        # Preferred Israel city list to try
        self.locations_to_try: List[str] = [
            location,
            "Tel Aviv-Yafo, Israel",
            "Jerusalem, Israel",
            "Haifa, Israel",
            "Herzliya, Israel",
            "Ra'anana, Israel",
            "Beer Sheva, Israel",
        ]

    @retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3))
    def _call(self, params: dict) -> dict:
        resp = requests.get("https://serpapi.com/search.json", params=params, timeout=45)
        if resp.status_code != 200:
            return {}
        return resp.json() or {}

    def _search(self) -> dict:
        attempts = []
        for loc in self.locations_to_try:
            attempts.append({
                "engine": "google_jobs",
                "q": f"{self.query} {loc}",
                "location": loc,
                "api_key": self.api_key,
            })
            # Also try with query only and location param
            attempts.append({
                "engine": "google_jobs",
                "q": self.query,
                "location": loc,
                "api_key": self.api_key,
            })
        data: dict = {}
        for p in attempts:
            data = self._call(p)
            if (data.get("jobs_results") or []):
                break
        return data

    def fetch(self, *, as_of: date) -> List[JobPosting]:
        if not self.api_key:
            return []
        data = self._search()
        items = data.get("jobs_results", []) if isinstance(data, dict) else []
        results: List[JobPosting] = []
        for item in items:
            title = (item.get("title") or "").strip()
            company = (item.get("company_name") or "").strip()
            location = (item.get("location") or self.location).strip()
            url = None
            for opt in item.get("apply_options") or []:
                link = _canonical_linkedin_url(opt.get("link") or "")
                if link:
                    url = link
                    break
            if not url:
                for opt in item.get("apply_options") or []:
                    link = _canonical_url(opt.get("link") or "")
                    if link:
                        url = link
                        break
            if not url:
                url = _canonical_url((item.get("related_links", [{}])[0].get("link") or ""))
            if not title or not company or not url:
                continue
            results.append(
                JobPosting(
                    source="Google Jobs (SerpAPI)",
                    job_title=title,
                    company=company,
                    location=location,
                    url=url,
                    collected_at=as_of,
                )
            )
        return results 