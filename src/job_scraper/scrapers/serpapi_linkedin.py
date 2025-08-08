from __future__ import annotations
from datetime import date
from typing import List, Optional
import os
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from ..models import JobPosting
from .base import ScraperBase


class SerpapiLinkedInScraper(ScraperBase):
    def __init__(self, *, api_key: Optional[str], query: str = "Data Scientist", location: str = "Israel"):
        self.api_key = api_key
        self.query = query
        self.location = location

    @retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3))
    def _search(self, q: str, location: str) -> dict:
        url = "https://serpapi.com/search.json"
        params = {
            "engine": "google_jobs",
            "q": q,
            "location": location,
            "api_key": self.api_key,
        }
        resp = requests.get(url, params=params, timeout=45)
        if resp.status_code != 200:
            return {}
        return resp.json() or {}

    def fetch(self, *, as_of: date) -> List[JobPosting]:
        if not self.api_key:
            return []
        data = self._search(self.query, self.location)
        items = data.get("jobs_results", []) if isinstance(data, dict) else []
        results: List[JobPosting] = []
        for item in items:
            via = (item.get("via") or "").lower()
            if "linkedin" not in via:
                # Try to detect LinkedIn link in apply options
                apply_options = item.get("apply_options") or []
                has_linkedin = any("linkedin.com" in (opt.get("link") or "").lower() for opt in apply_options)
                if not has_linkedin:
                    continue
            title = (item.get("title") or "").strip()
            company = (item.get("company_name") or "").strip()
            location = (item.get("location") or "Israel").strip()
            url = None
            # Prefer direct LinkedIn apply link
            for opt in item.get("apply_options") or []:
                link = (opt.get("link") or "").strip()
                if "linkedin.com" in link:
                    url = link
                    break
            if not url:
                # Fallback to job posting link
                url = (item.get("related_links", [{}])[0].get("link") or item.get("job_id") or "").strip()
            if not title or not company or not url:
                continue
            results.append(
                JobPosting(
                    source="LinkedIn (via SerpAPI)",
                    job_title=title,
                    company=company,
                    location=location,
                    url=url,
                    collected_at=as_of,
                )
            )
        return results 