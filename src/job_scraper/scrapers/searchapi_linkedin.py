from __future__ import annotations
from datetime import date
from typing import List, Optional
import os
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from ..models import JobPosting
from .base import ScraperBase


class SearchApiLinkedInScraper(ScraperBase):
    def __init__(self, *, api_key: Optional[str], query: str = "Data Scientist", location: str = "Israel"):
        self.api_key = api_key or os.getenv("SEARCHAPI_API_KEY")
        self.query = query
        self.location = location
        self.base_url = "https://www.searchapi.io/api/v1/search"

    @retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3))
    def _search(self, q: str, location: str) -> dict:
        if not self.api_key:
            return {}
        params = {
            "engine": "google_jobs",
            "q": q,
            "location": location,
            "api_key": self.api_key,
        }
        resp = requests.get(self.base_url, params=params, timeout=45)
        if resp.status_code != 200:
            return {}
        return resp.json() or {}

    def fetch(self, *, as_of: date) -> List[JobPosting]:
        if not self.api_key:
            return []
        data = self._search(self.query, self.location)
        items = []
        if isinstance(data, dict):
            # SearchApi.io typically returns items under 'jobs_results' for google_jobs
            items = data.get("jobs_results", [])
        results: List[JobPosting] = []
        for item in items:
            # Accept either LinkedIn as 'via' or presence of linkedin.com in apply links
            via = (item.get("via") or "").lower()
            apply_options = item.get("apply_options") or []
            has_linkedin_apply = any("linkedin.com" in (opt.get("link") or "").lower() for opt in apply_options)
            if "linkedin" not in via and not has_linkedin_apply:
                continue
            title = (item.get("title") or "").strip()
            company = (item.get("company_name") or "").strip()
            location = (item.get("location") or self.location).strip()
            url = None
            for opt in apply_options:
                link = (opt.get("link") or "").strip()
                if "linkedin.com" in link:
                    url = link
                    break
            if not url:
                # Fallback to any related link or job_id
                url = (item.get("related_links", [{}])[0].get("link") or item.get("job_id") or "").strip()
            if not title or not company or not url:
                continue
            results.append(
                JobPosting(
                    source="LinkedIn (SearchApi)",
                    job_title=title,
                    company=company,
                    location=location,
                    url=url,
                    collected_at=as_of,
                )
            )
        return results 