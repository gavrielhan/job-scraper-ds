from __future__ import annotations
from datetime import date
from typing import Iterable, List
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from ..models import JobPosting
from .base import ScraperBase


ISRAEL_KEYWORDS = [
    "israel",
    "tel aviv",
    "tel-aviv",
    "jerusalem",
    "haifa",
    "herzliya",
    "ra'anana",
    "beer sheva",
    "be'er sheva",
]


def _looks_israel(location: str) -> bool:
    s = location.lower()
    return any(k in s for k in ISRAEL_KEYWORDS)


class LeverScraper(ScraperBase):
    def __init__(self, companies: Iterable[str], *, title_keywords: Iterable[str] | None = None):
        self.companies = list(companies)
        self.title_keywords = [kw.lower() for kw in (title_keywords or ["data scientist"])]

    @retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3))
    def _fetch_company(self, company: str) -> List[dict]:
        url = f"https://api.lever.co/v0/postings/{company}?mode=json"
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            return []
        return resp.json() or []

    def fetch(self, *, as_of: date) -> List[JobPosting]:
        results: List[JobPosting] = []
        for company in self.companies:
            jobs = self._fetch_company(company)
            for job in jobs:
                title: str = (job.get("text") or job.get("title") or "").strip()
                location = (job.get("categories", {}).get("location") or "").strip()
                if not title:
                    continue
                title_l = title.lower()
                if not any(kw in title_l for kw in self.title_keywords):
                    continue
                if location and not _looks_israel(location):
                    continue
                url = (job.get("hostedUrl") or job.get("applyUrl") or "").strip()
                company_name = (job.get("company") or company).strip()
                results.append(
                    JobPosting(
                        source="Lever",
                        job_title=title,
                        company=company_name or company,
                        location=location or "Israel",
                        url=url or f"https://jobs.lever.co/{company}",
                        collected_at=as_of,
                    )
                )
        return results 