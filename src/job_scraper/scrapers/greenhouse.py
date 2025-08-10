from __future__ import annotations
import re
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


class GreenhouseScraper(ScraperBase):
    def __init__(self, boards: Iterable[str], *, title_keywords: Iterable[str] | None = None):
        self.boards = list(boards)
        self.title_keywords = [kw.lower() for kw in (title_keywords or ["data scientist"])]

    @retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3))
    def _fetch_board(self, board: str) -> List[dict]:
        url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true"
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            return []
        data = resp.json() or {}
        return data.get("jobs", [])

    def fetch(self, *, as_of: date) -> List[JobPosting]:
        results: List[JobPosting] = []
        for board in self.boards:
            jobs = self._fetch_board(board)
            for job in jobs:
                title: str = (job.get("title") or "").strip()
                location_obj = job.get("location") or {}
                location = (location_obj.get("name") or "").strip()
                if not title:
                    continue
                title_l = title.lower()
                if not any(kw in title_l for kw in self.title_keywords):
                    continue
                if location and not _looks_israel(location):
                    continue
                url = (job.get("absolute_url") or "").strip()
                company = (job.get("company", {}).get("name") or board).strip()
                results.append(
                    JobPosting(
                        source="Greenhouse",
                        job_title=title,
                        company=company or board,
                        location=location or "Israel",
                        url=url or f"https://boards.greenhouse.io/{board}",
                        collected_at=as_of,
                    )
                )
        return results 