from __future__ import annotations
import os
import re
import time
from datetime import date
from typing import List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from ..models import JobPosting
from .base import ScraperBase


def _normalize_title(title: str) -> str:
    t = " ".join((title or "").split())
    if not t:
        return t
    tokens = t.split(" ")
    # Remove any leading duplicated phrase (e.g., X Y X Y [rest] -> X Y [rest])
    changed = True
    while changed and len(tokens) >= 2:
        changed = False
        max_k = len(tokens) // 2
        for k in range(max_k, 0, -1):
            if tokens[:k] == tokens[k:2 * k]:
                tokens = tokens[:k] + tokens[2 * k:]
                changed = True
                break
    # Collapse consecutive duplicate words
    dedup: List[str] = []
    for w in tokens:
        if not dedup or dedup[-1].lower() != w.lower():
            dedup.append(w)
    t = " ".join(dedup)
    # Specific cleanup
    t = re.sub(r"\s+with verification\b", "", t, flags=re.IGNORECASE).strip()
    return t


class LinkedInPlaywrightScraper(ScraperBase):
    def __init__(
        self,
        *,
        email: Optional[str] = None,
        password: Optional[str] = None,
        query: str = "Data Scientist",
        location: str = "Israel",
        headless: bool = True,
        max_jobs: int = 60,
        storage_state_path: Optional[str] = None,
    ) -> None:
        self.email = email or os.getenv("LINKEDIN_EMAIL")
        self.password = password or os.getenv("LINKEDIN_PASSWORD")
        self.query = query
        self.location = location
        self.headless = headless
        self.max_jobs = max_jobs
        default_state = os.path.abspath(os.path.join(os.getcwd(), "data", "linkedin_state.json"))
        self.storage_state_path = storage_state_path or os.getenv("LINKEDIN_STORAGE_STATE", default_state)

    def _guard_creds(self) -> None:
        if not os.path.exists(self.storage_state_path) and (not self.email or not self.password):
            raise RuntimeError(
                "LinkedIn credentials are missing. Set LINKEDIN_EMAIL and LINKEDIN_PASSWORD in .env, or provide a storage state file."
            )

    @retry(wait=wait_exponential(multiplier=1, min=1, max=6), stop=stop_after_attempt(2))
    def fetch(self, *, as_of: date) -> List[JobPosting]:
        self._guard_creds()
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(storage_state=self.storage_state_path) if os.path.exists(self.storage_state_path) else browser.new_context()
            page = context.new_page()
            page.set_default_timeout(90000)
            try:
                if not os.path.exists(self.storage_state_path):
                    page.goto("https://www.linkedin.com/login", timeout=90000)
                    try:
                        page.click("button:has-text('Accept')", timeout=3000)
                    except Exception:
                        pass
                    page.fill("input#username", self.email)
                    page.fill("input#password", self.password)
                    page.click("button[type=submit]")
                    page.wait_for_timeout(2000)
                    os.makedirs(os.path.dirname(self.storage_state_path), exist_ok=True)
                    context.storage_state(path=self.storage_state_path)

                search_url = (
                    "https://www.linkedin.com/jobs/search/?keywords="
                    + self.query.replace(" ", "%20")
                    + "&location="
                    + self.location.replace(" ", "%20")
                )
                page.goto(search_url, timeout=90000)
                scroll_container_sel = ".jobs-search-results-list"
                probe_selectors = ", ".join([
                    "li.jobs-search-results__list-item",
                    "div.base-card",
                    "div.job-card-container",
                ])
                for _ in range(10):
                    if page.query_selector_all(probe_selectors):
                        break
                    try:
                        if page.query_selector(scroll_container_sel):
                            page.eval_on_selector(scroll_container_sel, "el => el.scrollBy(0, 800)")
                        else:
                            page.evaluate("window.scrollBy(0, 800)")
                    except Exception:
                        pass
                    page.wait_for_timeout(1000)

                jobs: List[JobPosting] = []
                seen_links: set[str] = set()
                load_iterations = 0

                while len(jobs) < self.max_jobs and load_iterations < 30:
                    job_cards = page.query_selector_all(probe_selectors)
                    for card in job_cards:
                        # Read link first for dedupe and validity
                        link_el = (
                            card.query_selector("a.base-card__full-link")
                            or card.query_selector("a.job-card-list__title")
                            or card.query_selector("a")
                        )
                        url = link_el.get_attribute("href") if link_el else None
                        if not url:
                            continue
                        if url.startswith("/"):
                            url = f"https://www.linkedin.com{url}"
                        if "linkedin.com" not in url.lower():
                            continue
                        if url in seen_links:
                            continue

                        # Open the card to read canonical title and company from the detail panel
                        try:
                            card.click()
                            page.wait_for_timeout(500)
                        except Exception:
                            pass
                        title = None
                        company = None
                        try:
                            detail_title = page.query_selector("h1.jobs-unified-top-card__job-title, h1.topcard__title")
                            if detail_title:
                                title = _normalize_title(detail_title.inner_text().strip())
                        except Exception:
                            pass
                        try:
                            detail_company = page.query_selector("a.jobs-unified-top-card__company-name, a.topcard__org-name-link, .jobs-unified-top-card__company-name")
                            if detail_company:
                                company = (detail_company.inner_text().strip() or None)
                        except Exception:
                            pass

                        # Fallbacks from card list if needed
                        if not title:
                            try:
                                title_el = (
                                    card.query_selector(".base-search-card__title")
                                    or card.query_selector(".job-card-list__title")
                                    or card.query_selector(".job-card-container__link")
                                )
                                if title_el:
                                    title = _normalize_title(title_el.inner_text().strip())
                            except Exception:
                                pass
                        if not company:
                            try:
                                company_el = (
                                    card.query_selector(".base-search-card__subtitle a")
                                    or card.query_selector(".job-card-container__company-name")
                                    or card.query_selector(".base-search-card__subtitle")
                                    or card.query_selector(".job-card-container__primary-description")
                                )
                                if company_el:
                                    company = company_el.inner_text().strip()
                            except Exception:
                                pass
                        if company and company.lower() == "none":
                            company = None

                        seen_links.add(url)
                        jobs.append(
                            JobPosting(
                                source="LinkedIn (Playwright)",
                                job_title=title or "",
                                company=company or "",
                                location=self.location,
                                url=url,
                                collected_at=as_of,
                            )
                        )
                        if len(jobs) >= self.max_jobs:
                            break
                    if len(jobs) >= self.max_jobs:
                        break
                    try:
                        if page.query_selector(scroll_container_sel):
                            page.eval_on_selector(scroll_container_sel, "el => el.scrollTo(0, el.scrollHeight)")
                        else:
                            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    except PlaywrightTimeoutError:
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(1200)
                    load_iterations += 1

                return jobs
            finally:
                context.close()
                browser.close() 