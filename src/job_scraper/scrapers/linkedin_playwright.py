from __future__ import annotations
import os, os.path
import re
import time
import json
from datetime import date
from typing import List, Optional, Any, Set
from tenacity import retry, stop_after_attempt, wait_exponential
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from ..models import JobPosting
from .base import ScraperBase

# New helpers to block trackers and clear modal overlays

def _should_block(url: str) -> bool:
    u = (url or "").lower()
    return any(host in u for host in [
        "doubleclick.net", "googletagmanager.com", "googlesyndication.com",
        "google-analytics.com", "demdex.net", "facebook.net", "bat.bing.com"
    ])


def clear_overlays(page) -> None:
    try:
        page.evaluate(
            """
          for (const sel of [
            '.modal__overlay', '.top-level-modal-container',
            '.artdeco-modal-overlay', '[data-test-modal-overlay]'
          ]) { document.querySelectorAll(sel).forEach(el => el.remove()); }
        """
        )
    except Exception:
        pass


def _normalize_title(title: str) -> str:
    t = " ".join((title or "").split())
    if not t:
        return t
    # Collapse exact duplicated phrase (with or without whitespace between repeats)
    # e.g., "Junior Data AnalystJunior Data Analyst" or "Title Title"
    m = re.match(r"^(?P<p>.+?)(?:\s*\1)+$", t, flags=re.IGNORECASE)
    if m:
        t = m.group("p").strip()
    tokens = t.split(" ")
    changed = True
    while changed and len(tokens) >= 2:
        changed = False
        max_k = len(tokens) // 2
        for k in range(max_k, 0, -1):
            if tokens[:k] == tokens[k:2 * k]:
                tokens = tokens[:k] + tokens[2 * k:]
                changed = True
                break
    dedup: List[str] = []
    for w in tokens:
        if not dedup or dedup[-1].lower() != w.lower():
            dedup.append(w)
    t = " ".join(dedup)
    t = re.sub(r"\s+with verification\b", "", t, flags=re.IGNORECASE).strip()
    return t


def _normalize_location_text(raw: str) -> str:
    if not raw:
        return ""
    t = " ".join(raw.replace("\n", " ").split()).strip(", ")
    # Common variants
    variants = {
        "tel aviv": "Tel Aviv",
        "tel-aviv": "Tel Aviv",
        "tel aviv-yafo": "Tel Aviv",
        "tel-aviv-yafo": "Tel Aviv",
        "jerusalem": "Jerusalem",
        "haifa": "Haifa",
        "herzliya": "Herzliya",
        "ra'anana": "Ra'anana",
        "beer sheva": "Beer Sheva",
        "be'er sheva": "Beer Sheva",
    }
    tl = t.lower()
    for k, v in variants.items():
        if k in tl:
            t = v
            break
    # Append country if only city
    if t and "israel" not in t.lower():
        t = f"{t}, Israel"
    # Collapse "Israel, Israel"
    t = t.replace(", Israel, Israel", ", Israel")
    return t


def _extract_company_from_topcard(page) -> str:
    selectors = [
        "a.jobs-unified-top-card__company-name",
        "a.topcard__org-name-link",
        ".jobs-unified-top-card__company-name a",
        ".jobs-unified-top-card__subtitle-primary-grouping a",
        ".jobs-unified-top-card__company-name-without-image a",
        ".topcard__flavor a",
        ".jobs-unified-top-card__primary-description a",
    ]
    try:
        topcard = page.query_selector(".jobs-unified-top-card, .topcard") or page
        link = topcard.query_selector("a[href*='/company/']")
        if link:
            txt = (link.inner_text() or "").strip()
            if txt:
                return txt
    except Exception:
        pass
    for sel in selectors:
        try:
            el = page.query_selector(sel)
            if el:
                txt = (el.inner_text() or "").strip()
                if txt and txt.lower() != "none":
                    return txt
        except Exception:
            continue
    return ""


def _deep_find_company(obj: Any) -> Optional[str]:
    try:
        if isinstance(obj, dict):
            # Direct patterns
            org = obj.get("hiringOrganization") or obj.get("organization")
            if isinstance(org, dict):
                name = org.get("name")
                if isinstance(name, str) and name.strip():
                    return name.strip()
            # Company-like keys
            for k, v in obj.items():
                kl = str(k).lower()
                if kl in {"company", "companyname", "employer"}:
                    if isinstance(v, str) and v.strip():
                        return v.strip()
                    if isinstance(v, dict):
                        n = v.get("name")
                        if isinstance(n, str) and n.strip():
                            return n.strip()
                # Recurse
                r = _deep_find_company(v)
                if r:
                    return r
        elif isinstance(obj, list):
            for it in obj:
                r = _deep_find_company(it)
                if r:
                    return r
    except Exception:
        return None
    return None


def _extract_company_from_json(page) -> str:
    # Try ld+json blocks first
    try:
        for script in page.query_selector_all('script[type="application/ld+json"]'):
            try:
                txt = script.inner_text()
                if not txt:
                    continue
                data = json.loads(txt)
                company = _deep_find_company(data)
                if company:
                    return company
            except Exception:
                continue
    except Exception:
        pass
    # Try __NEXT_DATA__
    try:
        next_data = page.query_selector('#__NEXT_DATA__')
        if next_data:
            data = json.loads(next_data.inner_text() or "{}")
            company = _deep_find_company(data)
            if company:
                return company
    except Exception:
        pass
    return ""


def _extract_location_from_json(page) -> str:
    # ld+json first
    try:
        for script in page.query_selector_all('script[type="application/ld+json"]'):
            try:
                data = json.loads(script.inner_text() or "{}")
                # jobLocation can be dict or list
                jl = data.get("jobLocation")
                if jl:
                    objs = jl if isinstance(jl, list) else [jl]
                    for o in objs:
                        addr = o.get("address") if isinstance(o, dict) else None
                        if isinstance(addr, dict):
                            city = (addr.get("addressLocality") or "").strip()
                            country = (addr.get("addressCountry") or "").strip()
                            if city:
                                loc = city
                                if country:
                                    loc = f"{city}, {country}"
                                return _normalize_location_text(loc)
            except Exception:
                continue
    except Exception:
        pass
    # __NEXT_DATA__ fallback
    try:
        next_data = page.query_selector('#__NEXT_DATA__')
        if next_data:
            data = json.loads(next_data.inner_text() or "{}")
            # Deep search for addressLocality
            def deep(o):
                if isinstance(o, dict):
                    addr = o.get("address")
                    if isinstance(addr, dict):
                        city = addr.get("addressLocality")
                        country = addr.get("addressCountry")
                        if city:
                            loc = city
                            if country:
                                loc = f"{city}, {country}"
                            return loc
                    for v in o.values():
                        r = deep(v)
                        if r:
                            return r
                if isinstance(o, list):
                    for it in o:
                        r = deep(it)
                        if r:
                            return r
                return None
            loc = deep(data)
            if loc:
                return _normalize_location_text(loc)
    except Exception:
        pass
    return ""


def _extract_location_from_topcard(page) -> str:
    try:
        # Look for bullets under subtitle grouping
        grouping = page.query_selector(".jobs-unified-top-card__subtitle-primary-grouping")
        if grouping:
            text = grouping.inner_text() or ""
            # Split by separators
            for seg in re.split(r"[•·|]", text):
                seg = seg.strip()
                if seg and ("israel" in seg.lower() or len(seg.split()) <= 3):
                    return _normalize_location_text(seg)
        # Generic bullets
        for sel in [".jobs-unified-top-card__bullet", ".topcard__flavor--bullet", ".topcard__flavor"]:
            for el in page.query_selector_all(sel):
                seg = (el.inner_text() or "").strip()
                if seg and ("israel" in seg.lower() or len(seg.split()) <= 3):
                    return _normalize_location_text(seg)
    except Exception:
        pass
    return ""


def _extract_location_from_guest_endpoint(context, job_url: str) -> str:
    try:
        m = re.search(r"/jobs/view/(\d+)/", job_url)
        if not m:
            return ""
        job_id = m.group(1)
        guest_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
        page = context.new_page()
        page.set_default_timeout(20000)
        page.goto(guest_url, timeout=20000)
        try:
            page.wait_for_selector(".topcard__flavor--bullet, .topcard__flavor", timeout=5000)
        except Exception:
            pass
        # Collect bullet flavors and pick plausible city
        bullets = []
        for sel in [".topcard__flavor--bullet", ".topcard__flavor"]:
            for el in page.query_selector_all(sel):
                bullets.append((el.inner_text() or "").strip())
        page.close()
        for b in bullets:
            if b and ("israel" in b.lower() or len(b.split()) <= 3):
                return _normalize_location_text(b)
        return ""
    except Exception:
        try:
            page.close()
        except Exception:
            pass
        return ""


def _extract_company_from_guest_endpoint(context, job_url: str) -> str:
    try:
        m = re.search(r"/jobs/view/(\d+)/", job_url)
        if not m:
            return ""
        job_id = m.group(1)
        guest_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
        page = context.new_page()
        page.set_default_timeout(20000)
        page.goto(guest_url, timeout=20000)
        try:
            page.wait_for_selector(".topcard__org-name-link, .topcard__flavor", timeout=5000)
        except Exception:
            pass
        el = page.query_selector(".topcard__org-name-link") or page.query_selector(".topcard__flavor")
        txt = (el.inner_text() or "").strip() if el else ""
        page.close()
        return txt
    except Exception:
        try:
            page.close()
        except Exception:
            pass
        return ""


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
        max_pages: int = 100,
        time_budget_sec: int = 400,
        seen_urls: Optional[Set[str]] = None,
        min_new: int = 10,
        time_window: str = "r604800",
    ) -> None:
        self.email = email or os.getenv("LINKEDIN_EMAIL")
        self.password = password or os.getenv("LINKEDIN_PASSWORD")
        self.query = query
        self.location = location
        self.headless = headless
        self.max_jobs = max_jobs
        self.max_pages = max_pages
        self.time_budget_sec = time_budget_sec
        self.seen_urls: Set[str] = set(seen_urls or set())
        self.min_new = min_new
        self.time_window = time_window
        self.debug = os.getenv("DEBUG_LINKEDIN", "false").lower() == "true"
        default_state = os.path.abspath(os.path.join(os.getcwd(), "data", "linkedin_state.json"))
        state_env = os.getenv("LINKEDIN_STORAGE_STATE") or os.getenv("STORAGE_STATE")
        self.storage_state_path = storage_state_path or state_env or default_state

    def _guard_creds(self) -> None:
        if not os.path.exists(self.storage_state_path) and (not self.email or not self.password):
            raise RuntimeError(
                "LinkedIn credentials are missing. Set LINKEDIN_EMAIL and LINKEDIN_PASSWORD in .env, or provide a storage state file."
            )

    def _collect_links_via_guest_search(self, context, start: int) -> List[str]:
        url = (
            "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
            f"?keywords={self.query.replace(' ', '%20')}"
            f"&location={self.location.replace(' ', '%20')}"
            f"&f_TPR={self.time_window}&sortBy=DD&start={start}"
        )
        page = context.new_page()
        page.set_default_timeout(20000)
        page.goto(url, timeout=20000)
        links: List[str] = []
        for a in page.query_selector_all("a.base-card__full-link"):
            href = a.get_attribute("href") or ""
            if not href:
                continue
            if href.startswith("/"):
                href = "https://www.linkedin.com" + href
            href = href.split("?", 1)[0]
            links.append(href)
        try:
            page.close()
        except Exception:
            pass
        return links

    @retry(wait=wait_exponential(multiplier=1, min=1, max=6), stop=stop_after_attempt(2))
    def fetch(self, *, as_of: date) -> List[JobPosting]:
        # Guest search does not require login; storage state optional
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context()
            try:
                new_urls: List[str] = []
                start = 0
                deadline = time.time() + max(30, self.time_budget_sec)
                # Page through guest search
                for page_idx in range(max(1, self.max_pages)):
                    if time.time() > deadline or len(new_urls) >= self.min_new:
                        break
                    batch = []
                    try:
                        batch = self._collect_links_via_guest_search(context, start)
                    except Exception:
                        batch = []
                    if not batch:
                        break
                    for u in batch:
                        if u not in self.seen_urls and u not in new_urls:
                            new_urls.append(u)
                            if len(new_urls) >= self.min_new:
                                break
                    start += 25

                # Build postings for up to max_jobs
                jobs: List[JobPosting] = []
                for url in new_urls[: self.max_jobs]:
                    comp = _extract_company_from_guest_endpoint(context, url) or ""
                    loc = _extract_location_from_guest_endpoint(context, url) or self.location

                    # Try to grab title from the detail page quickly
                    title_raw = ""
                    page = context.new_page()
                    page.set_default_timeout(20000)
                    try:
                        page.goto(url, timeout=20000)
                        try:
                            page.wait_for_selector(".jobs-unified-top-card, .topcard", timeout=5000)
                        except Exception:
                            pass
                        h1 = page.query_selector("h1.jobs-unified-top-card__job-title, h1.topcard__title")
                        if h1:
                            title_raw = (h1.inner_text() or "").strip()
                        if not comp:
                            comp = _extract_company_from_json(page) or _extract_company_from_topcard(page) or comp
                        if not loc:
                            loc = _extract_location_from_json(page) or _extract_location_from_topcard(page) or loc
                    except Exception:
                        pass
                    finally:
                        try:
                            page.close()
                        except Exception:
                            pass

                    jobs.append(
                        JobPosting(
                            source="LinkedIn (Playwright)",
                            job_title=_normalize_title(title_raw) or "",
                            company=(comp or "").strip(),
                            location=loc or self.location,
                            url=url,
                            collected_at=as_of,
                        )
                    )

                return jobs
            finally:
                context.close()
                browser.close() 