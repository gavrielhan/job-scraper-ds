from __future__ import annotations
import os
import re
import time
import json
from datetime import date
from typing import List, Optional, Any
from tenacity import retry, stop_after_attempt, wait_exponential
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from ..models import JobPosting
from .base import ScraperBase


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
    ) -> None:
        self.email = email or os.getenv("LINKEDIN_EMAIL")
        self.password = password or os.getenv("LINKEDIN_PASSWORD")
        self.query = query
        self.location = location
        self.headless = headless
        self.max_jobs = max_jobs
        self.debug = os.getenv("DEBUG_LINKEDIN", "false").lower() == "true"
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
                        link_el = (
                            card.query_selector("a.base-card__full-link")
                            or card.query_selector("a.job-card-list__title")
                            or card.query_selector("a")
                        )
                        link_raw = link_el.get_attribute("href") if link_el else None
                        if not link_raw:
                            continue
                        url = link_raw
                        if url.startswith("/"):
                            url = f"https://www.linkedin.com{url}"
                        if "linkedin.com" not in url.lower():
                            continue
                        if "?" in url:
                            url = url.split("?")[0]
                        if url in seen_links:
                            continue

                        title_raw = ""
                        company_raw = ""
                        try:
                            card_title_el = (
                                card.query_selector(".base-search-card__title")
                                or card.query_selector(".job-card-list__title")
                                or card.query_selector(".job-card-container__link")
                            )
                            if card_title_el:
                                title_raw = card_title_el.inner_text().strip()
                        except Exception:
                            pass
                        try:
                            card_company_el = (
                                card.query_selector(".base-search-card__subtitle a")
                                or card.query_selector(".job-card-container__company-name")
                                or card.query_selector(".base-search-card__subtitle")
                            )
                            if card_company_el:
                                company_raw = (card_company_el.inner_text() or "").strip()
                        except Exception:
                            pass

                        # Click and wait for detail
                        try:
                            card.click()
                            page.wait_for_timeout(500)
                            try:
                                page.wait_for_selector("h1.jobs-unified-top-card__job-title, .jobs-unified-top-card, .topcard", timeout=5000)
                            except Exception:
                                pass
                        except Exception:
                            pass
                        detail_title_raw = ""
                        try:
                            detail_title_el = page.query_selector("h1.jobs-unified-top-card__job-title, h1.topcard__title")
                            if detail_title_el:
                                detail_title_raw = detail_title_el.inner_text().strip()
                        except Exception:
                            pass
                        # Try JSON extraction first
                        json_company_raw = _extract_company_from_json(page)
                        # Fallback to topcard text
                        detail_company_raw = json_company_raw or _extract_company_from_topcard(page)

                        if self.debug:
                            print(json.dumps({
                                "debug_source": "LinkedInPlaywright",
                                "card": {"title_raw": title_raw, "company_raw": company_raw, "link_raw": link_raw},
                                "detail": {"title_raw": detail_title_raw, "company_raw": detail_company_raw, "json_company_raw": json_company_raw},
                            }, ensure_ascii=False))

                        # Processed values
                        title = _normalize_title(detail_title_raw or title_raw)
                        company = (detail_company_raw or company_raw or "").strip()
                        # Location extraction
                        loc = _extract_location_from_json(page) or _extract_location_from_topcard(page)

                        # Last resort: open job URL directly
                        if not company:
                            try:
                                details_page = context.new_page()
                                details_page.set_default_timeout(20000)
                                details_page.goto(url, timeout=20000)
                                try:
                                    details_page.wait_for_selector(".jobs-unified-top-card, .topcard", timeout=5000)
                                except Exception:
                                    pass
                                comp2 = _extract_company_from_json(details_page) or _extract_company_from_topcard(details_page)
                                if self.debug:
                                    print(json.dumps({"debug_source":"LinkedInPlaywright","direct_open_company_raw":comp2,"url":url}, ensure_ascii=False))
                                if comp2:
                                    company = comp2
                                # Try location too
                                if not loc:
                                    loc2 = _extract_location_from_json(details_page) or _extract_location_from_topcard(details_page)
                                    if loc2:
                                        loc = loc2
                            except Exception:
                                pass
                            finally:
                                try:
                                    details_page.close()
                                except Exception:
                                    pass

                        # Guest endpoint fallback (no login content)
                        if not company:
                            comp3 = _extract_company_from_guest_endpoint(context, url)
                            if self.debug:
                                print(json.dumps({"debug_source":"LinkedInPlaywright","guest_endpoint_company_raw":comp3,"url":url}, ensure_ascii=False))
                            if comp3:
                                company = comp3
                        if not loc:
                            loc3 = _extract_location_from_guest_endpoint(context, url)
                            if self.debug:
                                print(json.dumps({"debug_source":"LinkedInPlaywright","guest_endpoint_location_raw":loc3,"url":url}, ensure_ascii=False))
                            if loc3:
                                loc = loc3

                        seen_links.add(url)
                        jobs.append(
                            JobPosting(
                                source="LinkedIn (Playwright)",
                                job_title=title or "",
                                company=company or "",
                                location=loc or self.location,
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