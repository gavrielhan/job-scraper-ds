# Job Scraper DS

Automated system to collect and present data on open Data Scientist positions in Israel.

## Features
- Scrapes every run from multiple sources (configurable)
  - Greenhouse boards API (no API key)
  - Lever postings API (no API key)
  - Optional: LinkedIn via SerpAPI Google Jobs (requires API key)
  - Optional: LinkedIn via SearchApi.io Google Jobs (requires API key)
  - Optional: LinkedIn via Playwright with your credentials (headless browser)
- Appends to a dynamic CSV with columns: `source, job_title, company, location, url, collected_at`
- Interactive Streamlit dashboard with filters and trend over time
- Ready for cron or ad-hoc scheduling

## Quickstart
1. Clone and enter the repo, then setup:
   ```bash
   bash scripts/setup.sh
   ```
2. Copy `.env.example` to `.env` (or use the pre-created `.env`) and set values as needed:
   ```env
   TZ=Asia/Jerusalem
   SERPAPI_API_KEY=
   SEARCHAPI_API_KEY=
   LINKEDIN_EMAIL=you@example.com
   LINKEDIN_PASSWORD=your-password
   LINKEDIN_HEADLESS=true
   LINKEDIN_MAX_JOBS=60
   ```
3. Configure sources in `config/sources.yaml` (edit company slugs and enable/disable sources).
4. Run a scrape (today):
   ```bash
   python -m src.job_scraper.runner
   ```
5. Backfill 2 additional snapshots (example):
   ```bash
   python -m src.job_scraper.runner --as-of $(date -v-1d +%F)
   python -m src.job_scraper.runner --as-of $(date -v-2d +%F)
   ```
6. Launch the dashboard:
   ```bash
   streamlit run src/dashboard/app.py
   ```

## LinkedIn Options
- SerpAPI (indirect):
  - Get an API key at `https://serpapi.com`.
  - Put the key in `.env` as `SERPAPI_API_KEY`.
  - Set `linkedin_serpapi.enabled: true` in `config/sources.yaml`.
- SearchApi.io (indirect):
  - Sign up and get an API key (100 free requests; no card) at [SearchApi.io](https://www.searchapi.io/).
  - Put the key in `.env` as `SEARCHAPI_API_KEY`.
  - Set `searchapi_linkedin.enabled: true` in `config/sources.yaml`.
- Playwright (direct, requires credentials):
  - Put `LINKEDIN_EMAIL` and `LINKEDIN_PASSWORD` in `.env`.
  - In `config/sources.yaml`, set `linkedin_playwright.enabled: true` and adjust `query`, `location`, `headless`, `max_jobs` if needed.
  - Note: Accounts with 2FA or additional verification may fail non-interactively. If that happens, set `headless: false` to complete any prompts manually the first time.

## Scheduling
Use cron (every 12 hours example):
```cron
0 */12 * * * cd /path/to/job-scraper-ds && /path/to/python -m src.job_scraper.runner >> logs/scrape.log 2>&1
```
Or run ad-hoc with any interval using a process manager.

## Configuring Sources
- `config/sources.yaml` contains:
  - `greenhouse.companies`: list of Greenhouse board slugs (e.g., `lemonade`, `riskified`, `fiverr`, `appsflyer`, `similarweb`).
  - `lever.companies`: list of Lever slugs (e.g., `via`, `pagaya`, `vastdata`).
  - `title_keywords`: filter titles (default includes `data scientist`).
  - `linkedin_serpapi`: set `enabled: true` and provide `SERPAPI_API_KEY` in `.env` to include LinkedIn jobs discovered via Google Jobs.

### LinkedIn Options
Direct scraping of LinkedIn is subject to heavy anti-bot protections and Terms of Service. This project ships an optional integration using SerpAPI's Google Jobs engine:
- Get an API key by creating an account at `https://serpapi.com`.
- Put the key in `.env` as `SERPAPI_API_KEY`.
- Enable `linkedin_serpapi.enabled: true` in `config/sources.yaml`.

If you prefer browser automation (Playwright), you can add another scraper using your LinkedIn credentials in `.env` (`LINKEDIN_EMAIL`, `LINKEDIN_PASSWORD`). This is not included by default.

## Data & Dashboard
- CSV path: `data/jobs.csv`. The file is append-only with de-duplication per `(url, collected_at)`.
- Dashboard: `streamlit run src/dashboard/app.py` provides:
  - Filters by source, company, and title substring.
  - Chart of counts per `collected_at`.
  - Table of the latest snapshot.

## Notes
- Location filter is keyword-based for Israel, and can be tuned in the scrapers.
- Add more sources easily by adding new scrapers under `src/job_scraper/scrapers/` and wiring them in `runner.py`.

## License
MIT
