# Job Scraper DS

Automated collection + dashboard of open Data Scientist roles in Israel.

**Live demo**: https://job-scraper-ds-k24kqrim98fqo8zvskeuea.streamlit.app/

**Screenshot**:

![Dashboard](platform_1.png)

## Features
 - Pluggable scrapers:
    - Greenhouse (API, no key)
    - Lever (API, no key)
    - LinkedIn via SerpAPI or SearchApi.io (Google Jobs; API key)
    - LinkedIn via Playwright (headless Chromium + storage state)

 - Appends to a CSV with columns: source, job_title, company, location, url, collected_at
 - Streamlit dashboard: filters, trends, and a countdown to the next scheduled run
 - Cloud-ready: S3 storage, ECS Fargate runner, Lambda trigger, EventBridge schedule



## Quickstart
1. Setup:
   ```bash
   git clone https://github.com/gavrielhan/job-scraper-ds
   cd job-scraper-ds
   bash scripts/setup.sh
   ```
2. Create `.env` in the repo root:
   ```env
   # Timezone
   TZ=Asia/Jerusalem

   # Optional: data sources
   SERPAPI_API_KEY=
   SEARCHAPI_API_KEY=
   LINKEDIN_EMAIL=you@example.com
   LINKEDIN_PASSWORD=your-password
   LINKEDIN_HEADLESS=true
   LINKEDIN_MAX_JOBS=60

   # Optional: S3 output from runner (if set, uploads after each run)
   OUTPUT_BUCKET=your-bucket
   OUTPUT_PREFIX=snapshots/
   AWS_DEFAULT_REGION=us-east-1

   # Dashboard options
   USE_S3=true                  # read CSV from S3 if available
   ENABLE_FETCH_BUTTON=false    # set true only if you configure API_URL secret

   # Dashboard self-enrichment (in-memory)
   SELF_ENRICH=true             # enrich city/title for charts without writing CSV
   SELF_ENRICH_MODE=embed       # embed | flan
   HF_SENTENCE_MODEL=sentence-transformers/all-MiniLM-L6-v2
   HF_T2T_MODEL=google/flan-t5-small
   ENRICH_THRESHOLD=0.55

   # Countdown (sidebar) – how often to re-read next_run.json from S3
   SCHEDULE_HOURS=12
   NEXT_RUN_REFRESH_SECS=36000  # 10h
   ```
3. Configure sources in `config/sources.yaml` (edit to enable/disbale sources).
4. Run a scrape:
   ```bash
   python -m src.job_scraper.runner
   ```
   Backfill examples:
   ```bash
   python -m src.job_scraper.runner --as-of $(date -v-1d +%F)
   python -m src.job_scraper.runner --as-of $(date -v-2d +%F)
   ```
5. Launch the dashboard:
   ```bash
   streamlit run src/dashboard/app.py
   ```

## S3 Uploads (runner)
- When `OUTPUT_BUCKET` is set, the runner uploads the CSV after each run to:
  - `s3://$OUTPUT_BUCKET/$OUTPUT_PREFIX/jobs_YYYY-MM-DDTHH-MM-SSZ.csv`
  - `s3://$OUTPUT_BUCKET/$OUTPUT_PREFIX/latest.csv` (stable pointer)
- Ensure AWS credentials via a task role or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_DEFAULT_REGION`.

## Dashboard Data Sources
The dashboard chooses data in this order:
1. S3 (when `USE_S3=true` and bucket/prefix are configured)
2. Local file `data/jobs.csv`
3. Remote CSV fallback (`DASHBOARD_DATA_URL`, defaults to repo raw CSV)

## Fetch Button (optional)
- Hidden by default. To enable:
  - Set env: `ENABLE_FETCH_BUTTON=true`
  - Add Secret: `API_URL` pointing to your backend job trigger
- If the button is enabled but `API_URL` is missing, a warning is shown. If disabled, nothing is shown.

## Countdown to Next Fetch (optional)
- Sidebar shows a live countdown by reading `s3://$OUTPUT_BUCKET/$OUTPUT_PREFIX/meta/next_run.json` (expects `{ "next_run_at": ISO8601 }`).
- Auto-updates the digits every second but only re-reads S3 at most every 10 hours (configurable by `NEXT_RUN_REFRESH_SECS`).
- Requires `streamlit-autorefresh` (listed in `requirements.txt`).

## Self-Enrichment in the UI (optional)
- Set `SELF_ENRICH=true` to compute normalized columns on-the-fly for charts:
  - `SELF_ENRICH_MODE=embed` (default): sentence-transformers nearest-label
  - `SELF_ENRICH_MODE=flan`: small local FLAN-T5 classifier via `transformers`
- No files are written; enrichment is in-memory only. Charts also apply heuristics to collapse verbose titles and normalize cities.

## AWS deployment (reference)
- **System overview**: Streamlit UI (Streamlit Cloud) reads the latest CSV from S3 and shows dashboards + a countdown to the next run. EventBridge (every 12h) → Lambda → starts an ECS Fargate task (Playwright/Chromium) → writes CSV to S3. Secrets Manager stores LinkedIn creds, storage state JSON, and SERPAPI key. ECR hosts the scraper image (built by the GitHub workflow).
- **Key resources**:
  - **S3**: bucket `job-scraper-ds`; data prefix `snapshots/`; countdown `snapshots/meta/next_run.json`; optional stable `snapshots/latest.csv`
  - **ECR**: repo `job-scraper`
  - **ECS**: cluster `job-scraper-cluster`, task definition `job-scraper-task` (container `scraper`)
  - **Lambda**: `job-scraper-button-test` (runs task and writes countdown JSON)
  - **EventBridge**: rule `job-scraper-every-12h` (`rate(12 hours)`)
  - **API Gateway**: `POST /fetch` → Lambda (manual trigger/testing)
- **Secrets (Secrets Manager)**:
  - `job-scraper/linkedin-email`, `job-scraper/linkedin-password`
  - `job-scraper/serpapi-api-key`
  - `job-scraper/linkedin-storage-state` (Playwright storage state JSON; avoids 2FA and overlays)
- **Minimal IAM**:
  - ECS task execution role: `AmazonECSTaskExecutionRolePolicy` + `secretsmanager:GetSecretValue` on the 4 secrets
  - ECS task role (app): `s3:PutObject` to `arn:aws:s3:::job-scraper-ds/snapshots/*`
  - Lambda role: `ecs:RunTask`, `iam:PassRole` (for the task roles), `logs:*` (basic), and `s3:PutObject` to `.../snapshots/meta/next_run.json`
- **Lambda env**:
  - `CLUSTER_ARN`, `TASK_DEF` (full ARN or `job-scraper-task:<rev>`), `SUBNETS` (comma-separated), `SEC_GROUPS`, `ASSIGN_PUBLIC_IP=ENABLED`, `CONTAINER_NAME=scraper`
  - `OUTPUT_BUCKET=job-scraper-ds`, `OUTPUT_PREFIX=snapshots/`, `SCHEDULE_HOURS=12`
- **Streamlit UI**:
  - Reads S3 CSV (stable key) and `snapshots/meta/next_run.json` for countdown
  - Keeps normalized columns internal and hides them in tables
- **Deploy new scraper version**:
  - Push code; run the GitHub Action to build/push image; register a new ECS task definition revision; update Lambda `TASK_DEF`
  - Test:
    ```bash
    # trigger once
    curl -s -X POST "https://<api-id>.execute-api.us-east-1.amazonaws.com/fetch" -H "Content-Type: application/json" -d '{}'

    # lambda logs
    aws logs tail /aws/lambda/job-scraper-button-test --since 5m --follow

    # most recent ECS task logs
    LOG_GROUP=/ecs/job-scraper-task
    STREAM=$(aws logs describe-log-streams --log-group-name $LOG_GROUP --order-by LastEventTime --descending --max-items 1 --query 'logStreams[0].logStreamName' --output text)
    aws logs tail "$LOG_GROUP" --log-stream-names "$STREAM" --follow
    ```
- **Scheduling / countdown checks**:
  ```bash
  aws events describe-rule --name job-scraper-every-12h --query '{Schedule:ScheduleExpression,State:State}' --output table
  aws s3 cp s3://job-scraper-ds/snapshots/meta/next_run.json - | jq .
  ```
- **Troubleshooting**:
  - "Invalid revision / Task Definition can not be blank": set a real `TASK_DEF` (revision or full ARN)
  - Task can’t read secrets: allow `secretsmanager:GetSecretValue` on the secret ARNs to the task execution role
  - Empty/slow logs: re-query the latest log stream name (see snippet above)
  - LinkedIn overlays: we scrape by opening job URLs directly and rely on storage state; if you re-login locally, update the `linkedin-storage-state` secret

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
  - Tip: We open job URLs directly and remove overlays to avoid blocked clicks.

## Scheduling
Use cron (every 12 hours example):
```cron
0 */12 * * * cd /path/to/job-scraper-ds && /path/to/python -m src.job_scraper.runner >> logs/scrape.log 2>&1
```
Or run ad-hoc with any interval using a process manager. If you maintain a scheduler in AWS, also write `meta/next_run.json` for the dashboard countdown.

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
