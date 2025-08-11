# Uses Playwright + Python (Chromium preinstalled)
FROM mcr.microsoft.com/playwright/python:v1.45.0-jammy

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir awscli

# Install Playwright browsers (Chromium used by LinkedIn)
RUN python -m playwright install chromium

# Copy project
COPY . .

# Default command does nothing; ECS RunTask will override with CMD
CMD ["bash","-lc","python -m src.job_scraper.runner"]