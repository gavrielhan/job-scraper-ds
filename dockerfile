# Uses Playwright + Python (Chromium preinstalled)
FROM mcr.microsoft.com/playwright/python:v1.54.0-jammy

WORKDIR /app
COPY requirements.txt .
# Install CPU-only torch first, then the rest
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch==2.4.0 && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir awscli

# DO NOT install browsers again (already in base image)
# RUN python -m playwright install chromium   # <-- remove this line

# Copy project
COPY . .

# Default command does nothing; ECS RunTask will override with CMD
CMD ["bash","-lc","python -m src.job_scraper.runner"]