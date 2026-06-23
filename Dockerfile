FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt boto3>=1.34.0

COPY . .

RUN chmod +x scripts/run_job.sh \
    && useradd --create-home --uid 1000 cortex \
    && mkdir -p /var/cortex/cache \
    && chown -R cortex:cortex /app /var/cortex/cache

USER cortex
ENV CORTEX_SKIP_DOTENV=1 \
    CORTEX_CACHE_DIR=/var/cortex/cache \
    CORTEX_LOG_FORMAT=json \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/scripts/run_job.sh"]
CMD ["nightly-core"]
