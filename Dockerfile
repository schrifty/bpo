FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt boto3>=1.34.0

COPY . .

RUN chmod +x scripts/run_job.sh \
    && useradd --create-home --uid 1000 bpo \
    && mkdir -p /var/bpo/cache \
    && chown -R bpo:bpo /app /var/bpo/cache

USER bpo
ENV BPO_SKIP_DOTENV=1 \
    BPO_CACHE_DIR=/var/bpo/cache \
    BPO_LOG_FORMAT=json \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/scripts/run_job.sh"]
CMD ["nightly-core"]
