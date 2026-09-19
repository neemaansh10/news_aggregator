FROM python:3.12-slim

WORKDIR /app

# Dependencies first so the layer caches across code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir asyncpg redis aiokafka prometheus-client

COPY newsagg/ ./newsagg/
COPY main.py .

# Run as a non-root user.
RUN useradd --create-home --shell /bin/false app && chown -R app:app /app
USER app

ENV NEWSAGG_API_HOST=0.0.0.0 \
    NEWSAGG_LOG_JSON=true \
    PYTHONUNBUFFERED=1

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz').status==200 else 1)"

# Override with: demo | poll-once | init-db
CMD ["python", "main.py", "serve"]
