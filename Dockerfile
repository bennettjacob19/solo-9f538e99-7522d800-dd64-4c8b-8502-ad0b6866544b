FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ARTIFACT_DATA_DIR=/data

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY artifact_repo ./artifact_repo

RUN useradd --system --uid 10001 --home /app app \
 && mkdir -p /data \
 && chown -R app:app /data
USER app

VOLUME ["/data"]
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/state', timeout=4)"

CMD ["python", "-m", "artifact_repo", "--data-dir", "/data", "--host", "0.0.0.0", "--port", "8000"]
