FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py index.html ./
RUN useradd --create-home app && mkdir /data && chown app /data
USER app
ENV DB_PATH=/data/storm_baler.db
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz')"
# Two workers are safe: a database lease makes exactly one of them run the background jobs.
# --forwarded-allow-ips='*' is only safe because compose does NOT publish this port (only Caddy can reach it).
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2", "--proxy-headers", "--forwarded-allow-ips", "*"]
