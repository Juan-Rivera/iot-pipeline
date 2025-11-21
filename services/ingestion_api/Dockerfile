FROM --platform=linux/amd64 python:3.13-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./

RUN python3.13 -m venv .venv && \
    .venv/bin/pip install --no-cache-dir -r requirements.txt

COPY app/ ./app

RUN useradd -m -s /bin/bash appuser && \
    chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

CMD [".venv/bin/python3.13", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]