FROM --platform=linux/amd64 python:3.13-slim-bookworm AS base

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY README.md .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

COPY src ./src

RUN useradd -m appuser
USER appuser

CMD ["python", "src/main.py"]
