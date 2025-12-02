FROM --platform=linux/amd64 python:3.13-slim-bookworm AS base

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Create user early
RUN useradd -m appuser

# Create and activate virtual environment
ENV VIRTUAL_ENV=/app/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY pyproject.toml .
COPY README.md .

# Install dependencies into the virtual environment
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

# Copy application files with correct ownership
COPY --chown=appuser:appuser src ./src
COPY --chown=appuser:appuser kcl.properties .
COPY --chown=appuser:appuser run.sh .

RUN chmod +x run.sh

# Ensure appuser owns everything in /app (including the venv)
RUN chown -R appuser:appuser /app

USER appuser

CMD ["./run.sh"]
