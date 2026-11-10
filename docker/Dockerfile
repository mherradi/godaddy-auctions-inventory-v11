FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install runtime dependencies
COPY requirements-runtime.txt ./requirements-runtime.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements-runtime.txt

# Copy application code and configuration
COPY collect/ ./collect/
COPY config/ ./config/

# Run the complete pipeline
ENTRYPOINT ["python", "-m", "collect.runner"]