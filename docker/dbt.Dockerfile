FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV GOOGLE_CLOUD_PROJECT=godaddy-auctions-inventory-v11

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
       dbt-core==1.12.6 \
       dbt-bigquery==1.12.2

COPY dbt/ ./dbt/

RUN mkdir -p /root/.dbt
COPY docker/profiles/profiles.yml /root/.dbt/profiles.yml

WORKDIR /app/dbt

ENTRYPOINT ["dbt"]