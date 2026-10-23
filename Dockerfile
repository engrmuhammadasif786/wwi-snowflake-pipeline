FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    PYTHONPATH=/opt/dagster/app

WORKDIR /opt/dagster/app

# Install system dependencies required for building connectors & running git/dbt
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . /opt/dagster/app

# Ensure Dagster home & local data directories exist and install dbt packages
RUN mkdir -p ${DAGSTER_HOME} /opt/dagster/app/data/raw /opt/dagster/app/data/exports && \
    cp /opt/dagster/app/dagster.yaml ${DAGSTER_HOME}/dagster.yaml && \
    dbt deps --project-dir /opt/dagster/app/dbt_wwi --profiles-dir /opt/dagster/app/dbt_wwi

EXPOSE 3000

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "dagster_wwi.definitions"]
