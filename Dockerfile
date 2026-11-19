FROM python:3.13-slim

WORKDIR /app

# Install system dependencies if required
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY migrations/ ./migrations/
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY tests/ ./tests/
COPY pytest.ini .

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]

