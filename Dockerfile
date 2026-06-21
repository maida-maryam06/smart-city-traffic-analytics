# Multi-stage keeps the final image lean
FROM python:3.11-slim AS base

# System deps needed to compile mysqlclient / bcrypt C extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        pkg-config \
        default-libmysqlclient-dev \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (layer-cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

EXPOSE 5000

# Use a non-root user for security
RUN useradd -m appuser && chown -R appuser /app
USER appuser

CMD ["python", "app.py"]
