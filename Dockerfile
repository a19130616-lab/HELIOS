# Multi-stage Dockerfile for Helios Trading System
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r helios && useradd -r -g helios helios

# Set working directory
WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Production stage
FROM base as production

# Copy application code
COPY app/ ./app/
COPY config/ ./config/
COPY main.py .

# Create necessary directories
RUN mkdir -p logs models && \
    chown -R helios:helios /app

# Switch to non-root user
USER helios

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import redis; r = redis.Redis(host='redis', port=6379, db=0); r.ping()" || exit 1

# Expose port (if needed for future web interface)
EXPOSE 8080

# Default command
CMD ["python", "main.py"]

# Development stage (for debugging)
FROM base as development

# Install development dependencies
RUN pip install --no-cache-dir \
    pytest>=6.0.0 \
    pytest-asyncio>=0.15.0 \
    black>=21.0.0 \
    flake8>=3.9.0 \
    ipython>=7.0.0

# Copy application code
COPY . .

# Create directories
RUN mkdir -p logs models && \
    chown -R helios:helios /app

# Switch to non-root user
USER helios

# Development command
CMD ["python", "main.py"]