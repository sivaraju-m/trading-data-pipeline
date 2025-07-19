# Multi-stage Docker build for Trading Data Pipeline
FROM python:3.11-slim as builder

# Set build arguments
ARG BUILD_DATE
ARG VCS_REF
ARG VERSION=latest

# Add metadata
LABEL maintainer="AI Trading Machine Team" \
      org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.version=$VERSION \
      org.label-schema.name="trading-data-pipeline" \
      org.label-schema.description="Production-ready trading data pipeline for cloud deployment"

# Install system dependencies including TA-Lib C library
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    wget \
    build-essential \
    autotools-dev \
    automake \
    && wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz \
    && tar -xzf ta-lib-0.4.0-src.tar.gz \
    && cd ta-lib/ \
    && wget -O config.guess 'https://git.savannah.gnu.org/gitweb/?p=config.git;a=blob_plain;f=config.guess;hb=HEAD' \
    && wget -O config.sub 'https://git.savannah.gnu.org/gitweb/?p=config.git;a=blob_plain;f=config.sub;hb=HEAD' \
    && chmod +x config.guess config.sub \
    && ./configure --prefix=/usr \
    && make \
    && make install \
    && cd .. \
    && rm -rf ta-lib ta-lib-0.4.0-src.tar.gz \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt requirements-dev.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Production stage
FROM python:3.11-slim as production

# Install runtime dependencies including TA-Lib runtime
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash app

# Copy Python packages and TA-Lib from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /usr/lib /usr/lib
COPY --from=builder /usr/include /usr/include

# Set working directory
WORKDIR /app

# Copy application code
COPY . .

# Install the package requirements without the package itself to avoid conflicts
RUN pip install --no-cache-dir -r requirements.txt

# Change ownership to app user
RUN chown -R app:app /app

# Switch to non-root user
USER app

# Set environment variables
ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    GOOGLE_APPLICATION_CREDENTIALS=/app/config/secrets/gcp-service-account.json

# Expose port for health checks
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/status || exit 1

# Default command for cloud deployment - start the Flask web service
CMD ["gunicorn", "--config", "gunicorn.conf.py", "main:app"]
