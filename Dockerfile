# syntax=docker/dockerfile:1.7
# Binance Crypto Module — All-in-One Dockerfile
# This Dockerfile packages all 5 Binance microservices into a single container 
# using supervisord. This is perfect for single-instance cloud deployments (Render, VPS).

FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies including supervisord
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential ca-certificates tini curl supervisor \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10200 --no-create-home binance

WORKDIR /opt/binance

# Copy the Binance python module source code
COPY python/binance_module/pyproject.toml /opt/binance/pyproject.toml
COPY python/binance_module/README.md      /opt/binance/README.md
COPY python/binance_module/src            /opt/binance/src

# Install the Python package globally
RUN pip install --no-cache-dir .

# Create the supervisord configuration file to run all 5 services concurrently
RUN echo "[supervisord]" > /etc/supervisor/conf.d/supervisord.conf && \
    echo "nodaemon=true" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "user=binance" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "[program:binance-feed]" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "command=binance-feed" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stdout_logfile=/dev/stdout" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stdout_logfile_maxbytes=0" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stderr_logfile=/dev/stderr" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stderr_logfile_maxbytes=0" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "[program:binance-risk]" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "command=binance-risk" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stdout_logfile=/dev/stdout" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stdout_logfile_maxbytes=0" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stderr_logfile=/dev/stderr" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stderr_logfile_maxbytes=0" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "[program:binance-strategy]" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "command=binance-strategy" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stdout_logfile=/dev/stdout" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stdout_logfile_maxbytes=0" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stderr_logfile=/dev/stderr" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stderr_logfile_maxbytes=0" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "[program:binance-exec]" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "command=binance-exec" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stdout_logfile=/dev/stdout" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stdout_logfile_maxbytes=0" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stderr_logfile=/dev/stderr" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stderr_logfile_maxbytes=0" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "[program:binance-position]" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "command=binance-position" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stdout_logfile=/dev/stdout" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stdout_logfile_maxbytes=0" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stderr_logfile=/dev/stderr" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stderr_logfile_maxbytes=0" >> /etc/supervisor/conf.d/supervisord.conf

RUN chown -R binance:binance /opt/binance /etc/supervisor/conf.d

USER binance

# Expose Prometheus metric ports
EXPOSE 9300 9301 9302 9303 9304

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
