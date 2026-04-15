# syntax=docker/dockerfile:1.6
# OCI-compliant build — works with docker build, podman build, buildah, buildkit

ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim AS base

# --- OCI image labels (https://github.com/opencontainers/image-spec/blob/main/annotations.md) ---
ARG IMAGE_VERSION=1.0.0
ARG IMAGE_REVISION=local
ARG IMAGE_CREATED
LABEL org.opencontainers.image.title="cloudera-smoke-runner" \
      org.opencontainers.image.description="Orchestrates Cloudera CDP smoke/health tests by launching an ephemeral EC2 test runner inside the cluster VPC and collecting results in Nagios, Prometheus, JSON, and HTML formats." \
      org.opencontainers.image.version="${IMAGE_VERSION}" \
      org.opencontainers.image.revision="${IMAGE_REVISION}" \
      org.opencontainers.image.created="${IMAGE_CREATED}" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://example.invalid/cloudera-smoke-runner" \
      org.opencontainers.image.vendor="smoke-runner" \
      org.opencontainers.image.base.name="docker.io/library/python:${PYTHON_VERSION}-slim"

# --- stage 1: install AWS CLI v2 (kept separate so runtime stage stays small) ---
FROM base AS awscli
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl unzip ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN set -eux; \
    ARCH="$(uname -m)"; \
    curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-${ARCH}.zip" -o /tmp/awscliv2.zip; \
    unzip -q /tmp/awscliv2.zip -d /tmp; \
    /tmp/aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli; \
    rm -rf /tmp/awscliv2.zip /tmp/aws

# --- stage 2: runtime ---
FROM base AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates dumb-init jq \
    && rm -rf /var/lib/apt/lists/*

COPY --from=awscli /usr/local/aws-cli /usr/local/aws-cli
COPY --from=awscli /usr/local/bin/aws /usr/local/bin/aws
COPY --from=awscli /usr/local/bin/aws_completer /usr/local/bin/aws_completer

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY smoke.py remote_probe.py report.py entrypoint.sh ./
RUN chmod 0755 entrypoint.sh smoke.py

# Non-root user for OCI best practice
ARG APP_UID=10001
ARG APP_GID=10001
RUN groupadd -g "${APP_GID}" smoke \
    && useradd -u "${APP_UID}" -g "${APP_GID}" -m -s /usr/sbin/nologin smoke \
    && mkdir -p /output \
    && chown -R smoke:smoke /app /output

USER smoke:smoke
ENV OUTPUT_DIR=/output \
    HOME=/home/smoke \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

VOLUME ["/output"]

# dumb-init handles PID 1 signal forwarding (SIGTERM → graceful teardown)
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/app/entrypoint.sh"]
CMD []
