# Build stage
FROM python:3.13-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gcc \
        libxml2-dev \
        libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
ARG UID=1000
ARG GID=1000
RUN groupadd -g ${GID} appgroup && \
    useradd -m -u ${UID} -g ${GID} appuser

# Install Python dependencies
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN pip install --upgrade pip && \
    pip install build && \
    python -m build

# Runtime stage
FROM python:3.13-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        mkvtoolnix \
        mediainfo \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
ARG UID=1000
ARG GID=1000
RUN groupadd -g ${GID} appgroup && \
    useradd -m -u ${UID} -g ${GID} appuser

# Install the built package
COPY --from=builder /app/dist/*.whl /tmp/
RUN pip install /tmp/*.whl && rm /tmp/*.whl

# Set up directories
ARG MEDIA_ROOT=/media
ARG CACHE_DIR=/app/.cache
ENV MEDIA_ROOT=${MEDIA_ROOT}
ENV CACHE_DIR=${CACHE_DIR}

RUN mkdir -p ${MEDIA_ROOT} ${CACHE_DIR} && \
    chown -R ${UID}:${GID} ${MEDIA_ROOT} ${CACHE_DIR}

USER appuser

# Expose volumes
VOLUME ["/media", "/app/.cache"]

# Default command
ENTRYPOINT ["media-archive-sync"]
CMD ["--help"]