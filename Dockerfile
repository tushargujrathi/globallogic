# syntax=docker/dockerfile:1.7
# One image, several processes: `control-plane api|relay|consumer` and `worker-simulator`.

FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
RUN useradd --create-home --uid 10001 app
WORKDIR /app

# ---------------------------------------------------------------- runtime image
FROM base AS runtime
COPY requirements.lock ./
RUN pip install -r requirements.lock
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-deps .
COPY alembic.ini ./
COPY migrations ./migrations
USER app
EXPOSE 8000
CMD ["control-plane", "api"]

# ------------------------------------------------- dev image: tests + quality gates
FROM base AS dev
COPY requirements-dev.lock ./
RUN pip install -r requirements-dev.lock
COPY --chown=app:app . .
RUN pip install --no-deps -e .
USER app
CMD ["pytest"]
