# The project's Python services: the replayer (M1-S1) and the clickstream producer (M1-S3).
# Built from the repo root (compose: context "."), installed from uv.lock exactly.
FROM python:3.12.14-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.11.25 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1

# The user exists before anything is installed: a chown afterwards would copy every layer.
RUN useradd --create-home --uid 10001 app

WORKDIR /app
# Dependencies first, so a source change does not reinstall them.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev

USER app
ENTRYPOINT ["python", "-m"]
CMD ["olist_lakehouse.replayer"]
