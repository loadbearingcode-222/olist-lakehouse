# Airflow 3 for the batch side (M4): the stock image, plus the project in an ISOLATED venv.
# Airflow's own dependencies stay exactly as the image ships them. Tasks run the project's
# code (fx, GX checkpoint, dbt) with /opt/olist-venv, built from uv.lock like every other image.
FROM apache/airflow:3.3.2-python3.12

USER root
COPY --from=ghcr.io/astral-sh/uv:0.11.25 /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/olist-venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /opt/olist
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --group integration --group dbt --group quality --no-install-project
COPY src ./src
COPY tests/dags ./tests/dags
RUN uv sync --locked --no-dev --group integration --group dbt --group quality \
    && chown -R airflow:0 /opt/olist /opt/olist-venv

USER airflow
WORKDIR /opt/airflow
