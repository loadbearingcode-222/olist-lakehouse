# Superset 6.1.0 with the drivers this lake needs: Trino (the data) and Postgres (its own
# metadata, in platform-db). The trino client matches uv.lock.
FROM apache/superset:6.1.0

USER root
RUN uv pip install --python /app/.venv/bin/python --no-cache \
        "trino[sqlalchemy]==0.340.0" \
        "psycopg2-binary==2.9.13"
COPY docker/superset/superset_config.py /app/pythonpath/superset_config.py
COPY docker/superset/init.sh /app/olist-init.sh
USER superset
