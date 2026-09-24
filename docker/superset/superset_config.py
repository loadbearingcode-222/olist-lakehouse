"""Superset configuration for olist-lakehouse (read from /app/pythonpath)."""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://platform:{os.environ['PLATFORM_DB_PASSWORD']}@platform-db:5432/superset"
)
# Local demo: no HTTPS in front of it, so Talisman's HTTPS-only headers would break the UI.
TALISMAN_ENABLED = False
ROW_LIMIT = 50_000
SQLLAB_TIMEOUT = 120
FEATURE_FLAGS = {"DASHBOARD_NATIVE_FILTERS": True}
