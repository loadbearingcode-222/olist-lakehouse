#!/bin/sh
# One-shot: migrate Superset's metadata DB, ensure the admin exists, and import the committed
# dashboards (superset/dashboards: an export bundle; database, datasets, charts, dashboards).
# Idempotent: imports overwrite by UUID, so every `up` converges on what git holds.
set -eu
superset db upgrade
superset fab create-admin --username admin --password "$SUPERSET_ADMIN_PASSWORD" \
  --firstname olist --lastname admin --email admin@example.invalid || true
superset init
if [ -f /bundle/metadata.yaml ]; then
  rm -f /tmp/dashboards.zip
  (cd /bundle/.. && python -m zipfile -c /tmp/dashboards.zip "$(basename /bundle)")
  superset import-dashboards --path /tmp/dashboards.zip --username admin
  echo "superset-init: dashboards imported from the committed bundle"
else
  echo "superset-init: no committed bundle yet (build it with scripts/superset_dashboards.py)"
fi
