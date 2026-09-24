#!/bin/sh
# Bootstrap Lakekeeper and create the `lakehouse` warehouse. Safe to run on every `up`:
# each step reads the current state first and only writes what is missing.
#
# Request shapes follow Lakekeeper v0.13.6's own management OpenAPI
# (`lakekeeper management-openapi`), not older blog examples:
#   credential fields are `access-key-id` / `secret-access-key`, flavor is `s3-compat`.
set -eu

LK=http://lakekeeper:8181
WAREHOUSE=lakehouse

if curl -fsS "$LK/management/v1/info" | grep -q '"bootstrapped":true'; then
  echo "bootstrap: already bootstrapped"
else
  curl -fsS -X POST "$LK/management/v1/bootstrap" \
    -H 'Content-Type: application/json' \
    -d '{"accept-terms-of-use": true}'
  echo "bootstrap: server bootstrapped"
fi

if curl -fsS "$LK/management/v1/warehouse" | grep -q "\"name\":\"$WAREHOUSE\""; then
  echo "bootstrap: warehouse '$WAREHOUSE' already exists"
  exit 0
fi

# STS and remote signing are off: MinIO here has no STS, and every engine (Trino, Spark)
# brings its own S3 credentials. Hard deletes keep drop-and-recreate predictable in a demo.
curl -fsS -X POST "$LK/management/v1/warehouse" \
  -H 'Content-Type: application/json' \
  -d @- <<EOF
{
  "warehouse-name": "$WAREHOUSE",
  "storage-profile": {
    "type": "s3",
    "bucket": "lakehouse",
    "key-prefix": "warehouse",
    "endpoint": "http://minio:9000",
    "region": "local-01",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": false,
    "remote-signing-enabled": false
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "access-key-id": "$MINIO_ROOT_USER",
    "secret-access-key": "$MINIO_ROOT_PASSWORD"
  },
  "delete-profile": {"type": "hard"}
}
EOF
echo
echo "bootstrap: warehouse '$WAREHOUSE' created"
