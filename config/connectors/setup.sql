-- CDC prerequisites on postgres-source. Idempotent: the `cdc-setup` one-shot runs it on every
-- `up`, and init scripts cannot do it (they only run on a fresh volume).
--
-- - A dedicated `debezium` role: LOGIN + REPLICATION + read-only on schema olist. The connector
--   never uses the owner account.
-- - The publication is created here, by the owner, and the connector is told not to create
--   one (publication.autocreate.mode=disabled): creating a publication needs table ownership,
--   which a read-only CDC role should not have.
-- - The publication covers schema `olist` only, so replay.* bookkeeping is never captured.
\set ON_ERROR_STOP on

SELECT format('CREATE ROLE debezium LOGIN REPLICATION PASSWORD %L', :'debezium_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') \gexec

ALTER ROLE debezium WITH LOGIN REPLICATION PASSWORD :'debezium_password';
GRANT CONNECT ON DATABASE olist TO debezium;
GRANT USAGE ON SCHEMA olist TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA olist TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA olist GRANT SELECT ON TABLES TO debezium;

SELECT 'CREATE PUBLICATION olist_cdc FOR TABLES IN SCHEMA olist'
WHERE NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'olist_cdc') \gexec

SELECT 'cdc-setup: role debezium and publication olist_cdc ready' AS status;
