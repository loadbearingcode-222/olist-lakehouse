-- The Olist "application" database: what an e-commerce backend would hold, and what Debezium
-- captures (M1-S2). The replayer (src/olist_lakehouse/replayer) is its only writer.
--
-- Deliberate choices:
-- - Column names are the app's own, clean names (product_name_length, not the source's
--   "lenght"); the raw → app mapping lives in replayer/source.py.
-- - Timestamps are `timestamp` without time zone: Olist times are naive Brazilian local time.
--   `updated_at` is BUSINESS time written by the app; Debezium adds processing time.
-- - Few CHECK constraints, on purpose: a real app lets some bad data through, and the chaos
--   switch (CHAOS_RATE) relies on that so the quality gate (M2-S2) has something to catch.
-- - REPLICA IDENTITY FULL: update and delete events then carry the whole old row, which
--   silver's CDC merge (M3-S1) and the drills can use.

CREATE SCHEMA IF NOT EXISTS olist;
CREATE SCHEMA IF NOT EXISTS replay;

-- ---------------------------------------------------------------- reference data
CREATE TABLE olist.product_category_translation (
    category_name           text PRIMARY KEY,
    category_name_english   text NOT NULL
);

CREATE TABLE olist.zip_geolocation (
    zip_code_prefix     text PRIMARY KEY,
    lat                 double precision NOT NULL,
    lng                 double precision NOT NULL,
    city                text NOT NULL,
    state               text NOT NULL,
    samples             integer NOT NULL
);

CREATE TABLE olist.sellers (
    seller_id           text PRIMARY KEY,
    zip_code_prefix     text NOT NULL,
    city                text NOT NULL,
    state               text NOT NULL,
    updated_at          timestamp NOT NULL
);

CREATE TABLE olist.products (
    product_id                  text PRIMARY KEY,
    category_name               text,
    product_name_length         integer,
    product_description_length  integer,
    product_photos_qty          integer,
    product_weight_g            integer,
    product_length_cm           integer,
    product_height_cm           integer,
    product_width_cm            integer,
    updated_at                  timestamp NOT NULL
);

-- ---------------------------------------------------------------- transactional data
CREATE TABLE olist.customers (
    customer_id         text PRIMARY KEY,
    customer_unique_id  text NOT NULL,
    zip_code_prefix     text NOT NULL,
    city                text NOT NULL,
    state               text NOT NULL,
    updated_at          timestamp NOT NULL
);

CREATE TABLE olist.orders (
    order_id                    text PRIMARY KEY,
    customer_id                 text REFERENCES olist.customers (customer_id),  -- nullable: chaos
    order_status                text NOT NULL,
    purchase_ts                 timestamp NOT NULL,
    approved_at                 timestamp,
    delivered_carrier_at        timestamp,
    delivered_customer_at       timestamp,
    estimated_delivery_at       timestamp,
    updated_at                  timestamp NOT NULL
);

CREATE TABLE olist.order_items (
    order_id            text NOT NULL REFERENCES olist.orders (order_id),
    order_item_id       integer NOT NULL,
    product_id          text NOT NULL REFERENCES olist.products (product_id),
    seller_id           text NOT NULL REFERENCES olist.sellers (seller_id),
    shipping_limit_at   timestamp NOT NULL,
    price               numeric(12, 2) NOT NULL,
    freight_value       numeric(12, 2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE olist.order_payments (
    order_id                text NOT NULL REFERENCES olist.orders (order_id),
    payment_sequential      integer NOT NULL,
    payment_type            text NOT NULL,
    payment_installments    integer NOT NULL,
    payment_value           numeric(12, 2) NOT NULL,
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE olist.order_reviews (
    review_id               text NOT NULL,
    order_id                text NOT NULL REFERENCES olist.orders (order_id),
    review_score            integer NOT NULL,
    review_comment_title    text,
    review_comment_message  text,
    review_creation_at      timestamp NOT NULL,
    review_answer_at        timestamp,
    -- review_id alone is not unique in Olist (814 repeats); the pair is (docs/data/olist.md).
    PRIMARY KEY (review_id, order_id)
);

ALTER TABLE olist.sellers        REPLICA IDENTITY FULL;
ALTER TABLE olist.products       REPLICA IDENTITY FULL;
ALTER TABLE olist.customers      REPLICA IDENTITY FULL;
ALTER TABLE olist.orders         REPLICA IDENTITY FULL;
ALTER TABLE olist.order_items    REPLICA IDENTITY FULL;
ALTER TABLE olist.order_payments REPLICA IDENTITY FULL;
ALTER TABLE olist.order_reviews  REPLICA IDENTITY FULL;

-- ---------------------------------------------------------------- replayer bookkeeping
-- Not captured by CDC (M1-S2 publishes schema `olist` only).

CREATE TABLE replay.checkpoint (
    id              integer PRIMARY KEY CHECK (id = 1),
    fingerprint     text NOT NULL,          -- data manifest + seed + mutation rates
    last_seq        bigint NOT NULL,        -- last applied event, -1 = none
    business_ts     timestamp NOT NULL,     -- business time of that event
    events_total    bigint NOT NULL,
    chaos_rate      double precision NOT NULL,  -- fixed at the first batch; see chaos.py
    updated_wall    timestamptz NOT NULL DEFAULT now()
);

-- Every defect CHAOS_RATE injects, written in the same transaction as the defect itself.
-- M2-S2 reconciles quarantine counts against this, per kind.
CREATE TABLE replay.chaos_ledger (
    seq             bigint PRIMARY KEY,     -- the event that carried the defect
    business_ts     timestamp NOT NULL,
    kind            text NOT NULL,
    table_name      text NOT NULL,
    row_key         jsonb NOT NULL,
    detail          jsonb NOT NULL,
    injected_wall   timestamptz NOT NULL DEFAULT now()
);
