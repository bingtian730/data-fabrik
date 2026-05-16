-- Load demo CSV data into the raw schema.
-- Run via:
--   docker compose cp demo/data/customers.csv postgres:/tmp/customers.csv
--   docker compose cp demo/data/invoices.csv postgres:/tmp/invoices.csv
--   docker compose cp demo/data/customer_orders.csv postgres:/tmp/customer_orders.csv
--   docker compose exec -T postgres psql -U datafabrik -d datafabrik -f /dev/stdin < scripts/load-demo-csv.sql

CREATE SCHEMA IF NOT EXISTS raw;

-- ── customers ────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS raw.customers CASCADE;
CREATE TABLE raw.customers (
    customer_id   INTEGER,
    first_name    TEXT,
    last_name     TEXT,
    email         TEXT,
    phone         TEXT,
    status        TEXT,
    created_at    TEXT,
    updated_at    TIMESTAMPTZ DEFAULT now()
);

\COPY raw.customers (customer_id, first_name, last_name, email, phone, status, created_at)
  FROM '/tmp/customers.csv' CSV HEADER;

-- ── invoices ─────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS raw.invoices CASCADE;
CREATE TABLE raw.invoices (
    invoice_id    TEXT,
    customer_id   INTEGER,
    amount        TEXT,
    currency      TEXT,
    status        TEXT,
    issue_date    TEXT,
    paid_at       TEXT,
    updated_at    TIMESTAMPTZ DEFAULT now()
);

\COPY raw.invoices (invoice_id, customer_id, amount, currency, status, issue_date, paid_at)
  FROM '/tmp/invoices.csv' CSV HEADER;

-- ── customer_orders ──────────────────────────────────────────────────────────
DROP TABLE IF EXISTS raw.customer_orders CASCADE;
CREATE TABLE raw.customer_orders (
    order_id      TEXT,
    customer_id   INTEGER,
    invoice_id    TEXT,
    product_name  TEXT,
    quantity      TEXT,
    unit_price    TEXT,
    status        TEXT,
    ordered_at    TEXT,
    updated_at    TIMESTAMPTZ DEFAULT now()
);

\COPY raw.customer_orders (order_id, customer_id, invoice_id, product_name, quantity, unit_price, status, ordered_at)
  FROM '/tmp/customer_orders.csv' CSV HEADER;

SELECT 'raw.customers' AS table_name, count(*) FROM raw.customers
UNION ALL
SELECT 'raw.invoices',      count(*) FROM raw.invoices
UNION ALL
SELECT 'raw.customer_orders', count(*) FROM raw.customer_orders;
