-- Staging table for cleaned Online Retail II data.
-- Source: raw.online_retail
-- Transformations: type casting, dedup, text cleanup, derived flags.

CREATE TABLE IF NOT EXISTS staging.stg_online_retail (
    invoice         TEXT            NOT NULL,
    stock_code      TEXT            NOT NULL,
    description     TEXT,
    quantity        INTEGER         NOT NULL,
    invoice_date    TIMESTAMP       NOT NULL,
    price           NUMERIC(10, 4)  NOT NULL,
    customer_id     INTEGER,
    country         TEXT            NOT NULL,
    is_return       BOOLEAN         NOT NULL,
    is_stock_item   BOOLEAN         NOT NULL,
    staged_at       TIMESTAMP       NOT NULL DEFAULT now()
);
