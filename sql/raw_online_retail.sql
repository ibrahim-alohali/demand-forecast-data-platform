-- Raw table for UCI Online Retail II dataset.
-- Preserves source columns with minimal transformation.
-- See: https://archive.ics.uci.edu/dataset/502/online+retail+ii

CREATE TABLE IF NOT EXISTS raw.online_retail (
    invoice         TEXT,
    stock_code      TEXT,
    description     TEXT,
    quantity        INTEGER,
    invoice_date    TIMESTAMP,
    price           NUMERIC(10, 4),
    customer_id     TEXT,
    country         TEXT,
    source_file     TEXT NOT NULL,
    source_sheet    TEXT NOT NULL,
    loaded_at       TIMESTAMP NOT NULL DEFAULT now()
);
