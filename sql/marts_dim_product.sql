-- Dimension table: one row per product (stock_code).
-- Only includes stock items (is_stock_item = true).

CREATE TABLE IF NOT EXISTS marts.dim_product (
    stock_code         TEXT    NOT NULL PRIMARY KEY,
    description        TEXT,
    first_seen         DATE    NOT NULL,
    last_seen          DATE    NOT NULL,
    distinct_countries INTEGER NOT NULL
);
