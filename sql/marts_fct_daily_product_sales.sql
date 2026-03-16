-- Fact table: daily product sales aggregated from staging.
-- Grain: one row per (stock_code, sale_date, country).
-- Only includes stock items (is_stock_item = true).

CREATE TABLE IF NOT EXISTS marts.fct_daily_product_sales (
    stock_code        TEXT           NOT NULL,
    sale_date         DATE           NOT NULL,
    country           TEXT           NOT NULL,
    total_quantity    INTEGER        NOT NULL,
    total_revenue     NUMERIC(12, 2) NOT NULL,
    transaction_count INTEGER        NOT NULL,
    return_quantity   INTEGER        NOT NULL,
    return_revenue    NUMERIC(12, 2) NOT NULL,
    return_count      INTEGER        NOT NULL
);
