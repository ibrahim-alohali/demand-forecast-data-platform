-- Aggregate staging line items into daily product sales.
-- Source: staging.stg_online_retail (is_stock_item = true only).
-- Full refresh: caller TRUNCATEs before running this.
--
-- Sales (is_return = false) and returns (is_return = true) are aggregated
-- separately within the same grain row. A group with only returns and
-- zero sales is valid.

INSERT INTO marts.fct_daily_product_sales (
    stock_code,
    sale_date,
    country,
    total_quantity,
    total_revenue,
    transaction_count,
    return_quantity,
    return_revenue,
    return_count
)
SELECT
    stock_code,
    invoice_date::date AS sale_date,
    country,
    COALESCE(SUM(quantity) FILTER (WHERE NOT is_return), 0)
        AS total_quantity,
    COALESCE(SUM(quantity * price) FILTER (WHERE NOT is_return), 0)
        AS total_revenue,
    COALESCE(COUNT(DISTINCT invoice) FILTER (WHERE NOT is_return), 0)
        AS transaction_count,
    COALESCE(SUM(ABS(quantity)) FILTER (WHERE is_return), 0)
        AS return_quantity,
    COALESCE(SUM(ABS(quantity * price)) FILTER (WHERE is_return), 0)
        AS return_revenue,
    COALESCE(COUNT(DISTINCT invoice) FILTER (WHERE is_return), 0)
        AS return_count
FROM staging.stg_online_retail
WHERE is_stock_item
GROUP BY stock_code, invoice_date::date, country;
