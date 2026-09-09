-- One row per product/date/country for recorded paid sales and cancellations.
-- Paid sales: positive quantity, positive price, non-C invoice.
-- Returns: negative quantity, positive price, C-prefixed cancellation invoice.
-- Exclude non-stock codes, zero-price lines (meaning unknown), non-C negative
-- adjustments, zero quantities and positive C lines; never infer their demand.
INSERT INTO marts.fct_daily_product_sales (
    stock_code, sale_date, country, total_quantity, total_revenue,
    transaction_count, return_quantity, return_revenue, return_count
)
SELECT stock_code, invoice_date::date, country,
    COALESCE(SUM(quantity) FILTER (WHERE NOT is_return), 0),
    COALESCE(SUM(quantity * price) FILTER (WHERE NOT is_return), 0),
    COUNT(DISTINCT invoice) FILTER (WHERE NOT is_return),
    COALESCE(SUM(-quantity) FILTER (WHERE is_return), 0),
    COALESCE(SUM(-quantity * price) FILTER (WHERE is_return), 0),
    COUNT(DISTINCT invoice) FILTER (WHERE is_return)
FROM staging.stg_online_retail
WHERE is_stock_item AND price > 0
  AND ((NOT is_return AND quantity > 0) OR (is_return AND quantity < 0))
GROUP BY stock_code, invoice_date::date, country;
