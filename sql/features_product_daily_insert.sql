-- Build daily product features from marts tables.
-- Source: marts.fct_daily_product_sales JOIN marts.dim_product.
-- Full refresh: caller TRUNCATEs before running this.
--
-- Passthrough columns come directly from the fact table.
-- Derived columns:
--   avg_unit_price: revenue per unit (NULL when quantity is 0)
--   rolling_7d_quantity: 7-day rolling average of daily quantity
--   rolling_7d_revenue: 7-day rolling average of daily revenue
--   days_since_first_seen: days between sale_date and product first appearance
--   distinct_countries: number of countries the product has been sold in (from dim)

INSERT INTO features.product_daily_features (
    stock_code,
    sale_date,
    country,
    total_quantity,
    total_revenue,
    return_quantity,
    transaction_count,
    avg_unit_price,
    rolling_7d_quantity,
    rolling_7d_revenue,
    days_since_first_seen,
    distinct_countries
)
SELECT
    f.stock_code,
    f.sale_date,
    f.country,
    f.total_quantity,
    f.total_revenue,
    f.return_quantity,
    f.transaction_count,
    f.total_revenue / NULLIF(f.total_quantity, 0)
        AS avg_unit_price,
    AVG(f.total_quantity) OVER (
        PARTITION BY f.stock_code, f.country
        ORDER BY f.sale_date
        ROWS 6 PRECEDING
    ) AS rolling_7d_quantity,
    AVG(f.total_revenue) OVER (
        PARTITION BY f.stock_code, f.country
        ORDER BY f.sale_date
        ROWS 6 PRECEDING
    ) AS rolling_7d_revenue,
    f.sale_date - d.first_seen AS days_since_first_seen,
    d.distinct_countries
FROM marts.fct_daily_product_sales f
JOIN marts.dim_product d ON f.stock_code = d.stock_code;
