-- Build product dimension from staging line items.
-- Source: staging.stg_online_retail (is_stock_item = true only).
-- Full refresh: caller TRUNCATEs before running this.
--
-- description: most recent non-null description per stock_code,
-- tie-broken by invoice_date DESC then staged_at DESC.

INSERT INTO marts.dim_product (
    stock_code,
    description,
    first_seen,
    last_seen,
    distinct_countries
)
SELECT
    s.stock_code,
    d.description,
    MIN(s.invoice_date)::date AS first_seen,
    MAX(s.invoice_date)::date AS last_seen,
    COUNT(DISTINCT s.country) AS distinct_countries
FROM staging.stg_online_retail s
LEFT JOIN (
    -- Pick most recent non-null description per stock_code
    SELECT DISTINCT ON (stock_code)
        stock_code,
        description
    FROM staging.stg_online_retail
    WHERE is_stock_item
      AND description IS NOT NULL
    ORDER BY stock_code, invoice_date DESC, staged_at DESC
) d ON s.stock_code = d.stock_code
WHERE s.is_stock_item
GROUP BY s.stock_code, d.description;
