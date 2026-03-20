-- Transform raw.online_retail into staging.stg_online_retail.
-- Full refresh: caller TRUNCATEs staging before running this.
--
-- Transformations applied:
--   1. Deduplication (ROW_NUMBER on all business columns, keep first by loaded_at)
--   2. customer_id: trim → empty to NULL → strip trailing .0 → cast to INTEGER
--   3. description: trim + collapse multiple spaces
--   4. country: trim
--   5. is_return: TRUE when invoice starts with 'C' (cancellation)
--   6. is_stock_item: FALSE for known non-product codes (POST, DOT, etc.)
--   7. Filter: drop rows missing any NOT NULL target column

INSERT INTO staging.stg_online_retail (
    invoice,
    stock_code,
    description,
    quantity,
    invoice_date,
    price,
    customer_id,
    country,
    is_return,
    is_stock_item
)
SELECT
    invoice,
    stock_code,
    REGEXP_REPLACE(TRIM(description), '\s+', ' ', 'g') AS description,
    quantity,
    invoice_date,
    price,
    -- Defensive customer_id cast: trim → empty to NULL → strip .0 → integer
    CASE
        WHEN NULLIF(TRIM(customer_id), '') IS NOT NULL
        THEN CAST(
            REGEXP_REPLACE(TRIM(customer_id), '\.0$', '') AS INTEGER
        )
        ELSE NULL
    END AS customer_id,
    TRIM(country) AS country,
    (invoice LIKE 'C%') AS is_return,
    (TRIM(UPPER(stock_code)) NOT IN (
        'POST', 'DOT', 'D', 'M', 'B', 'BANK CHARGES',
        'PADS', 'AMAZONFEE', 'C2', 'CRUK'
    )) AS is_stock_item
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY invoice, stock_code, description, quantity,
                            invoice_date, price, customer_id, country
               ORDER BY loaded_at
           ) AS rn
    FROM raw.online_retail
    WHERE invoice IS NOT NULL
      AND stock_code IS NOT NULL
      AND invoice_date IS NOT NULL
      AND quantity IS NOT NULL
      AND price IS NOT NULL
      AND country IS NOT NULL
) deduped
WHERE rn = 1;
