-- Dimension uses the same paid sales/return eligibility as the fact table.
-- Latest non-empty description wins; same-time ties use lexical description.
WITH eligible AS (
    SELECT * FROM staging.stg_online_retail
    WHERE is_stock_item AND price > 0
      AND ((NOT is_return AND quantity > 0) OR (is_return AND quantity < 0))
)
INSERT INTO marts.dim_product (
    stock_code, description, first_seen, last_seen, distinct_countries
)
SELECT s.stock_code, d.description,
    MIN(s.invoice_date)::date, MAX(s.invoice_date)::date, COUNT(DISTINCT s.country)
FROM eligible s
LEFT JOIN (
    SELECT DISTINCT ON (stock_code) stock_code, description
    FROM eligible WHERE description IS NOT NULL
    ORDER BY stock_code, invoice_date DESC, description
) d ON s.stock_code = d.stock_code
GROUP BY s.stock_code, d.description;
