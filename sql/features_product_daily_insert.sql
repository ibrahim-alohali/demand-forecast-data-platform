-- Calendar-day forecasts of RECORDED gross sales, not unobserved demand.
-- Generate only observed product-country pairs, from first appearance through
-- the common dataset end. Do not stop at a pair's last sale (future knowledge).
-- Missing transaction days mean zero recorded sales; stock availability is unknown.
INSERT INTO features.product_daily_features (
    stock_code, sale_date, country, total_quantity, total_revenue,
    return_quantity, transaction_count, avg_unit_price,
    lag_1d_quantity, lag_7d_quantity, rolling_7d_quantity, rolling_7d_revenue,
    weekday, days_since_first_seen, distinct_countries
)
WITH series AS (
    SELECT stock_code, country, MIN(sale_date) AS first_date
    FROM marts.fct_daily_product_sales
    GROUP BY stock_code, country
), calendar AS (
    SELECT s.stock_code, s.country, s.first_date + day_offset AS sale_date
    FROM series s
    CROSS JOIN (SELECT MAX(sale_date) AS last_date
                FROM marts.fct_daily_product_sales) bounds
    CROSS JOIN LATERAL generate_series(0, bounds.last_date - s.first_date) day_offset
), daily AS (
    SELECT c.stock_code, c.country, c.sale_date,
           COALESCE(f.total_quantity, 0) AS total_quantity,
           COALESCE(f.total_revenue, 0) AS total_revenue,
           COALESCE(f.return_quantity, 0) AS return_quantity,
           COALESCE(f.transaction_count, 0) AS transaction_count
    FROM calendar c
    LEFT JOIN marts.fct_daily_product_sales f
      USING (stock_code, country, sale_date)
), history AS (
    SELECT *,
           LAG(total_quantity, 1) OVER series_days AS lag_1d_quantity,
           LAG(total_quantity, 7) OVER series_days AS lag_7d_quantity,
           COUNT(*) OVER prior_week AS history_days,
           AVG(total_quantity) OVER prior_week AS prior_quantity_mean,
           AVG(total_revenue) OVER prior_week AS prior_revenue_mean
    FROM daily
    WINDOW series_days AS (PARTITION BY stock_code, country ORDER BY sale_date),
           prior_week AS (PARTITION BY stock_code, country ORDER BY sale_date
                          ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)
)
SELECT h.stock_code, h.sale_date, h.country, h.total_quantity, h.total_revenue,
       h.return_quantity, h.transaction_count,
       h.total_revenue / NULLIF(h.total_quantity, 0),
       h.lag_1d_quantity, h.lag_7d_quantity,
       CASE WHEN h.history_days = 7 THEN h.prior_quantity_mean END,
       CASE WHEN h.history_days = 7 THEN h.prior_revenue_mean END,
       EXTRACT(ISODOW FROM h.sale_date) - 1,
       h.sale_date - d.first_seen,
       d.distinct_countries
FROM history h
JOIN marts.dim_product d ON h.stock_code = d.stock_code;
