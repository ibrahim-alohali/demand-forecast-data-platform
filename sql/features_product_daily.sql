-- Feature table: daily product features for demand forecasting.
-- Grain: one row per (stock_code, sale_date, country).
-- Derived schema v2. build_features recreates this table transactionally.

CREATE TABLE IF NOT EXISTS features.product_daily_features (
    stock_code          TEXT            NOT NULL,
    sale_date           DATE            NOT NULL,
    country             TEXT            NOT NULL,
    total_quantity      INTEGER         NOT NULL,
    total_revenue       NUMERIC(12, 2)  NOT NULL,
    return_quantity     INTEGER         NOT NULL,
    transaction_count   INTEGER         NOT NULL,
    avg_unit_price      NUMERIC(12, 4),
    lag_1d_quantity     INTEGER,
    lag_7d_quantity     INTEGER,
    rolling_7d_quantity DOUBLE PRECISION,
    rolling_7d_revenue  DOUBLE PRECISION,
    weekday            SMALLINT        NOT NULL,
    days_since_first_seen INTEGER      NOT NULL,
    distinct_countries  INTEGER         NOT NULL
);
