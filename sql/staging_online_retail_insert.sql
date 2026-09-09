-- Exact source-business duplicates are removed before text normalization.
-- Missing core values and negative prices are excluded; the raw layer is intact.
-- Malformed/non-positive/out-of-range customer IDs become NULL, with a count.
-- Zero-price lines and quantity/cancellation disagreements remain inspectable here.
WITH classified AS (
    SELECT *,
        (NULLIF(BTRIM(invoice, E' \t\r\n'), '') IS NOT NULL
         AND NULLIF(BTRIM(stock_code, E' \t\r\n'), '') IS NOT NULL
         AND NULLIF(BTRIM(country, E' \t\r\n'), '') IS NOT NULL
         AND invoice_date IS NOT NULL AND quantity IS NOT NULL
         AND price IS NOT NULL) AS core_complete,
        CASE WHEN BTRIM(customer_id) ~ '^[0-9]{1,10}(\.0+)?$'
             THEN CASE WHEN BTRIM(customer_id)::numeric BETWEEN 1 AND 2147483647
                       THEN BTRIM(customer_id)::numeric::integer END
        END AS clean_customer_id
    FROM raw.online_retail
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY invoice, stock_code, description, quantity,
                     invoice_date, price, customer_id, country
        ORDER BY loaded_at
    ) AS rn
    FROM classified
    WHERE core_complete AND price >= 0
), inserted AS (
    INSERT INTO staging.stg_online_retail (
        invoice, stock_code, description, quantity, invoice_date, price,
        customer_id, country, is_return, is_stock_item
    )
    SELECT BTRIM(invoice, E' \t\r\n'), BTRIM(stock_code, E' \t\r\n'),
        NULLIF(BTRIM(REGEXP_REPLACE(description, '\s+', ' ', 'g')), ''),
        quantity, invoice_date, price, clean_customer_id,
        BTRIM(country, E' \t\r\n'),
        (UPPER(BTRIM(invoice, E' \t\r\n')) LIKE 'C%'),
        (UPPER(BTRIM(stock_code, E' \t\r\n')) NOT IN (
            'POST', 'DOT', 'D', 'M', 'B', 'BANK CHARGES',
            'PADS', 'AMAZONFEE', 'C2', 'CRUK'
        ))
    FROM ranked WHERE rn = 1
    RETURNING 1
)
SELECT
    (SELECT COUNT(*) FROM classified) AS raw_rows,
    (SELECT COUNT(*) FROM classified WHERE NOT core_complete) AS missing_core,
    (SELECT COUNT(*) FROM classified WHERE core_complete AND price < 0) AS negative_price,
    (SELECT COUNT(*) FROM ranked WHERE rn > 1) AS exact_duplicates,
    (SELECT COUNT(*) FROM ranked WHERE rn = 1
       AND NULLIF(BTRIM(customer_id), '') IS NOT NULL
       AND clean_customer_id IS NULL) AS invalid_customer_ids,
    (SELECT COUNT(*) FROM inserted) AS staging_rows;
