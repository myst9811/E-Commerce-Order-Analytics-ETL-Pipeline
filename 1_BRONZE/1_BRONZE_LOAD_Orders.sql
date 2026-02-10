USE ROLE INTERN_ROLE;
USE DATABASE INTERN_DB;
USE SCHEMA INTERNS_SANDBOX;
USE WAREHOUSE INTERN_WH;

-- Load Bronze Orders (Incremental)
INSERT INTO SS_BRONZE_ORDERS (
    order_id,
    customer_id,
    order_date,
    order_status,
    total_amount,
    payment_method,
    created_at,
    ingestion_ts,
    source_file,
    row_hash
)
SELECT
    $1::NUMBER           AS order_id,
    $2::NUMBER           AS customer_id,
    $3::DATE             AS order_date,
    $4::STRING           AS order_status,
    $5::NUMBER(12,2)     AS total_amount,
    $6::STRING           AS payment_method,
    $7::TIMESTAMP_NTZ    AS created_at,
    CURRENT_TIMESTAMP    AS ingestion_ts,
    METADATA$FILENAME    AS source_file,
    HASH($1,$2,$3,$4,$5,$6,$7) AS row_hash
FROM @SS_BRONZE_STAGE/orders_day1.csv
WHERE $7::TIMESTAMP_NTZ >
      COALESCE(
          (SELECT last_loaded_ts
           FROM SS_BRONZE_LOAD_WATERMARKS
           WHERE table_name = 'SS_BRONZE_ORDERS'),
          '1900-01-01'
      )
AND HASH($1,$2,$3,$4,$5,$6,$7) NOT IN (
    SELECT row_hash FROM SS_BRONZE_ORDERS
);

-- Advance the Watermark Table for Orders

MERGE INTO SS_BRONZE_LOAD_WATERMARKS t
USING (
    SELECT
        'SS_BRONZE_PRODUCTS' AS table_name,
        MAX(last_updated)    AS last_loaded_ts
    FROM SS_BRONZE_PRODUCTS
) s
ON t.table_name = s.table_name
WHEN MATCHED THEN
    UPDATE SET last_loaded_ts = s.last_loaded_ts
WHEN NOT MATCHED THEN
    INSERT (table_name, last_loaded_ts)
    VALUES (s.table_name, s.last_loaded_ts);
