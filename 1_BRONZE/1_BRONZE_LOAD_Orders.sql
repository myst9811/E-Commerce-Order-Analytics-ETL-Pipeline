-- Load Bronze Orders
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
FROM @SS_BRONZE_STAGE/orders_day1.csv;
