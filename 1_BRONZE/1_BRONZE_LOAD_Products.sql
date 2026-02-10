INSERT INTO SS_BRONZE_PRODUCTS (
    product_id,
    product_name,
    category,
    price,
    cost,
    supplier_id,
    last_updated,
    ingestion_ts,
    source_file,
    row_hash
)
SELECT
    $1::NUMBER           AS product_id,
    $2::STRING           AS product_name,
    $3::STRING           AS category,
    $4::NUMBER(12,2)     AS price,
    $5::NUMBER(12,2)     AS cost,
    $6::NUMBER           AS supplier_id,
    $7::TIMESTAMP_NTZ    AS last_updated,
    CURRENT_TIMESTAMP    AS ingestion_ts,
    METADATA$FILENAME    AS source_file,
    HASH($1,$2,$3,$4,$5,$6,$7) AS row_hash
FROM @SS_BRONZE_STAGE/products_day1.csv
WHERE $7::TIMESTAMP_NTZ >
      COALESCE(
          (SELECT last_loaded_ts
           FROM SS_BRONZE_LOAD_WATERMARKS
           WHERE table_name = 'SS_BRONZE_PRODUCTS'),
          '1900-01-01'
      )
AND HASH($1,$2,$3,$4,$5,$6,$7) NOT IN (
    SELECT row_hash FROM SS_BRONZE_PRODUCTS
);
