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

-- Advance the Watermark Table for Products
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
