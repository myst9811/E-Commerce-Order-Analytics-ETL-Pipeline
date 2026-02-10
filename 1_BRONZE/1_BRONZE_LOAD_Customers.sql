-- Incremental load for Bronze Customers
INSERT INTO SS_BRONZE_CUSTOMERS (
    customer_id,
    first_name,
    last_name,
    email,
    city,
    country,
    customer_segment,
    last_updated,
    ingestion_ts,
    source_file,
    row_hash
)
SELECT
    $1::NUMBER        AS customer_id,
    $2::STRING        AS first_name,
    $3::STRING        AS last_name,
    $4::STRING        AS email,
    $5::STRING        AS city,
    $6::STRING        AS country,
    $7::STRING        AS customer_segment,
    $8::TIMESTAMP_NTZ AS last_updated,
    CURRENT_TIMESTAMP AS ingestion_ts,
    METADATA$FILENAME AS source_file,
    HASH($1,$2,$3,$4,$5,$6,$7,$8) AS row_hash
FROM @SS_BRONZE_STAGE/customers_day1.csv
WHERE $8::TIMESTAMP_NTZ >
      COALESCE(
          (SELECT last_loaded_ts
           FROM SS_BRONZE_LOAD_WATERMARKS
           WHERE table_name = 'SS_BRONZE_CUSTOMERS'),
          '1900-01-01'
      )
AND HASH($1,$2,$3,$4,$5,$6,$7,$8) NOT IN (
    SELECT row_hash FROM SS_BRONZE_CUSTOMERS
);

-- Advance the Watermark Table 

MERGE INTO SS_BRONZE_LOAD_WATERMARKS t
USING (
    SELECT
        'SS_BRONZE_CUSTOMERS' AS table_name,
        MAX(last_updated)     AS last_loaded_ts
    FROM SS_BRONZE_CUSTOMERS
) s
ON t.table_name = s.table_name
WHEN MATCHED THEN
    UPDATE SET last_loaded_ts = s.last_loaded_ts
WHEN NOT MATCHED THEN
    INSERT (table_name, last_loaded_ts)
    VALUES (s.table_name, s.last_loaded_ts);

/*
Read source file
↓
Filter rows newer than last watermark
↓
Generate row_hash
↓
Exclude already-seen hashes
↓
Insert new rows
↓
Advance watermark
*/