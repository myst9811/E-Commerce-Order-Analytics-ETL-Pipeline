-- Silver layer load
-- File: 2_SILVER_LOAD_PRODUCTS.sql
-- Purpose: SCD Type 2 load for products
-- Load type: Initial + Incremental (single script)
-- Idempotent: yes

USE ROLE INTERN_ROLE;
USE DATABASE INTERN_DB;
USE SCHEMA INTERNS_SANDBOX;
USE WAREHOUSE INTERN_WH;

BEGIN;

-- 1. Get latest Bronze record per product
WITH latest_bronze AS (
    SELECT
        product_id,
        product_name,
        category,
        price,
        cost,
        supplier_id,

        HASH(category, price, cost) AS row_hash,

        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY ingestion_ts DESC
        ) AS rn
    FROM SS_BRONZE_PRODUCTS
),

bronze_deduped AS (
    SELECT *
    FROM latest_bronze
    WHERE rn = 1
),

-- 2. Current Silver records
current_silver AS (
    SELECT *
    FROM SS_SILVER_PRODUCTS
    WHERE is_current = TRUE
),

-- 3. Detect changes
change_detection AS (
    SELECT
        b.product_id,
        b.product_name,
        b.category,
        b.price,
        b.cost,
        b.supplier_id,
        b.row_hash,

        s.product_sk,
        s.row_hash AS silver_row_hash,

        CASE
            WHEN s.product_id IS NULL THEN 'INSERT'
            WHEN b.row_hash <> s.row_hash THEN 'UPDATE'
            ELSE 'NO_CHANGE'
        END AS change_type
    FROM bronze_deduped b
    LEFT JOIN current_silver s
        ON b.product_id = s.product_id
)

-- 4. Expire old versions
UPDATE SS_SILVER_PRODUCTS
SET
    effective_to = CURRENT_DATE - 1,
    is_current = FALSE,
    updated_ts = CURRENT_TIMESTAMP
WHERE product_sk IN (
    SELECT product_sk
    FROM change_detection
    WHERE change_type = 'UPDATE'
);

-- 5. Insert new & changed records
INSERT INTO SS_SILVER_PRODUCTS (
    product_id,
    product_name,
    category,
    price,
    cost,
    supplier_id,
    effective_from,
    effective_to,
    is_current,
    row_hash,
    created_ts,
    updated_ts
)
SELECT
    product_id,
    product_name,
    category,
    price,
    cost,
    supplier_id,
    CURRENT_DATE,
    NULL,
    TRUE,
    row_hash,
    CURRENT_TIMESTAMP,
    NULL
FROM change_detection
WHERE change_type IN ('INSERT', 'UPDATE');

COMMIT;
