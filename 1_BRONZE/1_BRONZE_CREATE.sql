-- Bronze layer table creation
-- File: 1_BRONZE_CREATE.sql
-- Purpose: Raw landing tables + watermark table
-- Idempotent: yes

USE ROLE SYSADMIN;
USE DATABASE INTERN_DB;
USE SCHEMA INTERNS_SANDBOX;
USE WAREHOUSE INTERN_WH;

-- Bronze orders
CREATE TABLE IF NOT EXISTS SS_BRONZE_ORDERS (
    order_id NUMBER,
    customer_id NUMBER,
    order_date DATE,
    order_status VARCHAR(50),
    total_amount NUMBER(12,2),
    payment_method VARCHAR(50),
    created_at TIMESTAMP_NTZ,

    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(500),
    row_hash VARCHAR(64)
);

-- Bronze customers
CREATE TABLE IF NOT EXISTS SS_BRONZE_CUSTOMERS (
    customer_id NUMBER,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    customer_segment VARCHAR(50),
    last_updated TIMESTAMP_NTZ,

    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(500),
    row_hash VARCHAR(64)
);

-- Bronze products
CREATE TABLE IF NOT EXISTS SS_BRONZE_PRODUCTS (
    product_id NUMBER,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price NUMBER(12,2),
    cost NUMBER(12,2),
    supplier_id NUMBER,
    last_updated TIMESTAMP_NTZ,

    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(500),
    row_hash VARCHAR(64)
);

-- Bronze order items
CREATE TABLE IF NOT EXISTS SS_BRONZE_ORDER_ITEMS (
    order_item_id NUMBER,
    order_id NUMBER,
    product_id NUMBER,
    quantity NUMBER,
    unit_price NUMBER(12,2),
    discount_percent NUMBER(5,2),

    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(500),
    row_hash VARCHAR(64)
);

-- Bronze watermark table
CREATE TABLE IF NOT EXISTS SS_BRONZE_LOAD_WATERMARKS (
    table_name VARCHAR(100) PRIMARY KEY,
    last_loaded_ts TIMESTAMP_NTZ
);
