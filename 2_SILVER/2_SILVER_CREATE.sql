-- Silver layer table creation
-- File: 2_SILVER_CREATE.sql
-- Purpose: Cleansed & conformed tables (SCD Type 2 – Customers)
-- Idempotent: yes

USE ROLE INTERN_ROLE;
USE DATABASE INTERN_DB;
USE SCHEMA INTERNS_SANDBOX;
USE WAREHOUSE INTERN_WH;


-- Silver customers (SCD Type 2)
CREATE TABLE IF NOT EXISTS SS_SILVER_CUSTOMERS (
    customer_sk NUMBER AUTOINCREMENT,
    customer_id NUMBER,

    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    customer_segment VARCHAR(50),

    effective_from DATE,
    effective_to DATE,
    is_current BOOLEAN,

    row_hash VARCHAR(64),

    created_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    updated_ts TIMESTAMP_NTZ
);

-- Silver products (SCD Type 2)
CREATE TABLE IF NOT EXISTS SS_SILVER_PRODUCTS (
    product_sk NUMBER AUTOINCREMENT,
    product_id NUMBER,

    product_name VARCHAR(255),
    category VARCHAR(100),
    price NUMBER(12,2),
    cost NUMBER(12,2),
    supplier_id NUMBER,

    effective_from DATE,
    effective_to DATE,
    is_current BOOLEAN,

    row_hash VARCHAR(64),

    created_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    updated_ts TIMESTAMP_NTZ
);