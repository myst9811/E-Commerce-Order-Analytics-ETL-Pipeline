# SS E-Commerce Order Analytics ETL Pipeline

## Platform

- Snowflake (SQL-first)

## Architecture

- Medallion: Bronze → Silver → Gold

## Key Rules

- Single unified pipeline (no day-based scripts)
- Idempotent SQL
- Watermark-based incremental loads
- MERGE-based upserts
- SCD Type 2:
  - Customers
  - Products
  - Orders

## Naming Conventions

- Tables: SS*<LAYER>*<TABLE_NAME>
- File format: csv_file_format_saikia
- Stages: ss_bronze_stage, ss_silver_stage, ss_gold_stage
- Watermarks: ss\_<layer>\_load_watermarks

## Execution Order

0_SETUP → 1_BRONZE → 2_SILVER → 3_GOLD
