-- =============================================================================
-- 01_setup_warehouse_and_stage.sql
-- Sets up Snowflake Compute Warehouse, Database, Schemas, Azure Storage
-- Integration, Parquet File Format, and External Stage for WWI Data Lake.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- 1. Create Virtual Warehouse with Auto-Suspend & Auto-Resume
CREATE WAREHOUSE IF NOT EXISTS WWI_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Compute warehouse for Wide World Importers ingestion and dbt transformations';

-- 2. Create Database and Medallion Architecture Schemas
CREATE DATABASE IF NOT EXISTS WWI_DB
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Wide World Importers (WWI) Analytics Data Warehouse';

CREATE SCHEMA IF NOT EXISTS WWI_DB.RAW
    COMMENT = 'Raw landing zone loaded from Azure Data Lake Storage Gen2 (Parquet)';

CREATE SCHEMA IF NOT EXISTS WWI_DB.STAGING
    COMMENT = 'Cleaned, standardized, and deduplicated dbt staging views';

CREATE SCHEMA IF NOT EXISTS WWI_DB.MARTS
    COMMENT = 'Clustered dimensional star-schema and BI reporting marts';

-- 3. Configure Azure ADLS Gen2 Storage Integration
-- Note: Replace <AZURE_TENANT_ID> and <AZURE_STORAGE_ACCOUNT> or provision via Terraform
CREATE STORAGE INTEGRATION IF NOT EXISTS AZURE_WWI_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'AZURE'
    ENABLED = TRUE
    AZURE_TENANT_ID = '<AZURE_TENANT_ID>'
    STORAGE_ALLOWED_LOCATIONS = ('azure://<AZURE_STORAGE_ACCOUNT>.blob.core.windows.net/wwi-raw/');

-- 4. Create Snappy Parquet File Format
CREATE OR REPLACE FILE FORMAT WWI_DB.RAW.WWI_PARQUET_FORMAT
    TYPE = PARQUET
    COMPRESSION = SNAPPY
    BINARY_AS_TEXT = FALSE;

-- 5. Create External Stage pointing to Azure Blob Storage / ADLS Gen2
CREATE STAGE IF NOT EXISTS WWI_DB.RAW.WWI_AZURE_RAW_STAGE
    STORAGE_INTEGRATION = AZURE_WWI_INTEGRATION
    URL = 'azure://<AZURE_STORAGE_ACCOUNT>.blob.core.windows.net/wwi-raw/'
    FILE_FORMAT = WWI_DB.RAW.WWI_PARQUET_FORMAT;
