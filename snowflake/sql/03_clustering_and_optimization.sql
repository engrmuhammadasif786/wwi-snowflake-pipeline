-- =============================================================================
-- 03_clustering_and_optimization.sql
-- Configures Snowflake Micro-Partition Clustering Keys on Analytical Fact and
-- Mart Tables to optimize upstream BI dashboard queries (Temporal & Categorical).
-- =============================================================================
--
-- ARCHITECTURAL RATIONALE FOR SNOWFLAKE CLUSTERING:
-- Unlike BigQuery (which uses explicit PARTITION BY + CLUSTER BY), Snowflake
-- automatically stores all tables in immutable 16MB compressed micro-partitions
-- and maintains min/max metadata per column per micro-partition.
--
-- For multi-year WWI transactional tables (FCT_INVOICES, FCT_SALES_ORDERS) and
-- BI marts queried by Power BI / Looker Studio:
--   1. Dashboard Tile 2 filters and aggregates across temporal windows
--      (INVOICE_DATE, ORDER_DATE, INVOICE_MONTH).
--   2. Dashboard Tile 1 slices and groups by categorical dimensions
--      (CUSTOMER_CATEGORY_NAME, STOCK_GROUP_NAME, SALES_TERRITORY).
--
-- Defining multi-column CLUSTER BY keys ordered from lowest-cardinality date
-- grain (DATE_TRUNC / TO_DATE) to categorical dimension maximizes micro-partition
-- pruning and eliminates full table scans during interactive BI slicing.
-- =============================================================================

USE WAREHOUSE WWI_WH;
USE DATABASE WWI_DB;
USE SCHEMA MARTS;

-- 1. Cluster Fact Sales Orders by Order Date and Customer Category
ALTER TABLE IF EXISTS WWI_DB.MARTS.FCT_SALES_ORDERS
    CLUSTER BY (TO_DATE(ORDER_DATE), CUSTOMER_CATEGORY_NAME);

-- 2. Cluster Fact Invoices by Invoice Date, Customer Category, and Stock Group
ALTER TABLE IF EXISTS WWI_DB.MARTS.FCT_INVOICES
    CLUSTER BY (TO_DATE(INVOICE_DATE), CUSTOMER_CATEGORY_NAME, STOCK_GROUP_NAME);

-- 3. Cluster Temporal Monthly Revenue Mart by Month Start and Sales Territory
ALTER TABLE IF EXISTS WWI_DB.MARTS.MART_TEMPORAL_MONTHLY_REVENUE
    CLUSTER BY (INVOICE_MONTH, SALES_TERRITORY);

-- 4. Cluster Categorical Sales Mart by Customer Category and Stock Group
ALTER TABLE IF EXISTS WWI_DB.MARTS.MART_CATEGORICAL_SALES_BY_CATEGORY
    CLUSTER BY (CUSTOMER_CATEGORY_NAME, STOCK_GROUP_NAME);

-- 5. Inspect Micro-Partition Clustering Depth and Pruning Efficiency
SELECT SYSTEM$CLUSTERING_INFORMATION('WWI_DB.MARTS.FCT_INVOICES');
SELECT SYSTEM$CLUSTERING_INFORMATION('WWI_DB.MARTS.FCT_SALES_ORDERS');
