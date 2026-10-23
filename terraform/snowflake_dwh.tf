# ==============================================================================
# Snowflake Data Warehouse, Schemas, Azure Integration & Clustered Tables
# ==============================================================================

resource "snowflake_warehouse" "wwi_wh" {
  name                = var.snowflake_warehouse_name
  warehouse_size      = var.snowflake_warehouse_size
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
  comment             = "Compute warehouse for Wide World Importers ingestion and dbt transformations"
}

resource "snowflake_database" "wwi_db" {
  name                        = var.snowflake_database_name
  data_retention_time_in_days = 7
  comment                     = "Wide World Importers (WWI) Analytics Data Warehouse"
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.wwi_db.name
  name     = "RAW"
  comment  = "Raw landing zone ingested from Azure Data Lake Storage Gen2"
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.wwi_db.name
  name     = "STAGING"
  comment  = "Cleaned, typed, and deduplicated dbt staging models"
}

resource "snowflake_schema" "marts" {
  database = snowflake_database.wwi_db.name
  name     = "MARTS"
  comment  = "Clustered star-schema dimensions, facts, and BI aggregation marts"
}

# ------------------------------------------------------------------------------
# Azure Storage Integration & Parquet External Stage
# ------------------------------------------------------------------------------
resource "snowflake_storage_integration" "azure_wwi_integration" {
  name                      = "AZURE_WWI_INTEGRATION"
  type                      = "EXTERNAL_STAGE"
  enabled                   = true
  storage_provider          = "AZURE"
  azure_tenant_id           = var.azure_tenant_id
  storage_allowed_locations = [
    "azure://${azurerm_storage_account.wwi_datalake.name}.blob.core.windows.net/${azurerm_storage_container.wwi_raw.name}/"
  ]
  comment = "Snowflake Storage Integration to Azure ADLS Gen2 raw container"
}

data "azurerm_storage_account_blob_container_sas" "wwi_raw_sas" {
  connection_string = azurerm_storage_account.wwi_datalake.primary_connection_string
  container_name    = azurerm_storage_container.wwi_raw.name
  https_only        = true

  start  = "2025-01-01T00:00:00Z"
  expiry = "2028-12-31T23:59:59Z"

  permissions {
    read   = true
    add    = false
    create = false
    write  = false
    delete = false
    list   = true
  }
}

resource "snowflake_file_format" "wwi_parquet_format" {
  name        = "WWI_PARQUET_FORMAT"
  database    = snowflake_database.wwi_db.name
  schema      = snowflake_schema.raw.name
  format_type = "PARQUET"
  compression = "SNAPPY"
  comment     = "Snappy-compressed Parquet format for WWI lake-to-warehouse loads"
}

resource "snowflake_stage" "wwi_azure_raw_stage" {
  name        = "WWI_AZURE_RAW_STAGE"
  url         = "azure://${azurerm_storage_account.wwi_datalake.name}.blob.core.windows.net/${azurerm_storage_container.wwi_raw.name}/"
  database    = snowflake_database.wwi_db.name
  schema      = snowflake_schema.raw.name
  credentials = "AZURE_SAS_TOKEN = '${data.azurerm_storage_account_blob_container_sas.wwi_raw_sas.sas}'"
  file_format = "FORMAT_NAME = ${snowflake_database.wwi_db.name}.${snowflake_schema.raw.name}.${snowflake_file_format.wwi_parquet_format.name}"
  comment     = "External stage pointing to Azure ADLS Gen2 wwi-raw container"
}

# ------------------------------------------------------------------------------
# Clustered Fact Tables in RAW Schema for Optimal Micro-Partition Pruning
# ------------------------------------------------------------------------------
resource "snowflake_table" "raw_orders" {
  database    = snowflake_database.wwi_db.name
  schema      = snowflake_schema.raw.name
  name        = "RAW_ORDERS"
  comment     = "Raw WWI Sales.Orders table clustered by order_date for temporal pruning"
  cluster_by  = ["TO_DATE(ORDER_DATE)"]

  column {
    name     = "ORDER_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "CUSTOMER_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "SALESPERSON_PERSON_ID"
    type     = "NUMBER(38,0)"
    nullable = true
  }
  column {
    name     = "ORDER_DATE"
    type     = "DATE"
    nullable = false
  }
  column {
    name     = "EXPECTED_DELIVERY_DATE"
    type     = "DATE"
    nullable = true
  }
  column {
    name     = "CUSTOMER_PURCHASE_ORDER_NUMBER"
    type     = "VARCHAR(50)"
    nullable = true
  }
  column {
    name     = "IS_UNDERSUPPLY_BACKORDERED"
    type     = "BOOLEAN"
    nullable = true
  }
  column {
    name     = "INGESTED_AT"
    type     = "TIMESTAMP_NTZ(9)"
    nullable = false
  }
}

resource "snowflake_table" "raw_invoices" {
  database    = snowflake_database.wwi_db.name
  schema      = snowflake_schema.raw.name
  name        = "RAW_INVOICES"
  comment     = "Raw WWI Sales.Invoices table clustered by invoice_date and customer_id"
  cluster_by  = ["TO_DATE(INVOICE_DATE)", "CUSTOMER_ID"]

  column {
    name     = "INVOICE_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "CUSTOMER_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "ORDER_ID"
    type     = "NUMBER(38,0)"
    nullable = true
  }
  column {
    name     = "DELIVERY_METHOD_ID"
    type     = "NUMBER(38,0)"
    nullable = true
  }
  column {
    name     = "INVOICE_DATE"
    type     = "DATE"
    nullable = false
  }
  column {
    name     = "IS_CREDIT_NOTE"
    type     = "BOOLEAN"
    nullable = false
  }
  column {
    name     = "TOTAL_DRY_ITEMS"
    type     = "NUMBER(38,0)"
    nullable = true
  }
  column {
    name     = "TOTAL_CHILLER_ITEMS"
    type     = "NUMBER(38,0)"
    nullable = true
  }
  column {
    name     = "INGESTED_AT"
    type     = "TIMESTAMP_NTZ(9)"
    nullable = false
  }
}

resource "snowflake_table" "raw_customers" {
  database   = snowflake_database.wwi_db.name
  schema     = snowflake_schema.raw.name
  name       = "RAW_CUSTOMERS"
  comment    = "Raw WWI Sales.Customers table clustered by customer_id"
  cluster_by = ["CUSTOMER_ID"]

  column {
    name     = "CUSTOMER_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "CUSTOMER_NAME"
    type     = "VARCHAR(200)"
    nullable = false
  }
  column {
    name     = "CUSTOMER_CATEGORY_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "CUSTOMER_CATEGORY_NAME"
    type     = "VARCHAR(100)"
    nullable = false
  }
  column {
    name     = "BUYING_GROUP_NAME"
    type     = "VARCHAR(100)"
    nullable = true
  }
  column {
    name     = "DELIVERY_CITY_ID"
    type     = "NUMBER(38,0)"
    nullable = true
  }
  column {
    name     = "DELIVERY_CITY_NAME"
    type     = "VARCHAR(100)"
    nullable = true
  }
  column {
    name     = "STATE_PROVINCE_CODE"
    type     = "VARCHAR(10)"
    nullable = true
  }
  column {
    name     = "STATE_PROVINCE_NAME"
    type     = "VARCHAR(100)"
    nullable = true
  }
  column {
    name     = "SALES_TERRITORY"
    type     = "VARCHAR(100)"
    nullable = true
  }
  column {
    name     = "CREDIT_LIMIT"
    type     = "NUMBER(18,2)"
    nullable = true
  }
  column {
    name     = "ACCOUNT_OPENED_DATE"
    type     = "DATE"
    nullable = true
  }
  column {
    name     = "STANDARD_DISCOUNT_PERCENTAGE"
    type     = "NUMBER(18,3)"
    nullable = true
  }
  column {
    name     = "IS_ON_CREDIT_HOLD"
    type     = "BOOLEAN"
    nullable = true
  }
  column {
    name     = "INGESTED_AT"
    type     = "TIMESTAMP_NTZ(9)"
    nullable = false
  }
}

resource "snowflake_table" "raw_stock_items" {
  database = snowflake_database.wwi_db.name
  schema   = snowflake_schema.raw.name
  name     = "RAW_STOCK_ITEMS"
  comment  = "Raw WWI Warehouse.StockItems enriched with StockGroups"

  column {
    name     = "STOCK_ITEM_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "STOCK_ITEM_NAME"
    type     = "VARCHAR(200)"
    nullable = false
  }
  column {
    name     = "SUPPLIER_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "STOCK_GROUP_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "STOCK_GROUP_NAME"
    type     = "VARCHAR(100)"
    nullable = false
  }
  column {
    name     = "BRAND"
    type     = "VARCHAR(100)"
    nullable = true
  }
  column {
    name     = "SIZE"
    type     = "VARCHAR(50)"
    nullable = true
  }
  column {
    name     = "LEAD_TIME_DAYS"
    type     = "NUMBER(38,0)"
    nullable = true
  }
  column {
    name     = "IS_CHILLER_STOCK"
    type     = "BOOLEAN"
    nullable = true
  }
  column {
    name     = "TAX_RATE"
    type     = "NUMBER(18,3)"
    nullable = true
  }
  column {
    name     = "UNIT_PRICE"
    type     = "NUMBER(18,2)"
    nullable = true
  }
  column {
    name     = "RECOMMENDED_RETAIL_PRICE"
    type     = "NUMBER(18,2)"
    nullable = true
  }
  column {
    name     = "TYPICAL_WEIGHT_PER_UNIT"
    type     = "NUMBER(18,3)"
    nullable = true
  }
  column {
    name     = "INGESTED_AT"
    type     = "TIMESTAMP_NTZ(9)"
    nullable = false
  }
}

resource "snowflake_table" "raw_suppliers" {
  database = snowflake_database.wwi_db.name
  schema   = snowflake_schema.raw.name
  name     = "RAW_SUPPLIERS"
  comment  = "Raw WWI Purchasing.Suppliers enriched with SupplierCategories"

  column {
    name     = "SUPPLIER_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "SUPPLIER_NAME"
    type     = "VARCHAR(200)"
    nullable = false
  }
  column {
    name     = "SUPPLIER_CATEGORY_NAME"
    type     = "VARCHAR(100)"
    nullable = false
  }
  column {
    name     = "DELIVERY_CITY_NAME"
    type     = "VARCHAR(100)"
    nullable = true
  }
  column {
    name     = "STATE_PROVINCE_CODE"
    type     = "VARCHAR(10)"
    nullable = true
  }
  column {
    name     = "PAYMENT_DAYS"
    type     = "NUMBER(38,0)"
    nullable = true
  }
  column {
    name     = "INGESTED_AT"
    type     = "TIMESTAMP_NTZ(9)"
    nullable = false
  }
}

resource "snowflake_table" "raw_order_lines" {
  database = snowflake_database.wwi_db.name
  schema   = snowflake_schema.raw.name
  name     = "RAW_ORDER_LINES"
  comment  = "Raw WWI Sales.OrderLines item records"

  column {
    name     = "ORDER_LINE_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "ORDER_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "STOCK_ITEM_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "DESCRIPTION"
    type     = "VARCHAR(250)"
    nullable = true
  }
  column {
    name     = "QUANTITY"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "UNIT_PRICE"
    type     = "NUMBER(18,2)"
    nullable = false
  }
  column {
    name     = "TAX_RATE"
    type     = "NUMBER(18,3)"
    nullable = false
  }
  column {
    name     = "PICKED_QUANTITY"
    type     = "NUMBER(38,0)"
    nullable = true
  }
  column {
    name     = "INGESTED_AT"
    type     = "TIMESTAMP_NTZ(9)"
    nullable = false
  }
}

resource "snowflake_table" "raw_invoice_lines" {
  database = snowflake_database.wwi_db.name
  schema   = snowflake_schema.raw.name
  name     = "RAW_INVOICE_LINES"
  comment  = "Raw WWI Sales.InvoiceLines item-level revenue, tax, and profit records"

  column {
    name     = "INVOICE_LINE_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "INVOICE_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "STOCK_ITEM_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "DESCRIPTION"
    type     = "VARCHAR(250)"
    nullable = true
  }
  column {
    name     = "QUANTITY"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name     = "UNIT_PRICE"
    type     = "NUMBER(18,2)"
    nullable = false
  }
  column {
    name     = "TAX_RATE"
    type     = "NUMBER(18,3)"
    nullable = false
  }
  column {
    name     = "TAX_AMOUNT"
    type     = "NUMBER(18,2)"
    nullable = false
  }
  column {
    name     = "LINE_PROFIT"
    type     = "NUMBER(18,2)"
    nullable = false
  }
  column {
    name     = "EXTENDED_PRICE"
    type     = "NUMBER(18,2)"
    nullable = false
  }
  column {
    name     = "INGESTED_AT"
    type     = "TIMESTAMP_NTZ(9)"
    nullable = false
  }
}
