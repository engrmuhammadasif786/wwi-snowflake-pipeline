# ==============================================================================
# Azure Data Lake Storage Gen2 (ADLS) Infrastructure
# ==============================================================================

resource "azurerm_resource_group" "wwi_rg" {
  name     = var.azure_resource_group_name
  location = var.azure_location

  tags = {
    project     = "wwi-snowflake-pipeline"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "azurerm_storage_account" "wwi_datalake" {
  name                     = var.azure_storage_account_name
  resource_group_name      = azurerm_resource_group.wwi_rg.name
  location                 = azurerm_resource_group.wwi_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # Enables Hierarchical Namespace for ADLS Gen2
  min_tls_version          = "TLS1_2"

  blob_properties {
    delete_retention_policy {
      days = 7
    }
  }

  tags = {
    project     = "wwi-snowflake-pipeline"
    environment = var.environment
    layer       = "datalake"
    managed_by  = "terraform"
  }
}

resource "azurerm_storage_container" "wwi_raw" {
  name                  = var.azure_raw_container_name
  storage_account_name  = azurerm_storage_account.wwi_datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "wwi_staging" {
  name                  = var.azure_staging_container_name
  storage_account_name  = azurerm_storage_account.wwi_datalake.name
  container_access_type = "private"
}
