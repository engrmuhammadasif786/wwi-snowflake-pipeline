output "azure_resource_group_name" {
  description = "Provisioned Azure Resource Group name"
  value       = azurerm_resource_group.wwi_rg.name
}

output "azure_storage_account_name" {
  description = "Provisioned Azure ADLS Gen2 Storage Account name"
  value       = azurerm_storage_account.wwi_datalake.name
}

output "azure_primary_dfs_endpoint" {
  description = "Primary ADLS Gen2 Data Lake endpoint"
  value       = azurerm_storage_account.wwi_datalake.primary_dfs_endpoint
}

output "azure_raw_container_url" {
  description = "Azure Blob URL for the WWI raw container"
  value       = "azure://${azurerm_storage_account.wwi_datalake.name}.blob.core.windows.net/${azurerm_storage_container.wwi_raw.name}/"
}

output "snowflake_warehouse_name" {
  description = "Provisioned Snowflake virtual warehouse"
  value       = snowflake_warehouse.wwi_wh.name
}

output "snowflake_database_name" {
  description = "Provisioned Snowflake database"
  value       = snowflake_database.wwi_db.name
}

output "snowflake_external_stage_name" {
  description = "Snowflake external stage connected to Azure ADLS Gen2"
  value       = "${snowflake_database.wwi_db.name}.${snowflake_schema.raw.name}.${snowflake_stage.wwi_azure_raw_stage.name}"
}

output "snowflake_azure_consent_url" {
  description = "Azure AD consent URL to authorize Snowflake service principal access"
  value       = snowflake_storage_integration.azure_wwi_integration.azure_consent_url
}
