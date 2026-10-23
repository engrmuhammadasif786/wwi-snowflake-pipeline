# ==============================================================================
# Azure Variables
# ==============================================================================
variable "azure_subscription_id" {
  description = "Azure Subscription ID for provisioning the WWI Data Lake"
  type        = string
}

variable "azure_tenant_id" {
  description = "Azure Active Directory Tenant ID (used by both AzureRM and Snowflake Storage Integration)"
  type        = string
}

variable "azure_resource_group_name" {
  description = "Name of the Azure Resource Group"
  type        = string
  default     = "rg-wwi-analytics-dev"
}

variable "azure_location" {
  description = "Azure region for Resource Group and ADLS Gen2 Storage Account"
  type        = string
  default     = "East US"
}

variable "azure_storage_account_name" {
  description = "Globally unique name for the Azure Data Lake Storage Gen2 account (3-24 lowercase alphanumeric)"
  type        = string
  default     = "stwwidatalakedev01"
}

variable "azure_raw_container_name" {
  description = "Container name for raw WWI Parquet files in ADLS Gen2"
  type        = string
  default     = "wwi-raw"
}

variable "azure_staging_container_name" {
  description = "Container name for staged/processed WWI artifacts"
  type        = string
  default     = "wwi-staging"
}

variable "environment" {
  description = "Deployment environment tag (dev, staging, prod)"
  type        = string
  default     = "dev"
}

# ==============================================================================
# Snowflake Variables
# ==============================================================================
variable "snowflake_account" {
  description = "Snowflake account identifier (e.g., xy12345.east-us-2.azure)"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake username for Terraform provisioning"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake password for Terraform provisioning"
  type        = string
  sensitive   = true
}

variable "snowflake_role" {
  description = "Snowflake role with privileges to create Warehouse, Database, and Storage Integration"
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "snowflake_warehouse_name" {
  description = "Snowflake virtual warehouse name for WWI pipeline workloads"
  type        = string
  default     = "WWI_WH"
}

variable "snowflake_warehouse_size" {
  description = "Snowflake virtual warehouse size"
  type        = string
  default     = "X-SMALL"
}

variable "snowflake_database_name" {
  description = "Snowflake database name for Wide World Importers analytics"
  type        = string
  default     = "WWI_DB"
}
