"""Dagster Definitions entry point for the WWI Azure Data Lake + Snowflake pipeline."""

import os

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)
from dotenv import load_dotenv

from dagster_wwi.assets.ingestion import (
    extract_wwi_source_tables,
    load_azure_datalake_to_snowflake_raw,
    upload_wwi_to_azure_datalake,
)
from dagster_wwi.assets.transformations import (
    build_dbt_snowflake_marts,
    check_dbt_marts_quality,
    export_bi_dashboard_datasets,
)
from dagster_wwi.resources.azure_datalake import AzureDataLakeResource
from dagster_wwi.resources.snowflake_dwh import SnowflakeWarehouseResource

load_dotenv()


def _is_dry_run_local() -> bool:
    return os.getenv("DRY_RUN_LOCAL", "true").strip().lower() in ("1", "true", "yes")


wwi_end_to_end_job = define_asset_job(
    name="wwi_end_to_end_pipeline",
    selection=AssetSelection.all(),
    description="End-to-end batch pipeline: Extract WWI -> Upload to Azure ADLS Gen2 -> COPY INTO Snowflake -> dbt Build -> BI Marts",
)

wwi_daily_schedule = ScheduleDefinition(
    job=wwi_end_to_end_job,
    cron_schedule="0 2 * * *",
    execution_timezone="UTC",
    description="Runs the WWI Azure Data Lake to Snowflake pipeline daily at 02:00 UTC.",
)

defs = Definitions(
    assets=[
        extract_wwi_source_tables,
        upload_wwi_to_azure_datalake,
        load_azure_datalake_to_snowflake_raw,
        build_dbt_snowflake_marts,
        export_bi_dashboard_datasets,
    ],
    asset_checks=[check_dbt_marts_quality],
    jobs=[wwi_end_to_end_job],
    schedules=[wwi_daily_schedule],
    resources={
        "azure_datalake": AzureDataLakeResource(
            storage_account_name=os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "stwwidatalakedev01"),
            storage_account_key=os.getenv("AZURE_STORAGE_ACCOUNT_KEY", ""),
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING", ""),
            container_raw=os.getenv("AZURE_CONTAINER_RAW", "wwi-raw"),
            raw_prefix=os.getenv("AZURE_RAW_PREFIX", "wwi/v1"),
            dry_run_local=_is_dry_run_local(),
        ),
        "snowflake_dwh": SnowflakeWarehouseResource(
            account=os.getenv("SNOWFLAKE_ACCOUNT", "xy12345.east-us-2.azure"),
            user=os.getenv("SNOWFLAKE_USER", "WWI_PIPELINE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD", ""),
            role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "WWI_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE", "WWI_DB"),
            schema_raw=os.getenv("SNOWFLAKE_SCHEMA_RAW", "RAW"),
            schema_staging=os.getenv("SNOWFLAKE_SCHEMA_STAGING", "STAGING"),
            schema_marts=os.getenv("SNOWFLAKE_SCHEMA_MARTS", "MARTS"),
            external_stage=os.getenv("SNOWFLAKE_EXTERNAL_STAGE", "WWI_AZURE_RAW_STAGE"),
            raw_prefix=os.getenv("AZURE_RAW_PREFIX", "wwi/v1"),
            azure_storage_account_name=os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "stwwidatalakedev01"),
            azure_storage_account_key=os.getenv("AZURE_STORAGE_ACCOUNT_KEY", ""),
            azure_storage_connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING", ""),
            azure_sas_token=os.getenv("AZURE_SAS_TOKEN", ""),
            azure_container_raw=os.getenv("AZURE_CONTAINER_RAW", "wwi-raw"),
            dry_run_local=_is_dry_run_local(),
        ),
    },
)
