"""Dagster ingestion assets: Extract WWI -> Upload to Azure ADLS Gen2 -> COPY INTO Snowflake RAW."""



import os
from pathlib import Path
from typing import Dict

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, Output, asset

from dagster_wwi.resources.azure_datalake import AzureDataLakeResource
from dagster_wwi.resources.snowflake_dwh import SnowflakeWarehouseResource
from dagster_wwi.utils.wwi_data_generator import extract_or_generate_wwi_parquet


@asset(
    group_name="1_ingestion_datalake",
    compute_kind="python",
    description="Extracts Wide World Importers (WWI) source tables or generates fallback Snappy Parquet files.",
)
def extract_wwi_source_tables(context: AssetExecutionContext) -> Output[Dict[str, str]]:
    base_data_dir = Path(os.getenv("LOCAL_DATA_DIR", "./data")).resolve()
    raw_dir = base_data_dir / "raw"
    base_url = os.getenv("WWI_SOURCE_BASE_URL", "")
    order_count = int(os.getenv("WWI_SAMPLE_ORDER_COUNT", "5000"))
    start_date = os.getenv("WWI_SAMPLE_START_DATE", "2023-01-01")
    end_date = os.getenv("WWI_SAMPLE_END_DATE", "2025-12-31")
    seed = int(os.getenv("WWI_RANDOM_SEED", "42"))

    saved_files = extract_or_generate_wwi_parquet(
        output_dir=raw_dir,
        base_url=base_url,
        order_count=order_count,
        start_date=start_date,
        end_date=end_date,
        seed=seed,
    )

    row_counts = {}
    for table_name, file_path in saved_files.items():
        df = pd.read_parquet(file_path)
        row_counts[table_name] = len(df)
        context.log.info(f"Extracted WWI table '{table_name}': {len(df):,} rows -> {file_path}")

    serialized_paths = {k: str(v) for k, v in saved_files.items()}
    return Output(
        value=serialized_paths,
        metadata={
            "table_count": len(saved_files),
            "row_counts": MetadataValue.json(row_counts),
            "output_directory": str(raw_dir),
        },
    )


@asset(
    group_name="1_ingestion_datalake",
    compute_kind="azure_adls_gen2",
    description="Uploads partitioned WWI Parquet tables to Azure Data Lake Storage Gen2 (wwi-raw container).",
)
def upload_wwi_to_azure_datalake(
    context: AssetExecutionContext,
    extract_wwi_source_tables: Dict[str, str],
    azure_datalake: AzureDataLakeResource,
) -> Output[Dict[str, str]]:
    path_map = {k: Path(v) for k, v in extract_wwi_source_tables.items()}
    uploaded_uris = azure_datalake.upload_parquet_files(path_map)

    for table_name, uri in uploaded_uris.items():
        context.log.info(f"Staged '{table_name}' in Azure Data Lake: {uri}")

    return Output(
        value=uploaded_uris,
        metadata={
            "azure_storage_account": azure_datalake.storage_account_name,
            "azure_container": azure_datalake.container_raw,
            "uploaded_uris": MetadataValue.json(uploaded_uris),
            "dry_run_local": azure_datalake.dry_run_local,
        },
    )


@asset(
    group_name="2_snowflake_raw_load",
    compute_kind="snowflake",
    description="Loads WWI Parquet files from Azure External Stage into clustered Snowflake RAW tables via COPY INTO.",
)
def load_azure_datalake_to_snowflake_raw(
    context: AssetExecutionContext,
    extract_wwi_source_tables: Dict[str, str],
    upload_wwi_to_azure_datalake: Dict[str, str],
    snowflake_dwh: SnowflakeWarehouseResource,
) -> Output[Dict[str, dict]]:
    path_map = {k: Path(v) for k, v in extract_wwi_source_tables.items()}
    load_summary = snowflake_dwh.copy_from_azure_stage_to_raw(
        table_files=path_map,
        azure_uris=upload_wwi_to_azure_datalake,
    )

    total_rows = sum(item["rows_loaded"] for item in load_summary.values())
    context.log.info(
        f"Loaded {len(load_summary)} WWI tables ({total_rows:,} total rows) into Snowflake {snowflake_dwh.database}.{snowflake_dwh.schema_raw}"
    )

    return Output(
        value=load_summary,
        metadata={
            "snowflake_database": snowflake_dwh.database,
            "snowflake_schema": snowflake_dwh.schema_raw,
            "total_rows_loaded": total_rows,
            "tables_loaded": MetadataValue.json(load_summary),
        },
    )
