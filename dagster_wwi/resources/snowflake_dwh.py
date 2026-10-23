"""Snowflake Data Warehouse Dagster Resource for COPY INTO & Mart Execution."""



import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict

import pandas as pd
from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


class SnowflakeWarehouseResource(ConfigurableResource):
    """Manages Snowflake connection, External Stage COPY INTO loads, and table clustering."""

    account: str = "xy12345.east-us-2.azure"
    user: str = "WWI_PIPELINE_USER"
    password: str = ""
    role: str = "ACCOUNTADMIN"
    warehouse: str = "WWI_WH"
    database: str = "WWI_DB"
    schema_raw: str = "RAW"
    schema_staging: str = "STAGING"
    schema_marts: str = "MARTS"
    external_stage: str = "WWI_AZURE_RAW_STAGE"
    raw_prefix: str = "wwi/v1"
    azure_storage_account_name: str = "stwwidatalakedev01"
    azure_storage_account_key: str = ""
    azure_storage_connection_string: str = ""
    azure_sas_token: str = ""
    azure_container_raw: str = "wwi-raw"
    dry_run_local: bool = True

    def _resolve_azure_credentials(self) -> tuple[str, str]:
        """Resolve Azure storage account name and key from resource config or environment."""
        account_name = (
            self.azure_storage_account_name
            or os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "stwwidatalakedev01")
        ).strip()
        account_key = (
            self.azure_storage_account_key or os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "")
        ).strip()
        conn_str = (
            self.azure_storage_connection_string
            or os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
        ).strip()

        if conn_str and (not account_key or account_key == "your_azure_storage_account_key_here"):
            parts = dict(
                item.split("=", 1) for item in conn_str.split(";") if "=" in item
            )
            account_name = parts.get("AccountName", account_name)
            account_key = parts.get("AccountKey", account_key)

        return account_name, account_key

    def _generate_azure_sas_token(self) -> str:
        """Return an explicit SAS token or generate a read/list Container SAS token from the account key."""
        explicit_sas = (self.azure_sas_token or os.getenv("AZURE_SAS_TOKEN", "")).strip()
        if explicit_sas:
            return explicit_sas

        account_name, account_key = self._resolve_azure_credentials()
        if not account_key or account_key == "your_azure_storage_account_key_here":
            return ""

        from azure.storage.blob import ContainerSasPermissions, generate_container_sas

        now_utc = datetime.now(timezone.utc)
        container_name = (
            self.azure_container_raw or os.getenv("AZURE_CONTAINER_RAW", "wwi-raw")
        ).strip()
        return generate_container_sas(
            account_name=account_name,
            container_name=container_name,
            account_key=account_key,
            permission=ContainerSasPermissions(read=True, list=True),
            start=now_utc - timedelta(minutes=15),
            expiry=now_utc + timedelta(hours=24),
        )

    def get_connection(self):
        import snowflake.connector

        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            role=self.role,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema_raw,
        )

    def copy_from_azure_stage_to_raw(
        self,
        table_files: Dict[str, Path],
        azure_uris: Dict[str, str],
    ) -> Dict[str, dict]:
        """Load Parquet files from the Azure External Stage into Snowflake RAW tables."""
        load_summary: Dict[str, dict] = {}

        if self.dry_run_local:
            for table_name, parquet_path in table_files.items():
                df = pd.read_parquet(parquet_path)
                sf_table = f"{self.database}.{self.schema_raw}.RAW_{table_name.upper()}"
                copy_sql = (
                    f"COPY INTO {sf_table} "
                    f"FROM @{self.database}.{self.schema_raw}.{self.external_stage}/{self.raw_prefix}/{table_name}/ "
                    f"FILE_FORMAT = (FORMAT_NAME = '{self.database}.{self.schema_raw}.WWI_PARQUET_FORMAT') "
                    f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;"
                )
                load_summary[table_name] = {
                    "snowflake_table": sf_table,
                    "rows_loaded": int(len(df)),
                    "source_azure_uri": azure_uris.get(table_name, ""),
                    "copy_into_sql": copy_sql,
                    "mode": "dry_run_local",
                }
            return load_summary

        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"USE ROLE {self.role};")
                cur.execute(f"USE WAREHOUSE {self.warehouse};")
                cur.execute(f"USE DATABASE {self.database};")
                cur.execute(f"USE SCHEMA {self.schema_raw};")

                fq_format = f"{self.database}.{self.schema_raw}.WWI_PARQUET_FORMAT"
                fq_stage = f"{self.database}.{self.schema_raw}.{self.external_stage}"

                cur.execute(
                    f"CREATE OR REPLACE FILE FORMAT {fq_format} "
                    f"TYPE = PARQUET COMPRESSION = SNAPPY USE_LOGICAL_TYPE = TRUE;"
                )

                sas_token = self._generate_azure_sas_token()
                if sas_token:
                    account_name, _ = self._resolve_azure_credentials()
                    container_name = (
                        self.azure_container_raw or os.getenv("AZURE_CONTAINER_RAW", "wwi-raw")
                    ).strip()
                    stage_url = f"azure://{account_name}.blob.core.windows.net/{container_name}/"
                    cur.execute(
                        f"CREATE OR REPLACE STAGE {fq_stage} "
                        f"URL = '{stage_url}' "
                        f"CREDENTIALS = (AZURE_SAS_TOKEN = %s) "
                        f"FILE_FORMAT = (FORMAT_NAME = '{fq_format}');",
                        (sas_token,),
                    )

                for table_name, parquet_path in table_files.items():
                    sf_table = f"{self.database}.{self.schema_raw}.RAW_{table_name.upper()}"
                    copy_sql = (
                        f"COPY INTO {sf_table} "
                        f"FROM @{fq_stage}/{self.raw_prefix}/{table_name}/ "
                        f"FILE_FORMAT = (FORMAT_NAME = '{fq_format}') "
                        f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
                        f"ON_ERROR = 'ABORT_STATEMENT';"
                    )
                    cur.execute(copy_sql)
                    cur.execute(f"SELECT COUNT(*) FROM {sf_table};")
                    row_count = cur.fetchone()[0]
                    load_summary[table_name] = {
                        "snowflake_table": sf_table,
                        "rows_loaded": int(row_count),
                        "source_azure_uri": azure_uris.get(table_name, ""),
                        "copy_into_sql": copy_sql,
                        "mode": "snowflake_cloud",
                    }
        finally:
            conn.close()

        return load_summary
