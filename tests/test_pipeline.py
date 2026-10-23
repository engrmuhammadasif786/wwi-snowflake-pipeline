"""Unit and integration test suite for the WWI Azure Data Lake + Snowflake Dagster Pipeline."""



from pathlib import Path

import pandas as pd
from dagster import materialize

from dagster_wwi.assets.ingestion import (
    extract_wwi_source_tables,
    load_azure_datalake_to_snowflake_raw,
    upload_wwi_to_azure_datalake,
)
from dagster_wwi.assets.transformations import (
    build_dbt_snowflake_marts,
    check_dbt_marts_quality,
    compute_marts_from_raw_parquet,
    export_bi_dashboard_datasets,
)
from dagster_wwi.resources.azure_datalake import AzureDataLakeResource
from dagster_wwi.resources.snowflake_dwh import SnowflakeWarehouseResource
from dagster_wwi.utils.wwi_data_generator import (
    WWI_TABLES,
    extract_or_generate_wwi_parquet,
    generate_wwi_dataset,
    try_fetch_remote_wwi_parquet,
)


def test_resilient_http_fallback_on_404():
    """Verify external URL 404/unreachable returns None gracefully without raising."""
    result = try_fetch_remote_wwi_parquet(
        base_url="https://httpstat.us/404",
        table_name="orders",
        timeout_sec=3,
    )
    assert result is None


def test_generate_wwi_dataset_referential_integrity():
    """Verify generated WWI tables have expected schemas and foreign key integrity."""
    tables = generate_wwi_dataset(
        order_count=200,
        start_date="2024-01-01",
        end_date="2024-12-31",
        seed=42,
    )
    assert set(tables.keys()) == set(WWI_TABLES)
    assert len(tables["orders"]) == 200
    assert len(tables["invoices"]) == 200
    assert len(tables["order_lines"]) >= 200
    assert len(tables["invoice_lines"]) == len(tables["order_lines"])

    # Check foreign key integrity
    assert set(tables["orders"]["customer_id"]).issubset(set(tables["customers"]["customer_id"]))
    assert set(tables["order_lines"]["stock_item_id"]).issubset(
        set(tables["stock_items"]["stock_item_id"])
    )
    assert (tables["invoice_lines"]["extended_price"] > 0).all()


def test_extract_and_compute_dbt_marts(tmp_path: Path):
    """Verify Parquet extraction and mart computation produce reconciled Categorical & Temporal marts."""
    raw_dir = tmp_path / "raw"
    export_dir = tmp_path / "exports"

    saved_files = extract_or_generate_wwi_parquet(
        output_dir=raw_dir,
        base_url="",
        order_count=250,
        start_date="2024-01-01",
        end_date="2024-12-31",
        seed=99,
    )
    assert len(saved_files) == 7

    mart_paths = compute_marts_from_raw_parquet(raw_dir=raw_dir, export_dir=export_dir)
    expected_marts = {
        "dim_customers",
        "dim_stock_items",
        "fct_sales_orders",
        "fct_invoices",
        "mart_categorical_sales_by_category",
        "mart_temporal_monthly_revenue",
    }
    assert set(mart_paths.keys()) == expected_marts

    cat_df = pd.read_csv(mart_paths["mart_categorical_sales_by_category"])
    temp_df = pd.read_csv(mart_paths["mart_temporal_monthly_revenue"])
    assert abs(cat_df["total_net_revenue"].sum() - temp_df["monthly_net_revenue"].sum()) < 1.0


def test_dagster_end_to_end_materialization(tmp_path: Path, monkeypatch):
    """Verify the entire 5-step Dagster asset pipeline and asset check execute cleanly."""
    monkeypatch.setenv("LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("WWI_SAMPLE_ORDER_COUNT", "150")
    monkeypatch.setenv("DRY_RUN_LOCAL", "true")

    result = materialize(
        assets=[
            extract_wwi_source_tables,
            upload_wwi_to_azure_datalake,
            load_azure_datalake_to_snowflake_raw,
            build_dbt_snowflake_marts,
            export_bi_dashboard_datasets,
        ],
        resources={
            "azure_datalake": AzureDataLakeResource(
                storage_account_name="stwwitest",
                container_raw="wwi-raw",
                dry_run_local=True,
            ),
            "snowflake_dwh": SnowflakeWarehouseResource(
                database="WWI_DB",
                schema_raw="RAW",
                schema_marts="MARTS",
                dry_run_local=True,
            ),
        },
    )
    assert result.success

    marts_output = result.output_for_node("build_dbt_snowflake_marts")
    quality_check = check_dbt_marts_quality(marts_output)
    assert quality_check.passed


def test_snowflake_resource_generates_azure_sas_token():
    """Verify SnowflakeWarehouseResource generates a valid Azure Container SAS token from account key."""
    dummy_key = "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3RrZXk="
    sf_resource = SnowflakeWarehouseResource(
        azure_storage_account_name="stwwidatalakedev01",
        azure_storage_account_key=dummy_key,
        azure_container_raw="wwi-raw",
        dry_run_local=True,
    )
    sas_token = sf_resource._generate_azure_sas_token()
    assert "sv=" in sas_token
    assert "sr=c" in sas_token
    assert "sp=rl" in sas_token
    assert "sig=" in sas_token

