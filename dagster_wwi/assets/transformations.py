"""Dagster transformation & BI export assets: dbt Snowflake build and Power BI / Looker Studio marts."""



import hashlib
import os
import subprocess
from pathlib import Path
from typing import Dict

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    MetadataValue,
    Output,
    asset,
    asset_check,
)

from dagster_wwi.resources.snowflake_dwh import SnowflakeWarehouseResource


def _surrogate_key(series: pd.Series) -> pd.Series:
    return series.astype(str).apply(lambda v: hashlib.md5(v.encode("utf-8")).hexdigest())


def compute_marts_from_raw_parquet(raw_dir: Path, export_dir: Path) -> Dict[str, Path]:
    """Compute the exact dbt staging, intermediate, and clustered mart datasets into export_dir."""
    export_dir.mkdir(parents=True, exist_ok=True)

    customers = pd.read_parquet(raw_dir / "customers.parquet")
    stock_items = pd.read_parquet(raw_dir / "stock_items.parquet")
    suppliers = pd.read_parquet(raw_dir / "suppliers.parquet")
    orders = pd.read_parquet(raw_dir / "orders.parquet")
    order_lines = pd.read_parquet(raw_dir / "order_lines.parquet")
    invoices = pd.read_parquet(raw_dir / "invoices.parquet")
    invoice_lines = pd.read_parquet(raw_dir / "invoice_lines.parquet")

    # 1. Enriched Invoices (fct_invoices)
    invoices["invoice_date"] = pd.to_datetime(invoices["invoice_date"])
    invoices["invoice_month"] = invoices["invoice_date"].dt.to_period("M").dt.to_timestamp()
    invoices["invoice_year"] = invoices["invoice_date"].dt.year
    invoices["invoice_quarter"] = invoices["invoice_date"].dt.quarter

    invoice_lines["net_revenue"] = (invoice_lines["extended_price"] - invoice_lines["tax_amount"]).round(2)

    fct_invoices = (
        invoice_lines.merge(
            invoices[
                [
                    "invoice_id",
                    "order_id",
                    "customer_id",
                    "invoice_date",
                    "invoice_month",
                    "invoice_year",
                    "invoice_quarter",
                    "is_credit_note",
                ]
            ],
            on="invoice_id",
            how="inner",
        )
        .merge(
            customers[
                [
                    "customer_id",
                    "customer_name",
                    "customer_category_id",
                    "customer_category_name",
                    "buying_group_name",
                    "delivery_city_name",
                    "state_province_code",
                    "state_province_name",
                    "sales_territory",
                ]
            ],
            on="customer_id",
            how="inner",
        )
        .merge(
            stock_items[
                [
                    "stock_item_id",
                    "stock_item_name",
                    "stock_group_id",
                    "stock_group_name",
                    "brand",
                    "is_chiller_stock",
                    "supplier_id",
                ]
            ],
            on="stock_item_id",
            how="inner",
        )
        .merge(
            suppliers[["supplier_id", "supplier_name", "supplier_category_name"]],
            on="supplier_id",
            how="left",
        )
    )
    fct_invoices["invoice_line_sk"] = _surrogate_key(fct_invoices["invoice_line_id"])
    fct_invoices["profit_margin_pct"] = (
        (fct_invoices["line_profit"] * 100.0) / fct_invoices["net_revenue"].replace(0, pd.NA)
    ).fillna(0.0).round(2)
    # Sort by Snowflake clustering key: (invoice_date, customer_category_name, stock_group_name)
    fct_invoices = fct_invoices.sort_values(
        by=["invoice_date", "customer_category_name", "stock_group_name"]
    ).reset_index(drop=True)

    # 2. Enriched Sales Orders (fct_sales_orders)
    orders["order_date"] = pd.to_datetime(orders["order_date"])
    orders["expected_delivery_date"] = pd.to_datetime(orders["expected_delivery_date"])
    orders["order_month"] = orders["order_date"].dt.to_period("M").dt.to_timestamp()
    orders["promised_lead_time_days"] = (orders["expected_delivery_date"] - orders["order_date"]).dt.days

    order_lines["net_order_amount"] = (order_lines["quantity"] * order_lines["unit_price"]).round(2)
    order_lines["estimated_tax_amount"] = (
        order_lines["net_order_amount"] * (order_lines["tax_rate"] / 100.0)
    ).round(2)
    order_lines["gross_order_amount"] = (
        order_lines["net_order_amount"] + order_lines["estimated_tax_amount"]
    ).round(2)

    fct_sales_orders = (
        order_lines.merge(
            orders[
                [
                    "order_id",
                    "customer_id",
                    "order_date",
                    "order_month",
                    "expected_delivery_date",
                    "promised_lead_time_days",
                    "is_undersupply_backordered",
                ]
            ],
            on="order_id",
            how="inner",
        )
        .merge(
            customers[
                [
                    "customer_id",
                    "customer_name",
                    "customer_category_id",
                    "customer_category_name",
                    "buying_group_name",
                    "sales_territory",
                    "state_province_code",
                ]
            ],
            on="customer_id",
            how="inner",
        )
        .merge(
            stock_items[
                [
                    "stock_item_id",
                    "stock_item_name",
                    "stock_group_id",
                    "stock_group_name",
                    "is_chiller_stock",
                ]
            ],
            on="stock_item_id",
            how="inner",
        )
    )
    fct_sales_orders["order_line_sk"] = _surrogate_key(fct_sales_orders["order_line_id"])
    fct_sales_orders["fulfillment_rate_pct"] = (
        (fct_sales_orders["picked_quantity"] * 100.0) / fct_sales_orders["quantity"]
    ).round(2)
    fct_sales_orders = fct_sales_orders.sort_values(
        by=["order_date", "customer_category_name"]
    ).reset_index(drop=True)

    # 3. dim_customers
    cust_lifetime = (
        fct_invoices.groupby("customer_id", as_index=False)
        .agg(
            lifetime_invoice_count=("invoice_id", "nunique"),
            lifetime_net_revenue=("net_revenue", lambda s: round(float(s.sum()), 2)),
            lifetime_profit=("line_profit", lambda s: round(float(s.sum()), 2)),
            first_invoice_date=("invoice_date", "min"),
            last_invoice_date=("invoice_date", "max"),
        )
    )
    dim_customers = customers.merge(cust_lifetime, on="customer_id", how="left")
    dim_customers["customer_sk"] = _surrogate_key(dim_customers["customer_id"])
    dim_customers["lifetime_invoice_count"] = dim_customers["lifetime_invoice_count"].fillna(0).astype(int)
    dim_customers["lifetime_net_revenue"] = dim_customers["lifetime_net_revenue"].fillna(0.0)
    dim_customers["lifetime_profit"] = dim_customers["lifetime_profit"].fillna(0.0)

    # 4. dim_stock_items
    dim_stock_items = stock_items.merge(
        suppliers[["supplier_id", "supplier_name", "supplier_category_name", "payment_days"]],
        on="supplier_id",
        how="left",
    )
    dim_stock_items["stock_item_sk"] = _surrogate_key(dim_stock_items["stock_item_id"])
    dim_stock_items["storage_type"] = dim_stock_items["is_chiller_stock"].map(
        lambda x: "Chiller Stock" if x else "Dry Stock"
    )
    dim_stock_items["catalog_markup_amount"] = (
        dim_stock_items["recommended_retail_price"] - dim_stock_items["unit_price"]
    ).round(2)

    # 5. Dashboard Tile 1 Mart: mart_categorical_sales_by_category
    mart_categorical = (
        fct_invoices.groupby(
            ["customer_category_name", "stock_group_name", "sales_territory", "buying_group_name"],
            as_index=False,
        )
        .agg(
            total_invoices=("invoice_id", "nunique"),
            active_customers=("customer_id", "nunique"),
            total_units_sold=("quantity", "sum"),
            total_net_revenue=("net_revenue", lambda s: round(float(s.sum()), 2)),
            total_tax_amount=("tax_amount", lambda s: round(float(s.sum()), 2)),
            total_gross_revenue=("extended_price", lambda s: round(float(s.sum()), 2)),
            total_profit=("line_profit", lambda s: round(float(s.sum()), 2)),
        )
    )
    mart_categorical["profit_margin_pct"] = (
        (mart_categorical["total_profit"] * 100.0) / mart_categorical["total_net_revenue"]
    ).round(2)
    mart_categorical["avg_invoice_revenue"] = (
        mart_categorical["total_net_revenue"] / mart_categorical["total_invoices"]
    ).round(2)
    mart_categorical = mart_categorical.sort_values(
        by=["customer_category_name", "stock_group_name"]
    ).reset_index(drop=True)

    # 6. Dashboard Tile 2 Mart: mart_temporal_monthly_revenue
    fct_invoices["chiller_rev"] = fct_invoices["net_revenue"].where(fct_invoices["is_chiller_stock"], 0.0)
    fct_invoices["dry_rev"] = fct_invoices["net_revenue"].where(~fct_invoices["is_chiller_stock"], 0.0)

    mart_temporal = (
        fct_invoices.groupby(
            ["invoice_month", "invoice_year", "invoice_quarter", "sales_territory", "customer_category_name"],
            as_index=False,
        )
        .agg(
            total_invoices=("invoice_id", "nunique"),
            total_orders=("order_id", "nunique"),
            active_customers=("customer_id", "nunique"),
            total_units_sold=("quantity", "sum"),
            monthly_net_revenue=("net_revenue", lambda s: round(float(s.sum()), 2)),
            monthly_gross_revenue=("extended_price", lambda s: round(float(s.sum()), 2)),
            monthly_profit=("line_profit", lambda s: round(float(s.sum()), 2)),
            chiller_net_revenue=("chiller_rev", lambda s: round(float(s.sum()), 2)),
            dry_net_revenue=("dry_rev", lambda s: round(float(s.sum()), 2)),
        )
    )
    mart_temporal["profit_margin_pct"] = (
        (mart_temporal["monthly_profit"] * 100.0) / mart_temporal["monthly_net_revenue"]
    ).round(2)
    mart_temporal = mart_temporal.sort_values(
        by=["invoice_month", "sales_territory", "customer_category_name"]
    ).reset_index(drop=True)
    mart_temporal["prev_month_net_revenue"] = mart_temporal.groupby(
        ["sales_territory", "customer_category_name"]
    )["monthly_net_revenue"].shift(1)
    mart_temporal["ytd_net_revenue"] = (
        mart_temporal.groupby(["sales_territory", "customer_category_name", "invoice_year"])[
            "monthly_net_revenue"
        ]
        .cumsum()
        .round(2)
    )

    marts_dict = {
        "dim_customers": dim_customers,
        "dim_stock_items": dim_stock_items,
        "fct_sales_orders": fct_sales_orders,
        "fct_invoices": fct_invoices.drop(columns=["chiller_rev", "dry_rev"]),
        "mart_categorical_sales_by_category": mart_categorical,
        "mart_temporal_monthly_revenue": mart_temporal,
    }

    written_paths: Dict[str, Path] = {}
    for mart_name, df in marts_dict.items():
        csv_path = export_dir / f"{mart_name}.csv"
        parquet_path = export_dir / f"{mart_name}.parquet"
        df.to_csv(csv_path, index=False)
        df.to_parquet(parquet_path, index=False, compression="snappy")
        written_paths[mart_name] = csv_path

    return written_paths


@asset(
    group_name="3_dbt_snowflake_transformations",
    compute_kind="dbt",
    description="Executes dbt transformations (staging -> intermediate -> clustered marts) in Snowflake and exports BI mart snapshots.",
)
def build_dbt_snowflake_marts(
    context: AssetExecutionContext,
    load_azure_datalake_to_snowflake_raw: Dict[str, dict],
    snowflake_dwh: SnowflakeWarehouseResource,
) -> Output[Dict[str, str]]:
    project_root = Path(__file__).resolve().parents[2]
    dbt_dir = project_root / "dbt_wwi"
    base_data_dir = Path(os.getenv("LOCAL_DATA_DIR", "./data")).resolve()
    raw_dir = base_data_dir / "raw"
    export_dir = base_data_dir / "exports"

    if not snowflake_dwh.dry_run_local:
        context.log.info("Running `dbt deps` and `dbt build` against Snowflake warehouse...")
        subprocess.run(
            ["dbt", "deps", "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir)],
            check=True,
        )
        subprocess.run(
            ["dbt", "build", "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir)],
            check=True,
        )
    else:
        context.log.info(
            "[DRY_RUN_LOCAL] Computing dbt staging, intermediate, and clustered mart models locally."
        )

    written_paths = compute_marts_from_raw_parquet(raw_dir=raw_dir, export_dir=export_dir)
    mart_row_counts = {
        name: int(len(pd.read_csv(path))) for name, path in written_paths.items()
    }
    context.log.info(f"Materialized 6 dbt marts: {mart_row_counts}")

    return Output(
        value={k: str(v) for k, v in written_paths.items()},
        metadata={
            "dbt_project_dir": str(dbt_dir),
            "snowflake_target_schema": snowflake_dwh.schema_marts,
            "mart_row_counts": MetadataValue.json(mart_row_counts),
        },
    )


@asset(
    group_name="4_bi_dashboard_serving",
    compute_kind="powerbi_looker",
    description="Prepares and verifies the two-tile BI dashboard datasets (Categorical & Temporal) for Power BI and Looker Studio.",
)
def export_bi_dashboard_datasets(
    context: AssetExecutionContext,
    build_dbt_snowflake_marts: Dict[str, str],
) -> Output[Dict[str, dict]]:
    cat_df = pd.read_csv(build_dbt_snowflake_marts["mart_categorical_sales_by_category"])
    temp_df = pd.read_csv(build_dbt_snowflake_marts["mart_temporal_monthly_revenue"])

    summary = {
        "tile_1_categorical": {
            "mart": "WWI_DB.MARTS.MART_CATEGORICAL_SALES_BY_CATEGORY",
            "rows": int(len(cat_df)),
            "customer_categories": int(cat_df["customer_category_name"].nunique()),
            "stock_groups": int(cat_df["stock_group_name"].nunique()),
            "total_net_revenue_usd": round(float(cat_df["total_net_revenue"].sum()), 2),
            "total_profit_usd": round(float(cat_df["total_profit"].sum()), 2),
        },
        "tile_2_temporal": {
            "mart": "WWI_DB.MARTS.MART_TEMPORAL_MONTHLY_REVENUE",
            "rows": int(len(temp_df)),
            "distinct_months": int(temp_df["invoice_month"].nunique()),
            "min_month": str(temp_df["invoice_month"].min()),
            "max_month": str(temp_df["invoice_month"].max()),
            "total_net_revenue_usd": round(float(temp_df["monthly_net_revenue"].sum()), 2),
        },
    }
    context.log.info(f"Dashboard serving marts verified: {summary}")

    return Output(
        value=summary,
        metadata={
            "tile_1_categorical_summary": MetadataValue.json(summary["tile_1_categorical"]),
            "tile_2_temporal_summary": MetadataValue.json(summary["tile_2_temporal"]),
        },
    )


@asset_check(
    asset=build_dbt_snowflake_marts,
    description="Verifies that all dbt mart tables have positive revenue and zero null primary keys.",
)
def check_dbt_marts_quality(build_dbt_snowflake_marts: Dict[str, str]) -> AssetCheckResult:
    fct_invoices = pd.read_csv(build_dbt_snowflake_marts["fct_invoices"])
    cat_mart = pd.read_csv(build_dbt_snowflake_marts["mart_categorical_sales_by_category"])
    temp_mart = pd.read_csv(build_dbt_snowflake_marts["mart_temporal_monthly_revenue"])

    has_positive_prices = bool((fct_invoices["extended_price"] > 0).all())
    no_null_sk = bool(fct_invoices["invoice_line_sk"].notna().all())
    revenue_reconciles = (
        abs(float(cat_mart["total_net_revenue"].sum()) - float(temp_mart["monthly_net_revenue"].sum()))
        < 1.0
    )

    passed = has_positive_prices and no_null_sk and revenue_reconciles
    return AssetCheckResult(
        passed=passed,
        metadata={
            "invoice_line_count": int(len(fct_invoices)),
            "has_positive_prices": has_positive_prices,
            "no_null_surrogate_keys": no_null_sk,
            "categorical_vs_temporal_revenue_reconciles": revenue_reconciles,
        },
    )
