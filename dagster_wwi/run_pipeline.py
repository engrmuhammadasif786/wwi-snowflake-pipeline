"""CLI entrypoint to materialize the entire WWI Dagster asset pipeline."""



import sys

from dagster import materialize

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
from dagster_wwi.definitions import defs


def main() -> int:
    print("=" * 80)
    print("Executing Wide World Importers (WWI) -> Azure ADLS -> Snowflake Pipeline")
    print("=" * 80)

    result = materialize(
        assets=[
            extract_wwi_source_tables,
            upload_wwi_to_azure_datalake,
            load_azure_datalake_to_snowflake_raw,
            build_dbt_snowflake_marts,
            export_bi_dashboard_datasets,
        ],
        resources=defs.resources,
    )

    if not result.success:
        print("Pipeline execution failed!", file=sys.stderr)
        return 1

    export_summary = result.output_for_node("export_bi_dashboard_datasets")
    check_eval = check_dbt_marts_quality(result.output_for_node("build_dbt_snowflake_marts"))

    print("\nPipeline Materialization Succeeded!")
    print(f"  - Asset Check (check_dbt_marts_quality): PASSED={check_eval.passed}")
    print(f"  - Tile 1 (Categorical Mart): {export_summary['tile_1_categorical']}")
    print(f"  - Tile 2 (Temporal Mart):    {export_summary['tile_2_temporal']}")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
