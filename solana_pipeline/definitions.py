"""Dagster definitions for Solana pipeline."""
import os
from pathlib import Path

from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
    DefaultScheduleStatus,
    EnvVar,
    AssetExecutionContext,
    AssetKey,
)
from dagster_postgres.storage import postgres_io_manager


from dagster_slack import SlackResource
from dagster_dbt import DbtCliResource, dbt_assets

# Asset modules
from solana_pipeline.assets import (
    tokens,
    whales,
    webhook,
    txn_process,
    active_tokens,
    token_enrichment,
)
from solana_pipeline.assets.google import gsheet_assets
from solana_pipeline.resources import (
    db_resource,
    birdeye_api_resource,
    google_sheets_resource,
)

# Constants
DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt"
DBT_MANIFEST = DBT_PROJECT_DIR / "target" / "manifest.json"

# Verify DBT setup
if not DBT_MANIFEST.exists():
    raise FileNotFoundError(
        f"DBT manifest not found at {DBT_MANIFEST}. "
        "Run 'dbt parse' in the dbt directory to generate it."
    )


# --- DBT Assets ---
@dbt_assets(manifest=DBT_MANIFEST)
def solana_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run DBT models for Solana analytics."""
    yield from dbt.cli(["build"], context=context).stream()


# --- Load Python Assets ---
python_assets = load_assets_from_modules(
    [tokens, whales, webhook, txn_process, active_tokens, token_enrichment]
)
all_assets = [*python_assets, *gsheet_assets, solana_dbt_models]


# --- Job Definitions ---

whale_tracking_job = define_asset_job(
    "whale_tracking_job",
    description="Track whale wallets and analyze their trading patterns.",
    selection=AssetSelection.assets(
        "token_list_v3",
        "trending_tokens",
        "token_whales",
        "wallet_trade_history",
        "tracked_tokens",
        "wallet_pnl",
        "top_traders",
    ),
    tags={"pipeline": "whale_analysis"},
)

webhook_processing_job = define_asset_job(
    "webhook_processing_job",
    description="Process Helius webhooks and detect active tokens via a hybrid Python/DBT approach.",
    selection=AssetSelection.keys(
        "webhook_staging_data",
        "mark_webhooks_processed",
        "fetch_token_creation",
        "fetch_token_metadata",
        "fetch_token_security",
        "active_token_notification",
        # Explicitly select DBT assets that are part of this job's flow
        AssetKey(["silver", "mart_txns_clean"]),
        AssetKey(["silver", "stg_recent_transactions"]),
        AssetKey(["gold", "mart_active_tokens"]),
        AssetKey(["silver", "stg_webhook_transactions"])
    ),
    tags={"pipeline": "webhook_processing", "includes_dbt": "true"},
)

google_sheets_export_job = define_asset_job(
    "google_sheets_export_job",
    description="Export analytics data to Google Sheets.",
    selection=AssetSelection.groups("google_sheets"),
    tags={"pipeline": "exports"},
)

helius_webhook_job = define_asset_job(
    "helius_webhook_job",
    description="Receive and store raw Helius webhook data.",
    selection=AssetSelection.assets("helius_webhook"),
    tags={"pipeline": "ingestion"},
)


# --- Schedule Definitions ---

whale_tracking_schedule = ScheduleDefinition(
    job=whale_tracking_job,
    cron_schedule="0 */6 * * *",  # Runs every 6 hours
    default_status=DefaultScheduleStatus.RUNNING,
)

webhook_processing_schedule = ScheduleDefinition(
    job=webhook_processing_job,
    cron_schedule="*/30 * * * *",  # Runs every 30 minutes
    default_status=DefaultScheduleStatus.RUNNING,
)

google_sheets_export_schedule = ScheduleDefinition(
    job=google_sheets_export_job,
    cron_schedule="0 6,18 * * *",  # Runs twice daily at 6 AM and 6 PM
    default_status=DefaultScheduleStatus.RUNNING,
)


# --- Resource Configuration ---
resources = {
    "db": db_resource.configured({"setup_db": True}),
    "birdeye_api": birdeye_api_resource,
    "google_sheets": google_sheets_resource.configured({
        "service_account_key_path": EnvVar("GOOGLE_SHEETS_KEY_PATH"),
        "spreadsheet_id": EnvVar("GOOGLE_SHEETS_SPREADSHEET_ID"),
    }),
    "slack": SlackResource(token=EnvVar("SLACK_BOT_TOKEN")),
    "dbt": DbtCliResource(
        project_dir=str(DBT_PROJECT_DIR),
        profiles_dir=str(DBT_PROJECT_DIR),
        target_path=str(DBT_PROJECT_DIR / "target"),
    ),
    "io_manager": postgres_io_manager(
        conn_string=f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
        schema="public"
    ),
}


# --- Dagster Definitions ---
defs = Definitions(
    assets=all_assets,
    jobs=[
        whale_tracking_job,
        webhook_processing_job,
        google_sheets_export_job,
        helius_webhook_job,
    ],
    schedules=[
        whale_tracking_schedule,
        webhook_processing_schedule,
        google_sheets_export_schedule,
    ],
    resources=resources,
)