# definitions.py
"""
Unified Dagster definitions file - combines all elements from definitions.py and repository.py
"""
from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
    DefaultScheduleStatus,
    EnvVar,
)
from dagster_slack import SlackResource

from solana_pipeline.assets import (
    tokens,
    whales,
    webhook,
    txn_process,
    active_tokens,
)
from solana_pipeline.assets.google import gsheet_assets
from solana_pipeline.resources import (
    db_resource,
    birdeye_api_resource,
    google_sheets_resource,
)

# Load all assets
main_assets = load_assets_from_modules(
    [tokens, whales, webhook, txn_process, active_tokens]
)
all_assets = [*main_assets, *gsheet_assets]

# Define all jobs (including comprehensive_alpha_job from repository.py)
complete_pipeline_job = define_asset_job(
    "complete_pipeline_job",
    description="Process tokens, whales, and webhook data",
    selection=AssetSelection.assets(
        "token_list_v3",
        "trending_tokens", 
        "token_whales",
        "wallet_trade_history",
        "tracked_tokens",
        "wallet_pnl",
        "top_traders",
        "helius_webhook",
    ),
)

google_sheets_job = define_asset_job(
    "google_sheets_job",
    description="Export data to Google Sheets",
    selection=AssetSelection.groups("google_sheets"),
)

transaction_processing_job = define_asset_job(
    "transaction_processing_job",
    description="Process unprocessed webhook data into clean transaction records",
    selection=AssetSelection.assets(
        "unprocessed_webhook_data",
        "processed_transactions",
    ),
)

active_token_job = define_asset_job(
    "active_token_job", 
    description="Detect active token signals based on transaction activity",
    selection=AssetSelection.assets("active_token_notification"),
)

# Note: comprehensive_alpha_job from repository.py was removed because 
# alpha_signal_detection and enhanced_notification assets don't exist yet

# Define schedules (using timing from repository.py where different)
complete_pipeline_schedule = ScheduleDefinition(
    job=complete_pipeline_job,
    cron_schedule="0 */8 * * *",  # Every 8 hours (from repository.py)
    default_status=DefaultScheduleStatus.RUNNING,
)

google_sheets_schedule = ScheduleDefinition(
    job=google_sheets_job,
    cron_schedule="0 6 * * *",  # Daily at 6 AM UTC
    default_status=DefaultScheduleStatus.RUNNING,
)

active_token_schedule = ScheduleDefinition(
    job=active_token_job,
    cron_schedule="0 * * * *",  # Every hour
    default_status=DefaultScheduleStatus.RUNNING,
)

transaction_processing_schedule = ScheduleDefinition(
    job=transaction_processing_job,
    cron_schedule="*/3 * * * *",  # Every 3 minutes
    default_status=DefaultScheduleStatus.RUNNING,
)

# Configure resources with detailed configuration from repository.py
resources = {
    "db": db_resource.configured({
        "db_name": {"env": "DB_NAME"},
        "host": {"env": "DB_HOST"},
        "port": {"env": "DB_PORT"},
        "user": {"env": "DB_USER"},
        "password": {"env": "DB_PASSWORD"},
        "setup_db": True,  # Always create schemas
    }),
    "birdeye_api": birdeye_api_resource,
    "google_sheets": google_sheets_resource,
    "slack": SlackResource(
        token=EnvVar("SLACK_BOT_TOKEN"),
    ),
}

# Create unified Dagster Definitions
defs = Definitions(
    assets=all_assets,
    jobs=[
        complete_pipeline_job,
        google_sheets_job,
        transaction_processing_job,
        active_token_job,
        # comprehensive_alpha_job removed - assets don't exist yet
    ],
    schedules=[
        complete_pipeline_schedule,
        google_sheets_schedule,
        active_token_schedule,
        transaction_processing_schedule,
    ],
    resources=resources,
)