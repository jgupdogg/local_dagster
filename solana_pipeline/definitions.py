from dagster import Definitions, load_assets_from_modules, ScheduleDefinition

# Import assets 
from solana_pipeline.assets import active_tokens, tokens, whales, webhook, txn_process
from solana_pipeline.assets.google import gsheet_assets

# Import jobs
from solana_pipeline.jobs import (
    complete_pipeline_job, 
    google_sheets_job, 
    transaction_processing_job,
    active_token_job  # New import for active token job
)

# Import resources
from solana_pipeline.resources.api import birdeye_api_resource
from solana_pipeline.resources.database import db_resource
from solana_pipeline.resources.google_sheets import google_sheets_resource
from solana_pipeline.resources.slack import slack_resource

# Load main assets
main_assets = load_assets_from_modules([
    tokens, whales, webhook, txn_process, active_tokens, active_tokens  # Added active_tokens
])

# Combine with Google Sheets assets
all_assets = main_assets + gsheet_assets

# Schedule for Google Sheets export
google_sheets_schedule = ScheduleDefinition(
    name="google_sheets_daily",
    cron_schedule="0 6 * * *",  # Daily at 6 AM
    job=google_sheets_job,
    execution_timezone="UTC",
    description="Export data to Google Sheets daily at 6 AM"
)



# Schedule for Active Token Detection
active_token_schedule = ScheduleDefinition(
    name="active_token_hourly",
    cron_schedule="0 * * * *",  # Every hour
    job=active_token_job,
    execution_timezone="UTC",
    description="Detect active tokens from transaction data every hour"
)

# Schedule for transaction processing
transaction_processing_schedule = ScheduleDefinition(
    name="transaction_processing_frequent",
    cron_schedule="*/3 * * * *",  # Every 3 minutes
    job=transaction_processing_job,
    execution_timezone="UTC",
    description="Process incoming transaction webhooks every 3 minutes"
)

# Definitions
defs = Definitions(
    assets=all_assets,
    jobs=[
        complete_pipeline_job, 
        google_sheets_job, 
        transaction_processing_job,
        active_token_job  # Added active token job
    ],
    schedules=[
        google_sheets_schedule,
        active_token_schedule,  # Added active token schedule
        transaction_processing_schedule
    ],
    resources={
        "db": db_resource,
        "birdeye_api": birdeye_api_resource,
        "google_sheets": google_sheets_resource,
        "slack": slack_resource,
    }
)