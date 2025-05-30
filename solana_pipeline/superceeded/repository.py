"""Updated repository configuration including Slack and Alpha Detection."""
from dagster import repository, ScheduleDefinition, load_assets_from_modules, with_resources

# Import asset modules
from solana_pipeline.assets import tokens, whales, webhook, txn_process
from solana_pipeline.assets.google import gsheet_assets
from solana_pipeline.assets import active_tokens  # Import active tokens module once

# Import jobs 
from solana_pipeline.superceeded.jobs import (
    complete_pipeline_job,
    google_sheets_job,
    transaction_processing_job,
    active_token_job
)

# Import resources
from solana_pipeline.resources import db_resource, birdeye_api_resource
from solana_pipeline.resources.google_sheets import google_sheets_resource
from solana_pipeline.resources.slack import slack_resource

# Load main assets (now including active tokens)
main_assets = load_assets_from_modules([tokens, whales, webhook, txn_process, active_tokens])

# Combine with Google Sheets assets
solana_assets = main_assets + gsheet_assets

# Update schedule for the complete pipeline to run every 8 hours
complete_pipeline_schedule = ScheduleDefinition(
    name="complete_pipeline_every_8hours",
    cron_schedule="0 */8 * * *",  # Every 8 hours (12am, 8am, 4pm)
    job=complete_pipeline_job,
    execution_timezone="UTC",
    description="Run the complete Solana pipeline every 8 hours"
)

# Define Google Sheets schedule
google_sheets_schedule = ScheduleDefinition(
    name="google_sheets_daily",
    cron_schedule="0 6 * * *",  # Daily at 6 AM
    job=google_sheets_job,
    execution_timezone="UTC",
    description="Export data to Google Sheets daily at 6 AM"
)

# Define Active Token Detection schedule - runs every hour
active_token_schedule = ScheduleDefinition(
    name="active_token_hourly",
    cron_schedule="0 * * * *",  # Every hour
    job=active_token_job,
    execution_timezone="UTC",
    description="Detect active tokens from transaction data every hour"
)

# Resource configurations
resource_configs = {
    "db": db_resource.configured({
        "db_name": {"env": "DB_NAME"},
        "db_host": {"env": "DB_HOST"},
        "db_port": {"env": "DB_PORT"},
        "db_user": {"env": "DB_USER"},
        "db_password": {"env": "DB_PASSWORD"},
        "setup_db": True
    }),
    "birdeye_api": birdeye_api_resource,
    "google_sheets": google_sheets_resource,
    "slack": slack_resource.configured({
        "webhook_url": {"env": "SLACK_WEBHOOK_URL"},
        "default_channel": "#alpha-notifications",
        "username": "Solana Alpha Bot"
    })
}

# Define repository
@repository
def solana_repository():
    """Solana Pipeline repository with Alpha Detection."""
    return [
        # All assets with resources applied
        with_resources(solana_assets, resource_defs=resource_configs),
        
        # Jobs
        complete_pipeline_job,
        google_sheets_job,
        transaction_processing_job,
        active_token_job,  
                
        # Schedules
        complete_pipeline_schedule,
        google_sheets_schedule,
        active_token_schedule,
    ]