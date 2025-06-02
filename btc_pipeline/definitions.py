"""
Bitcoin pipeline Dagster definitions - Fixed for ConfigurableResource
"""
from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
)

from btc_pipeline.assets import btc_fng, btc_oi
from common.resources import DatabaseResource, UnifiedScraperResource

# Load all assets from modules
btc_assets = load_assets_from_modules([
    btc_fng, 
    btc_oi
    ])


# Define jobs
btc_market_indicators_job = define_asset_job(
    name="btc_market_indicators_job",
    selection=AssetSelection.groups("market_indicators"),
    description="Scrape and analyze Bitcoin market indicators"
)

# Define schedules
btc_hourly_schedule = ScheduleDefinition(
    job=btc_market_indicators_job,
    cron_schedule="0 * * * *",  # Every hour
    execution_timezone="UTC",
)

# Configure resources - ConfigurableResource uses direct instantiation
resources = {
    "db": DatabaseResource(
        setup_db=True,
        echo=False,
    ),
    "unified_scraper": UnifiedScraperResource(
        headless=False,  # Change to True for headless
        visible=False,  # Change to False for headless
        no_sandbox=True,  # Already set correctly
        timeout=60,  # Increase timeout
    ),
}

# Create Dagster definitions
btc_pipeline_defs = Definitions(
    assets=btc_assets,
    resources=resources,
    jobs=[btc_market_indicators_job],
    schedules=[btc_hourly_schedule],
)