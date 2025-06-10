"""
Engineering pipeline Dagster definitions
"""
from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    AssetSelection,
    EnvVar,
)

from engineering_pipeline import assets
from common.resources import DatabaseResource, UnifiedScraperResource, google_sheets_resource

# Load all assets from modules
engineering_assets = load_assets_from_modules([assets])

# Define jobs
emma_scraping_job = define_asset_job(
    name="emma_scraping_job",
    selection=AssetSelection.groups("engineering_data"),
    description="Scrape EMMA public solicitations data"
)

emma_gsheets_export_job = define_asset_job(
    name="emma_gsheets_export_job",
    selection=AssetSelection.groups("google_sheets_emma"),
    description="Export EMMA gold data to Google Sheets"
)

# Configure resources
resources = {
    "db": DatabaseResource(
        database_url="postgresql://postgres:St0ck!adePG@localhost:5432/engineering",  # Use postgres credentials
        setup_db=True,  # Enable DB setup to create schemas and tables
        echo=False,
    ),
    "unified_scraper": UnifiedScraperResource(
        headless=False,  # Show browser for debugging
        visible=True,    # Make display visible
        no_sandbox=True,
        timeout=60,
    ),
    "google_sheets": google_sheets_resource.configured({
        "service_account_key_path": EnvVar("GOOGLE_SHEETS_KEY_PATH"),
        "spreadsheet_id": EnvVar("GOOGLE_SHEETS_SPREADSHEET_ID"),
    }),
}

# Create Dagster definitions
engineering_pipeline_defs = Definitions(
    assets=engineering_assets,
    resources=resources,
    jobs=[emma_scraping_job, emma_gsheets_export_job],
)