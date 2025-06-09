# solana_pipeline/assets/google.py
import os
import pandas as pd
from typing import Dict, List, Callable
from dagster import asset, AssetExecutionContext, Config

# Define the table configurations
# solana_pipeline/assets/google.py

# Define the table configurations with full schema qualifications
SHEET_CONFIGS = [
    {
        "asset_name": "wallet_pnl_to_gsheets",
        "description": "Export wallet_pnl data to Google Sheets",
        "table": "solana_pipeline.silver.wallet_pnl",  # Updated with full schema
        "worksheet_env_var": "GOOGLE_SHEETS_TRADERS_WORKSHEET_NAME",
        "order_by": "processed_at DESC",
        "limit": 1000
    },
    {
        "asset_name": "top_swaps_to_gsheets",
        "description": "Export top_swaps data to Google Sheets",
        "table": "solana_pipeline.silver.helius_txns_clean",  # Updated with full schema
        "worksheet_env_var": "GOOGLE_SHEETS_SWAPS_WORKSHEET_NAME",
        "order_by": "timestamp DESC",
        "limit": 1000
    },
    {
        "asset_name": "token_creation_to_gsheets",
        "description": "Export token creation info to Google Sheets",
        "table": "solana_pipeline.bronze.token_creation_info",  # Updated with full schema
        "worksheet_env_var": "GOOGLE_SHEETS_CREATION_WORKSHEET_NAME",
        "order_by": "scraped_at DESC",
        "limit": 1000
    },
    {
        "asset_name": "x_followers_to_gsheets", 
        "description": "Export X followers data to Google Sheets",
        "table": "solana_pipeline.bronze.x_followers",  # Updated with full schema
        "worksheet_env_var": "GOOGLE_SHEETS_FOLLOWERS_WORKSHEET_NAME",
        "order_by": "scraped_at DESC",
        "limit": 1000
    },
    {
        "asset_name": "token_metadata_to_gsheets",
        "description": "Export token metadata to Google Sheets",
        "table": "solana_pipeline.bronze.token_metadata",  # Updated with full schema
        "worksheet_env_var": "GOOGLE_SHEETS_METADATA_WORKSHEET_NAME",
        "order_by": "created_at DESC",
        "limit": 1000
    },
    {
        "asset_name": "solsniffer_data_to_gsheets",
        "description": "Export Solsniffer token data to Google Sheets",
        "table": "solana_pipeline.bronze.solsniffer_token_data",  # Updated with full schema
        "worksheet_env_var": "GOOGLE_SHEETS_SOLSNIFFER_WORKSHEET_NAME",
        "order_by": "scraped_at DESC",
        "limit": 1000
    }
]





def create_gsheets_asset(config: Dict):
    @asset(
        name=config["asset_name"],
        description=config["description"],
        required_resource_keys={"db", "google_sheets"},
        group_name="google_sheets"
    )
    def _asset(context: AssetExecutionContext):
        """Export data from database to Google Sheets."""
        db = context.resources.db
        google_sheets = context.resources.google_sheets

        # Get worksheet name from environment variable
        worksheet_name = os.getenv(config["worksheet_env_var"])
        if not worksheet_name:
            raise ValueError(
                f"Worksheet name not found in environment variable "
                f"{config['worksheet_env_var']}"
            )

        context.log.info(
            f"Exporting data from {config['table']} to worksheet {worksheet_name}"
        )

        # If this is wallet_pnl_to_gsheets, add a WHERE filter to exclude NULL/NaN
        if config["asset_name"] == "wallet_pnl_to_gsheets":
            query = f"""
                SELECT *
                FROM {config['table']}
                WHERE
                  realized_pnl IS NOT NULL
                  AND realized_pnl::text <> 'NaN'
                ORDER BY {config['order_by']}
                LIMIT {config['limit']}
            """
        else:
            query = (
                f"SELECT * FROM {config['table']} "
                f"ORDER BY {config['order_by']} "
                f"LIMIT {config['limit']}"
            )

        try:
            with db.get_connection() as conn:
                df = pd.read_sql(query, conn)

            if df.empty:
                context.log.info(f"No data found in {config['table']} to export.")
                return "No data exported."

            # Log data shape
            context.log.info(f"Data shape: {df.shape}")

            # Update the worksheet
            result = google_sheets.update_worksheet(df, worksheet_name)

            # Log the result
            export_message = (
                f"Successfully exported {len(df)} rows and {len(df.columns)} columns "
                f"to worksheet '{worksheet_name}'."
            )
            context.log.info(export_message)

            # Add metadata for the asset
            context.add_output_metadata({
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "worksheet_name": worksheet_name,
                "updated_cells": result.get("updated_rows", 0)
                                 * result.get("updated_columns", 0)
            })

            return export_message

        except Exception as e:
            error_message = f"Error exporting {config['table']} to Google Sheets: {e}"
            context.log.error(error_message)
            raise

    return _asset


# Generate all the assets
gsheet_assets = [create_gsheets_asset(config) for config in SHEET_CONFIGS]

# Make the assets available for import
__all__ = ["gsheet_assets"]
