# engineering_pipeline/assets/google_sheets.py
import os
import pandas as pd
import json
from typing import Dict, List, Callable
from dagster import asset, AssetExecutionContext, Config

# Define the table configurations for EMMA gold tables
SHEET_CONFIGS = [
    {
        "asset_name": "emma_solicitations_to_gsheets",
        "description": "Export EMMA solicitations gold data to Google Sheets",
        "table": "gold.emma_solicitations_gold",
        "worksheet_env_var": "GOOGLE_SHEETS_EMMA_SOLICITATIONS_WORKSHEET_NAME",
        "order_by": "created_at DESC",
        "limit": 2000  # Increased limit for comprehensive data
    },
    {
        "asset_name": "emma_contracts_to_gsheets",
        "description": "Export EMMA contracts gold data to Google Sheets",
        "table": "gold.emma_contracts_gold",
        "worksheet_env_var": "GOOGLE_SHEETS_EMMA_CONTRACTS_WORKSHEET_NAME",
        "order_by": "created_at DESC",
        "limit": 2000
    }
]


def create_gsheets_asset(config: Dict):
    @asset(
        name=config["asset_name"],
        description=config["description"],
        required_resource_keys={"db", "google_sheets"},
        group_name="google_sheets_emma"
    )
    def _asset(context: AssetExecutionContext):
        """Export EMMA data from database to Google Sheets."""
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

        # Query with proper JSON handling for complex fields
        query = f"""
            SELECT 
                id,
                solicitation_id,
                source_silver_id,
                detail_url,
                title,
                bmp_id,
                alternate_id,
                lot_number,
                round_number,
                status,
                due_date,
                solicitation_type,
                main_category,
                issuing_agency,
                procurement_officer,
                email,
                solicitation_summary,
                additional_instructions,
                project_cost_class,
                mbe_participation_pct,
                african_american_pct,
                asian_american_pct,
                hispanic_american_pct,
                women_owned_pct,
                dbe_participation_pct,
                sbe_participation_pct,
                vsbe_participation_pct,
                small_business_reserve,
                CASE 
                    WHEN solicitation_links IS NOT NULL 
                    THEN solicitation_links::text 
                    ELSE NULL 
                END as solicitation_links,
                CASE 
                    WHEN attachments IS NOT NULL 
                    THEN attachments::text 
                    ELSE NULL 
                END as attachments,
                processed_at,
                created_at,
                updated_at
            FROM {config['table']}
            ORDER BY {config['order_by']}
            LIMIT {config['limit']}
        """ if "solicitations" in config["asset_name"] else f"""
            SELECT 
                id,
                contract_id,
                source_silver_id,
                detail_url,
                contract_title,
                alternate_id,
                contract_type,
                effective_date,
                expiration_date,
                contract_amount,
                currency,
                vendor_name,
                procurement_officer,
                contact_email,
                agency_org,
                commodities,
                vsbe_goal_percentage,
                linked_solicitation,
                contract_scope,
                documents_available,
                processed_at,
                created_at,
                updated_at
            FROM {config['table']}
            ORDER BY {config['order_by']}
            LIMIT {config['limit']}
        """

        try:
            with db.get_connection() as conn:
                df = pd.read_sql(query, conn)

            if df.empty:
                context.log.info(f"No data found in {config['table']} to export.")
                return "No data exported."

            # Process JSON fields if they exist
            for json_col in ['solicitation_links', 'attachments']:
                if json_col in df.columns:
                    df[json_col] = df[json_col].apply(
                        lambda x: json.dumps(x) if x and x != 'null' else ''
                    )

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
emma_gsheet_assets = [create_gsheets_asset(config) for config in SHEET_CONFIGS]

# Make the assets available for import
__all__ = ["emma_gsheet_assets"]