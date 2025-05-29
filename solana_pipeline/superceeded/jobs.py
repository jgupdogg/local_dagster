# solana_pipeline/jobs.py
from dagster import define_asset_job, AssetSelection

# Original jobs (unchanged)
complete_pipeline_job = define_asset_job(
    name="complete_pipeline_job",
    description="Process tokens, whales, and webhook data",
    selection=AssetSelection.assets(
        "token_list_v3", "trending_tokens", "tracked_tokens",
        "token_whales", "wallet_trade_history", "wallet_pnl", 
        "top_traders", "helius_webhook"
    )
)


# Google Sheets export job (unchanged)
google_sheets_job = define_asset_job(
    name="google_sheets_job",
    description="Export data to Google Sheets",
    selection=AssetSelection.groups("google_sheets")
)

# Updated transaction_processing_job definition to include in jobs.py

transaction_processing_job = define_asset_job(
    name="transaction_processing_job",
    description="Process unprocessed webhook data into clean transaction records and enrich token metadata",
    selection=AssetSelection.assets(
        # Original assets
        "unprocessed_webhook_data",
        "processed_transactions",
        # New token enrichment assets
        "tokens_needing_metadata",
        "fetch_token_metadata", 
        "fetch_token_security",
        "fetch_token_creation", 
        "active_token_notification",
""
    )
)


# Comprehensive alpha pipeline job
comprehensive_alpha_job = define_asset_job(
    name="comprehensive_alpha_job",
    description="Process transactions and detect alpha signals in a single pipeline run",
    selection=AssetSelection.assets(
        "unprocessed_webhook_data", 
        "processed_transactions",
        "alpha_signal_detection", 
        "enhanced_notification"
    )
)