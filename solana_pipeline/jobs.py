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

# Transaction processing job - REVISED to remove the transaction_notification asset
transaction_processing_job = define_asset_job(
    name="transaction_processing_job",
    description="Process unprocessed webhook data into clean transaction records",
    selection=AssetSelection.assets(
        "unprocessed_webhook_data", 
        "processed_transactions"
        # Removed "transaction_notification" since we're using enhanced_notification instead
    )
)

# Alpha detection job - simplified for active token signals
active_token_job = define_asset_job(
    name="active_token_job",
    description="Detect active token signals based on transaction activity and send notifications",
    selection=AssetSelection.assets(
        "active_token_notification",
    ),
    # No config needed for this simplified version
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