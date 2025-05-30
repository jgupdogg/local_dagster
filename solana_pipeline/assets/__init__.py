"""Asset definitions for the Solana pipeline."""
# Import individual assets from modules
from solana_pipeline.assets.tokens import token_list_v3, trending_tokens, tracked_tokens
from solana_pipeline.assets.whales import token_whales, wallet_trade_history, wallet_pnl, top_traders
from solana_pipeline.assets.webhook import helius_webhook
from solana_pipeline.assets.google import gsheet_assets  # Now importing the list of generated assets
from solana_pipeline.assets.txn_process import unprocessed_webhook_data, processed_transactions
from solana_pipeline.assets.active_tokens import active_token_notification
from solana_pipeline.assets.token_enrichment import fetch_token_security, fetch_token_metadata, fetch_token_creation

# Export all assets
solana_assets = [
    # Bronze tier
    token_list_v3,
    trending_tokens,
    token_whales,
    wallet_trade_history,
    fetch_token_creation, 
    fetch_token_metadata, 
    fetch_token_security,
        
    # Silver tier
    tracked_tokens,
    wallet_pnl,
    
    # Gold tier
    top_traders,
    helius_webhook,
    
    # Google Sheets assets
    *gsheet_assets,  # Unpack all the generated Google Sheets assets
    
    # Transaction processing assets
    unprocessed_webhook_data,
    processed_transactions,
    # Alpha detection assets
    active_token_notification,
]