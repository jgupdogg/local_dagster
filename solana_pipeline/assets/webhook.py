"""Assets for managing Helius webhooks."""
from dagster import asset, AssetExecutionContext, Config, Field, Definitions, define_asset_job
import logging
import pandas as pd
import os
from typing import List
from sqlalchemy import text
from helius import WebhooksAPI  # Direct import of the official SDK


class WebhookConfig(Config):
    max_addresses: int = 1000
    webhook_url: str = None


@asset(
    required_resource_keys={"db"},  # We're not using helius_api resource
    deps=["top_traders"],  # This asset depends on the top_traders asset
    group_name="gold"  # This belongs to the gold tier
)
def helius_webhook(context: AssetExecutionContext, config: WebhookConfig) -> None:
    """
    Fetch tracked addresses from the database and update the Helius webhook.
    Uses the first existing webhook found or creates a new one if webhook_url provided.
    """
    logger = context.log
    logger.info("Starting Helius webhook update...")
    
    # Get database resource
    db = context.resources.db
    
    # Get API key directly from environment (like in your example)
    api_key = os.getenv("HELIUS_API_KEY")
    if not api_key:
        logger.error("HELIUS_API_KEY is not set.")
        context.add_output_metadata(metadata={"status": "error", "message": "HELIUS_API_KEY is not set."})
        return
    
    # Get configuration
    max_addresses = config.max_addresses
    new_webhook_url = config.webhook_url
    
    # Get configuration from environment variables
    transaction_types_env = os.getenv("HELIUS_TRANSACTION_TYPES", "SWAP")
    transaction_types = [x.strip() for x in transaction_types_env.split(",")]
    webhook_type = os.getenv("HELIUS_WEBHOOK_TYPE", "enhanced")
    auth_header = os.getenv("HELIUS_AUTH_HEADER", "")
    
    # Fetch addresses from the database
    account_addresses = fetch_addresses_from_db(db, logger)
    
    if not account_addresses:
        logger.warning("No account addresses were found in the database.")
        context.add_output_metadata(metadata={"status": "no_addresses", "message": "No account addresses were found."})
        return
    
    # Limit addresses if needed
    if len(account_addresses) > max_addresses:
        logger.warning(f"Limiting addresses from {len(account_addresses)} to {max_addresses}")
        account_addresses = account_addresses[:max_addresses]
    
    logger.info(f"Using {len(account_addresses)} trader addresses for webhook update.")
    
    try:
        # Initialize the Helius SDK directly like in your example
        webhooks_api = WebhooksAPI(api_key)
        
        # Get existing webhooks
        existing_webhooks = webhooks_api.get_all_webhooks()
        logger.info(f"Found {len(existing_webhooks) if existing_webhooks else 0} existing webhooks")
        
        webhook_id = None
        webhook_url = None
        
        if existing_webhooks and len(existing_webhooks) > 0:
            # If webhooks exist, update the first one
            webhook_id = existing_webhooks[0].get("webhookID")
            webhook_url = existing_webhooks[0].get("webhookURL")
            
            if webhook_id and webhook_url:
                logger.info(f"Updating existing webhook with ID: {webhook_id}")
                result = webhooks_api.edit_webhook(
                    webhook_id,
                    webhook_url,
                    transaction_types,
                    account_addresses,
                    webhook_type,
                    auth_header
                )
                logger.info(f"Webhook update result: {result}")
            else:
                logger.warning("Existing webhook found but missing ID or URL")
        elif new_webhook_url:
            # If no webhooks exist, create a new one
            logger.info(f"Creating new webhook with URL: {new_webhook_url}")
            result = webhooks_api.create_webhook(
                new_webhook_url,
                transaction_types,
                account_addresses,
                webhook_type,
                auth_header
            )
            logger.info(f"Webhook creation result: {result}")
            
            webhook_id = result.get("webhookID")
            webhook_url = new_webhook_url
        else:
            logger.error("No existing webhooks found and no webhook URL provided")
            context.add_output_metadata(metadata={
                "status": "error", 
                "message": "No existing webhooks found and no webhook URL provided"
            })
            return
        
        # Update database to mark addresses as added to webhook
        updated_count = update_db_webhook_status(db, logger, account_addresses)
        
        # Add metadata about the webhook update
        context.add_output_metadata(metadata={
            "status": "success",
            "webhook_id": webhook_id or "unknown",
            "addresses_count": len(account_addresses),
            "webhook_url": webhook_url,
            "updated_records": updated_count
        })
        
    except Exception as e:
        logger.error(f"Error managing webhook: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        context.add_output_metadata(metadata={
            "status": "error",
            "message": str(e)
        })
        raise


def fetch_addresses_from_db(db, logger) -> List[str]:
    """
    Fetch trader addresses from the database.
    """
    try:
        # Query top traders from the gold schema
        query = """
        SELECT DISTINCT
            wallet_address as address,
            created_at
        FROM 
            gold.top_traders
        WHERE 
            created_at > (CURRENT_DATE - INTERVAL '30 days')
            AND total_pnl > 0
            AND trade_frequency_daily < 10
            AND trade_frequency_daily > 0.25
            AND win_rate > 0.5
        ORDER BY 
            created_at DESC;
        """
        
        # Use get_connection instead of execute_query
        with db.get_connection() as conn:
            result = conn.execute(text(query))
            traders = result.fetchall()
        
        if not traders:
            logger.warning("No matching traders found in the gold.top_traders table.")
            return []
            
        # Extract addresses from result (first column is the address)
        addresses = [row[0] for row in traders]
        logger.info(f"Fetched {len(addresses)} unique trader addresses from gold.top_traders.")
        return addresses
        
    except Exception as e:
        logger.error(f"Error fetching trader addresses: {str(e)}")
        raise


def update_db_webhook_status(db, logger, addresses):
    """Update database to mark addresses as added to webhook."""
    try:
        with db.get_connection() as conn:
            # Use parameterized query for safety
            update_query = text("""
            UPDATE gold.top_traders
            SET 
                added_to_webhook = TRUE,
                added_at = CURRENT_TIMESTAMP
            WHERE 
                wallet_address = ANY(:addresses)
                AND (added_to_webhook = FALSE OR added_to_webhook IS NULL)
            """)
            
            update_result = conn.execute(update_query, {"addresses": addresses})
            updated_count = update_result.rowcount
            logger.info(f"Updated {updated_count} records in top_traders with webhook status")
            return updated_count
            
    except Exception as e:
        logger.error(f"Error updating top_traders table: {str(e)}")
        return 0

