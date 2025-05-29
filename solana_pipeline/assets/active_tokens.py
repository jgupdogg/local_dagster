# File: solana_pipeline/assets/active_tokens.py

import json
import traceback
from datetime import datetime, timedelta
from sqlmodel import select
from dagster import asset, AssetExecutionContext, AssetIn

from solana_pipeline.queries.alpha_signals import get_active_tokens
from solana_pipeline.models.solana_models import AlphaSignal
from solana_pipeline.notifications.token_messages import format_active_token_message  # Import the formatter

@asset(
    key="active_token_notification",
    ins={
        "processed_txns": AssetIn("processed_transactions")
    },
    compute_kind="python",
    io_manager_key="io_manager",
    description="Detects active tokens and sends simple notifications",
    required_resource_keys={"db", "slack"}
)
def active_token_notification(context, processed_txns):
    """
    Detect active tokens from recent transactions and send simple notifications.
    
    Args:
        context: Dagster execution context
        processed_txns: Output from processed_transactions asset (ensures dependency)
    """
    # Log the dependency
    if processed_txns:
        context.log.info(f"Processing active tokens after {processed_txns.get('processed_count', 0)} transactions were processed")
    
    # Connect to database
    db_manager = context.resources.db
    now = datetime.utcnow()
    
    try:
        # Use SQLModel session for cleaner operations
        with db_manager.get_session() as session:
            # 1. Get active tokens from our query (still need connection for custom query)
            with db_manager.get_connection() as conn:
                active_tokens = get_active_tokens(conn)
            
            context.log.info(f"Found {len(active_tokens)} active tokens")
            
            notifications_sent = 0
            notifications_failed = 0
            signals_saved = 0
            
            # 2. Process each token
            for token in active_tokens[:5]:  # Just process top 5 tokens
                # Skip tokens with no data
                if not token['token_address'] or (not token['unique_buyers'] and not token['unique_sellers']):
                    continue
                
                # 3. Create message using the formatter
                message = format_active_token_message(token)
                
                # 4. Check if we already notified about this token recently using SQLModel
                cutoff_time = now - timedelta(hours=24)
                
                existing = session.exec(
                    select(AlphaSignal).where(
                        AlphaSignal.token_address == token['token_address'],
                        AlphaSignal.detected_at > cutoff_time
                    )
                ).first()
                
                # 5. If no recent notification, create one
                if not existing:
                    # 6. Map signal type
                    if token['signal_type'] == 'most_unique_buyers':
                        signal_type = 'high_buying_activity'
                        confidence = 80
                    elif token['signal_type'] == 'most_unique_sellers':
                        signal_type = 'high_selling_activity'
                        confidence = 75
                    elif token['signal_type'] == 'most_net_buyers':
                        signal_type = 'net_accumulation'
                        confidence = 85
                    else:
                        signal_type = 'token_activity'
                        confidence = 70
                    
                    # 7. Get symbol with fallback
                    symbol = token.get('symbol') or f"Unknown-{token['token_address'][:6]}"
                    name = token.get('name') or f"Unknown Token ({token['token_address'][:8]}...)"
                    
                    # Flag to track if notification was sent
                    notification_sent = False
                    
                    # 9. Try to send notification (but don't fail if it doesn't work)
                    if context.resources.slack:
                        try:
                            response = context.resources.slack.get_client().chat_postMessage(
                                channel="#alpha-notifications",
                                text=message
                            )
                            if response.get('ok', False):
                                context.log.info(f"Sent notification for {symbol}")
                                notifications_sent += 1
                                notification_sent = True
                            else:
                                context.log.warning(f"Slack returned not ok: {response}")
                                notifications_failed += 1
                            
                        except Exception as e:
                            # Check if it's a token type error
                            if 'not_allowed_token_type' in str(e):
                                context.log.warning(
                                    "Slack token appears to be incorrect type. "
                                    "Please use a Bot User OAuth Token (starts with xoxb-) "
                                    "and ensure it has chat:write scope."
                                )
                            else:
                                context.log.warning(f"Failed to send Slack notification: {e}")
                            notifications_failed += 1
                            # Continue processing - don't fail the entire job
                    else:
                        context.log.info("Slack resource not available. Skipping notification.")
                    
                    # 8. Create and save the signal using SQLModel
                    signal = AlphaSignal(
                        token_address=token['token_address'],
                        symbol=symbol,
                        name=name,
                        signal_type=signal_type,
                        confidence=confidence,
                        
                        # Simple values for the rest
                        alpha_tier_count=0,
                        solid_tier_count=0,
                        tracking_tier_count=0,
                        total_wallet_count=token.get('unique_buyers', 0) + token.get('unique_sellers', 0),
                        
                        alpha_wallets=[],
                        solid_wallets=[],
                        tracking_wallets=[],
                        
                        total_volume=0.0,
                        weighted_score=token.get('score', 0),
                        
                        window_hours=24,
                        first_transaction_time=now - timedelta(hours=24),
                        latest_transaction_time=now,
                        
                        notification_sent=notification_sent,
                        detected_at=now,
                        created_at=now,
                        updated_at=now
                    )
                    
                    # Add to session and commit
                    session.add(signal)
                    context.log.info(f"Saved signal for {symbol} (notification_sent={notification_sent})")
                    signals_saved += 1
            
            # Commit all signals at once
            session.commit()
            context.log.info(f"Committed {signals_saved} signals to database")
            
            # Log summary
            context.log.info(
                f"Processing complete: {len(active_tokens)} tokens found, "
                f"{signals_saved} signals saved, "
                f"{notifications_sent} notifications sent, "
                f"{notifications_failed} notifications failed"
            )
            
            return {
                "tokens_found": len(active_tokens),
                "signals_saved": signals_saved,
                "notifications_sent": notifications_sent,
                "notifications_failed": notifications_failed
            }
    
    except Exception as e:
        context.log.error(f"Error in active token notification: {e}")
        context.log.error(traceback.format_exc())
        return {"tokens_found": 0, "signals_saved": 0, "notifications_sent": 0, "error": str(e)}