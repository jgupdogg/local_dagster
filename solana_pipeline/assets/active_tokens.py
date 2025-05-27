# File: solana_pipeline/assets/active_tokens.py

import json
import traceback
from datetime import datetime, timedelta
from sqlalchemy import text
from dagster import asset, AssetExecutionContext

from solana_pipeline.queries.alpha_signals import get_active_tokens
from solana_pipeline.models.solana_models import AlphaSignal
from solana_pipeline.notifications.token_messages import format_active_token_message  # Import the formatter

@asset(
    key="active_token_notification",
    compute_kind="python",
    io_manager_key="io_manager",
    description="Detects active tokens and sends simple notifications",
    required_resource_keys={"db", "slack"}
)
def active_token_notification(context):
    """
    Detect active tokens from recent transactions and send simple notifications.
    """
    # Connect to database
    db_manager = context.resources.db
    now = datetime.utcnow()
    
    try:
        with db_manager.get_connection() as conn:
            # 1. Get active tokens from our query
            active_tokens = get_active_tokens(conn)
            context.log.info(f"Found {len(active_tokens)} active tokens")
            
            notifications_sent = 0
            
            # 2. Process each token
            for token in active_tokens[:5]:  # Just process top 5 tokens
                # Skip tokens with no data
                if not token['token_address'] or (not token['unique_buyers'] and not token['unique_sellers']):
                    continue
                
                # 3. Create message using the formatter
                message = format_active_token_message(token)
                
                # 4. Check if we already notified about this token recently
                check_query = text("""
                SELECT 1 FROM gold.alpha_signals
                WHERE token_address = :token_address
                AND detected_at > :cutoff_time
                LIMIT 1
                """)
                
                cutoff_time = now - timedelta(hours=24)
                existing = conn.execute(check_query, {
                    "token_address": token['token_address'],
                    "cutoff_time": cutoff_time
                }).fetchone()
                
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
                    
                    # 8. Create and save the signal
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
                        
                        detected_at=now,
                        created_at=now,
                        updated_at=now
                    )
                    
                    # Convert to JSON for db storage
                    alpha_wallets_json = json.dumps([])
                    solid_wallets_json = json.dumps([])
                    tracking_wallets_json = json.dumps([])
                    
                    # Insert into DB
                    insert_query = text("""
                    INSERT INTO gold.alpha_signals (
                        token_address, symbol, name, signal_type, confidence,
                        alpha_tier_count, solid_tier_count, tracking_tier_count, total_wallet_count,
                        alpha_wallets, solid_wallets, tracking_wallets,
                        total_volume, weighted_score,
                        window_hours, first_transaction_time, latest_transaction_time,
                        notification_sent, detected_at, created_at, updated_at
                    ) VALUES (
                        :token_address, :symbol, :name, :signal_type, :confidence,
                        0, 0, 0, :total_wallet_count,
                        :alpha_wallets, :solid_wallets, :tracking_wallets,
                        0, :weighted_score,
                        24, :first_transaction_time, :latest_transaction_time,
                        true, :detected_at, :created_at, :updated_at
                    )
                    """)
                    
                    conn.execute(insert_query, {
                        "token_address": signal.token_address,
                        "symbol": signal.symbol,
                        "name": signal.name,
                        "signal_type": signal.signal_type,
                        "confidence": signal.confidence,
                        "total_wallet_count": signal.total_wallet_count,
                        "alpha_wallets": alpha_wallets_json,
                        "solid_wallets": solid_wallets_json,
                        "tracking_wallets": tracking_wallets_json,
                        "weighted_score": signal.weighted_score,
                        "first_transaction_time": signal.first_transaction_time,
                        "latest_transaction_time": signal.latest_transaction_time,
                        "detected_at": signal.detected_at,
                        "created_at": signal.created_at,
                        "updated_at": signal.updated_at
                    })
                    
                    # 9. Send notification
                    if context.resources.slack:
                        try:
                            response = context.resources.slack.get_client().chat_postMessage(
                                channel="#alpha-notifications",
                                text=message
                            )
                            context.log.info(f"Sent notification for {symbol}, Slack status: {response.get('ok', False)}")
                            notifications_sent += 1
                            
                        except Exception as e:
                            context.log.error(f"Failed to send Slack notification: {e}")
                            context.log.error(traceback.format_exc())
                    else:
                        context.log.info("Slack resource not available. Skipping notification.")
            
            return {
                "tokens_found": len(active_tokens),
                "notifications_sent": notifications_sent
            }
    
    except Exception as e:
        context.log.error(f"Error in active token notification: {e}")
        context.log.error(traceback.format_exc())
        return {"tokens_found": 0, "notifications_sent": 0, "error": str(e)}