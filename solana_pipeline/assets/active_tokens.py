"""Active token detection and notification asset."""
from datetime import datetime, timedelta
from sqlalchemy import text
from dagster import asset, AssetExecutionContext, AssetKey

from solana_pipeline.models.solana_models import AlphaSignal
from solana_pipeline.notifications.token_messages import format_active_token_message

# Schema constants
GOLD = "gold"

# Signal type mapping
SIGNAL_MAPPING = {
    'most_unique_buyers': ('initial_accumulation', 80),
    'most_unique_sellers': ('distribution', 75),
    'most_net_buyers': ('strong_accumulation', 85)
}


@asset(
    key="active_token_notification",
    deps=[AssetKey([GOLD, "mart_active_tokens"])],  # Use deps instead of ins
    required_resource_keys={"db", "slack"}
)
def active_token_notification(context: AssetExecutionContext):
    """Detect active tokens from DBT mart and create notifications."""
    db = context.resources.db
    slack = context.resources.slack
    now = datetime.utcnow()
    cutoff = now - timedelta(hours=24)
    
    with db.get_session() as session:
        # Check which schema the table actually exists in
        with db.get_connection() as conn:
            # First check if table exists
            check_result = conn.execute(text("""
                SELECT schemaname, tablename 
                FROM pg_tables 
                WHERE tablename = 'mart_active_tokens'
            """))
            tables = check_result.fetchall()
            
            if not tables:
                context.log.warning("Table mart_active_tokens not found. DBT models may not have run yet.")
                return {
                    "tokens_processed": 0,
                    "signals_created": 0,
                    "notifications_sent": 0
                }
            
            # Use the actual schema name found
            schema_name = tables[0][0]
            context.log.info(f"Found mart_active_tokens in schema: {schema_name}")
            
            # Get active tokens from DBT mart
            result = conn.execute(text(f"""
                SELECT * FROM {schema_name}.mart_active_tokens
                ORDER BY score DESC
                LIMIT 5
            """))
            tokens = result.mappings().all()
        
        # Process tokens
        signals_created = []
        notifications_sent = 0
        
        for token in tokens:
            # Skip if already notified
            existing = session.query(AlphaSignal).filter(
                AlphaSignal.token_address == token['token_address'],
                AlphaSignal.detected_at > cutoff
            ).first()
            
            if existing:
                continue
            
            # Map signal type
            signal_type, confidence = SIGNAL_MAPPING.get(
                token['signal_type'], 
                ('unknown', 70)
            )
            
            # Send notification
            notification_sent = False
            if slack:
                try:
                    response = slack.get_client().chat_postMessage(
                        channel="#alpha-notifications",
                        text=format_active_token_message(token)
                    )
                    notification_sent = response.get('ok', False)
                    notifications_sent += notification_sent
                except Exception as e:
                    context.log.warning(f"Slack error: {e}")
            
            # Create signal record
            signal = AlphaSignal(
                token_address=token['token_address'],
                symbol=token.get('symbol', 'Unknown'),
                name=token.get('name', 'Unknown Token'),
                signal_type=signal_type,
                confidence=confidence,
                
                # Activity metrics
                total_wallet_count=token.get('unique_buyers', 0) + token.get('unique_sellers', 0),
                weighted_score=float(token.get('score', 0)),
                
                # Default values for unused fields
                alpha_tier_count=0,
                solid_tier_count=0,
                tracking_tier_count=0,
                alpha_wallets=[],
                solid_wallets=[],
                tracking_wallets=[],
                total_volume=0.0,
                
                # Time window
                window_hours=24,
                first_transaction_time=cutoff,
                latest_transaction_time=token.get('latest_activity', now),
                
                # Notification status
                notification_sent=notification_sent,
                notification_sent_at=now if notification_sent else None,
                
                # Timestamps
                detected_at=now,
                created_at=now,
                updated_at=now
            )
            
            session.add(signal)
            signals_created.append(signal.token_address)
        
        session.commit()
        
        return {
            "tokens_processed": len(tokens),
            "signals_created": len(signals_created),
            "notifications_sent": notifications_sent
        }