"""Transaction processing assets with hybrid DBT approach."""
import json
from datetime import datetime
from sqlalchemy import text
from sqlmodel import select

from dagster import asset, AssetExecutionContext, AssetKey

from solana_pipeline.models.solana_models import HeliusHook

# Schema constants
BRONZE = "bronze"
SILVER = "silver"

# DDL for staging table
STAGING_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {BRONZE}.webhook_staging (
    id SERIAL PRIMARY KEY,
    raw_id INTEGER NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(raw_id)
)
"""


@asset(
    key="webhook_staging_data",
    compute_kind="sql",
    required_resource_keys={"db"}
)
def webhook_staging_data(context: AssetExecutionContext):
    """Stage unprocessed webhook data for DBT transformation."""
    db = context.resources.db
    
    with db.get_connection() as conn:
        # Ensure staging table exists
        conn.execute(text(STAGING_TABLE_DDL))
        conn.commit()
        
        # Get unprocessed webhooks and process them within session
        staging_data = []
        with db.get_session() as session:
            hooks = session.exec(
                select(HeliusHook)
                .where(HeliusHook.processed == False)
                .order_by(HeliusHook.received_at.desc())
                .limit(100)
            ).all()
            
            # Process hooks while session is active
            for hook in hooks:
                try:
                    payload = hook.payload
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    
                    staging_data.append({
                        "raw_id": hook.id,
                        "payload": json.dumps(payload)
                    })
                except Exception as e:
                    context.log.warning(f"Skipping webhook {hook.id}: {e}")
                    continue
        
        # Bulk upsert
        if staging_data:
            conn.execute(
                text(f"""
                    INSERT INTO {BRONZE}.webhook_staging (raw_id, payload)
                    VALUES (:raw_id, :payload)
                    ON CONFLICT (raw_id) DO UPDATE
                    SET payload = EXCLUDED.payload,
                        created_at = CURRENT_TIMESTAMP
                """),
                staging_data
            )
            conn.commit()
        
        return {
            "staged_count": len(staging_data),
            "total_unprocessed": len(staging_data)
        }


@asset(
    key="mark_webhooks_processed",
    deps=[
        AssetKey("webhook_staging_data"),
        AssetKey([SILVER, "mart_txns_clean"])
    ],
    compute_kind="sql",
    required_resource_keys={"db"}
)
def mark_webhooks_processed(context: AssetExecutionContext):
    """Mark webhooks as processed after DBT transformation."""
    db = context.resources.db
    
    with db.get_connection() as conn:
        # Find processed webhooks
        result = conn.execute(text(f"""
            SELECT DISTINCT ws.raw_id
            FROM {BRONZE}.webhook_staging ws
            JOIN {SILVER}.helius_txns_clean htc ON htc.raw_id = ws.raw_id
        """))
        processed_ids = [row[0] for row in result]
        
        # Mark as processed
        if processed_ids:
            with db.get_session() as session:
                hooks = session.exec(
                    select(HeliusHook)
                    .where(HeliusHook.id.in_(processed_ids))
                    .where(HeliusHook.processed == False)
                ).all()
                
                for hook in hooks:
                    hook.processed = True
                    hook.processed_at = datetime.utcnow()
                
                session.commit()
                marked_count = len(hooks)
        else:
            marked_count = 0
        
        # Cleanup old staging records
        conn.execute(text(f"""
            DELETE FROM {BRONZE}.webhook_staging
            WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '7 days'
        """))
        conn.commit()
        
        return {
            "marked_count": marked_count,
            "processed_ids": processed_ids[:10]  # Sample for logging
        }