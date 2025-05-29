"""Transaction processing assets with SQLModel."""
import json
import logging
import traceback
from typing import List, Dict, Any
from datetime import datetime
from sqlmodel import Session, select

from dagster import (
    asset, AssetIn, 
    OpExecutionContext
)

from solana_pipeline.models.solana_models import HeliusHook, HeliusTxnClean

logger = logging.getLogger(__name__)

@asset(
    key="unprocessed_webhook_data",
    compute_kind="sql",
    io_manager_key="io_manager",
    description="Retrieves unprocessed webhook data from the bronze layer",
    metadata={
        "schema": "bronze",
        "table": "helius_hook",
    },
    required_resource_keys={"db"}
)
def unprocessed_webhook_data(context: OpExecutionContext) -> List[Dict[str, Any]]:
    """
    Retrieve unprocessed webhook data from the helius_hook table using SQLModel.
    """
    limit = context.op_config.get("limit", 20) if context.op_config else 20
    
    db_manager = context.resources.db
    
    try:
        with db_manager.get_session() as session:
            # Query using SQLModel
            statement = select(HeliusHook).where(
                HeliusHook.processed == False
            ).order_by(
                HeliusHook.received_at.desc()
            ).limit(limit)
            
            hooks = session.exec(statement).all()
            
            # Convert to list of dictionaries for compatibility
            raw_rows = [
                {
                    "id": hook.id,
                    "payload": hook.payload,
                    "processed": hook.processed
                }
                for hook in hooks
            ]
            
            context.log.info(f"Retrieved {len(raw_rows)} unprocessed webhook records")
            return raw_rows
            
    except Exception as e:
        context.log.error(f"Error querying webhook data: {e}")
        context.log.error(traceback.format_exc())
        return []


@asset(
    key="processed_transactions",
    ins={"webhook_data": AssetIn("unprocessed_webhook_data")},
    compute_kind="python",
    io_manager_key="io_manager",
    description="Processes raw webhook data into clean transaction records",
    metadata={
        "schema": "silver",
        "table": "helius_txns_clean",
    },
    required_resource_keys={"db"}
)
def processed_transactions(context: OpExecutionContext, webhook_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Process webhook data into clean transaction records using SQLModel.
    """
    if not webhook_data:
        context.log.info("No webhook data to process")
        return {"processed_count": 0, "raw_ids": []}
    
    filter_source = context.op_config.get("filter_source", "PUMP_FUN") if context.op_config else "PUMP_FUN"
    context.log.info(f"Processing webhook data, filtering out source: {filter_source}")
    
    db_manager = context.resources.db
    processed_records = []
    raw_ids = []
    
    try:
        with db_manager.get_session() as session:
            for row in webhook_data:
                raw_id = row.get("id")
                raw_ids.append(raw_id)
                
                # Parse payload
                payload_val = row.get("payload")
                if isinstance(payload_val, str):
                    try:
                        payload_val = json.loads(payload_val)
                    except Exception as e:
                        context.log.error(f"Error parsing JSON for id {raw_id}: {e}")
                        continue
                
                # Skip if payload is not a list or is empty
                if not isinstance(payload_val, list) or len(payload_val) == 0:
                    context.log.warning(f"Row id {raw_id}: payload is empty or not a list")
                    continue
                
                # Get first transaction
                tx = payload_val[0]
                token_transfers = tx.get("tokenTransfers", [])
                
                # Skip if no token transfers
                if not token_transfers:
                    context.log.warning(f"Row id {raw_id}: no tokenTransfers found")
                    continue
                
                # Get first and last token transfer for swap details
                first_tt = token_transfers[0]
                last_tt = token_transfers[-1]
                
                # Convert timestamp
                ts = tx.get("timestamp")
                ts_dt = datetime.fromtimestamp(ts) if ts else datetime.utcnow()
                
                # Skip if source matches filter
                if tx.get("source") == filter_source:
                    context.log.info(f"Skipping record with source {filter_source}")
                    continue
                
                signature = tx.get("signature")
                
                # Check if record already exists using SQLModel
                existing = session.exec(
                    select(HeliusTxnClean).where(HeliusTxnClean.signature == signature)
                ).first()
                
                if existing:
                    context.log.info(f"Record with signature {signature} already exists, skipping")
                    continue
                
                # Create new record using SQLModel with lowercase field names
                new_txn = HeliusTxnClean(
                    signature=signature,
                    raw_id=raw_id,
                    user_address=first_tt.get("fromUserAccount"),
                    swapfromtoken=first_tt.get("mint"),  # lowercase!
                    swapfromamount=float(first_tt.get("tokenAmount", 0)),  # lowercase!
                    swaptotoken=last_tt.get("mint"),  # lowercase!
                    swaptoamount=float(last_tt.get("tokenAmount", 0)),  # lowercase!
                    source=tx.get("source"),
                    timestamp=ts_dt,
                    processed=False,
                    notification_sent=False
                )
                
                # Add to session
                session.add(new_txn)
                context.log.info(f"Added record with signature {signature}")
                context.log.debug(f"Token swap: {first_tt.get('mint')} -> {last_tt.get('mint')}")
                processed_records.append(signature)
            
            # Commit all new records
            session.commit()
            context.log.info(f"Committed {len(processed_records)} new records")
            
            # Update webhook records as processed
            if processed_records:
                for raw_id in raw_ids:
                    hook = session.exec(
                        select(HeliusHook).where(HeliusHook.id == raw_id)
                    ).first()
                    
                    if hook:
                        hook.processed = True
                        hook.processed_at = datetime.utcnow()
                
                session.commit()
                context.log.info(f"Marked {len(raw_ids)} webhook records as processed")
    
    except Exception as e:
        context.log.error(f"Error during transaction processing: {e}")
        context.log.error(traceback.format_exc())
    
    return {
        "processed_count": len(processed_records), 
        "raw_ids": raw_ids
    }