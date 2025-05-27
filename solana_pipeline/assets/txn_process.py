"""Transaction processing assets with detailed error debugging."""
import json
import logging
import traceback
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime
from sqlalchemy import text, inspect, exc

from dagster import (
    asset, AssetIn, 
    Field, OpExecutionContext, AssetExecutionContext, Config
)

from solana_pipeline.models.solana_models import HeliusHook, HeliusTxnClean, AlphaSignal

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
    Retrieve unprocessed webhook data from the helius_hook table.
    """
    # Get limit from config or use default - safely handle None op_config
    limit = 20  # Default limit
    if hasattr(context, 'op_config') and context.op_config is not None:
        limit = context.op_config.get("limit", 20)
    
    # Use the DBManager
    db_manager = context.resources.db
    
    try:
        # Using direct connection for raw SQL query
        with db_manager.get_connection() as conn:
            query = text(f"""
            SELECT id, payload, processed 
            FROM bronze.helius_hook 
            WHERE processed = false 
            ORDER BY received_at DESC 
            LIMIT {limit}
            """)
            
            result = conn.execute(query)
            
            # Convert to list of dictionaries
            raw_rows = [dict(zip(result.keys(), row)) for row in result.fetchall()]
            
            context.log.info(f"Retrieved {len(raw_rows)} unprocessed webhook records")
            
            # Return raw value
            return raw_rows
    except Exception as e:
        context.log.error(f"Error querying webhook data: {e}")
        context.log.error(traceback.format_exc())
        return []  # Return empty list on error


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
    Process webhook data into clean transaction records and store in silver schema.
    """
    if not webhook_data:
        context.log.info("No webhook data to process")
        # Return raw dict
        return {"processed_count": 0, "raw_ids": []}
    
    # Filter source to exclude - safely handle None op_config
    filter_source = "PUMP_FUN"  # Default filter
    if hasattr(context, 'op_config') and context.op_config is not None:
        filter_source = context.op_config.get("filter_source", "PUMP_FUN")
        
    context.log.info(f"Processing webhook data, filtering out source: {filter_source}")
    
    # Connect to the database
    db_manager = context.resources.db
    
    # First, let's check the actual structure of the table to confirm the schema
    try:
        with db_manager.get_connection() as conn:
            context.log.info("Inspecting silver.helius_txns_clean table structure...")
            inspector = inspect(conn)
            if 'silver' in inspector.get_schema_names() and 'helius_txns_clean' in inspector.get_table_names(schema='silver'):
                columns = inspector.get_columns('helius_txns_clean', schema='silver')
                column_names = [col['name'] for col in columns]
                primary_keys = inspector.get_pk_constraint('helius_txns_clean', schema='silver')
                unique_constraints = inspector.get_unique_constraints('helius_txns_clean', schema='silver')
                
                context.log.info(f"Table columns: {column_names}")
                context.log.info(f"Primary keys: {primary_keys}")
                context.log.info(f"Unique constraints: {unique_constraints}")
            else:
                context.log.warning("Table silver.helius_txns_clean does not exist!")
                # Create it
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS silver.helius_txns_clean (
                    signature VARCHAR(255) PRIMARY KEY,
                    raw_id INTEGER,
                    user_address VARCHAR(255),
                    swapfromtoken VARCHAR(255),
                    swapfromamount NUMERIC,
                    swaptotoken VARCHAR(255),
                    swaptoamount NUMERIC,
                    source VARCHAR(255),
                    timestamp TIMESTAMP,
                    processed BOOLEAN DEFAULT FALSE,
                    notification_sent BOOLEAN DEFAULT FALSE
                )
                """
                conn.execute(text(create_table_sql))
                context.log.info("Created table silver.helius_txns_clean")
    except Exception as e:
        context.log.error(f"Error inspecting table: {e}")
        context.log.error(traceback.format_exc())
    
    # Process each webhook record
    processed_records = []
    raw_ids = []
    
    try:
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
            
            # Check if record already exists
            signature = tx.get("signature")
            try:
                with db_manager.get_connection() as conn:
                    check_query = text("SELECT 1 FROM silver.helius_txns_clean WHERE signature = :signature")
                    result = conn.execute(check_query, {"signature": signature})
                    if result.fetchone():
                        context.log.info(f"Record with signature {signature} already exists, skipping")
                        continue
            except Exception as e:
                context.log.error(f"Error checking existing record: {e}")
            
            # Use direct SQL insertion instead of SQLModel
            try:
                with db_manager.get_connection() as conn:
                    # Detailed logging of values
                    context.log.info(f"Inserting record - signature: {signature}")
                    context.log.info(f"Values: raw_id={raw_id}, user_address={first_tt.get('fromUserAccount')}, " +
                                  f"from_token={first_tt.get('mint')}, to_token={last_tt.get('mint')}, " +
                                  f"source={tx.get('source')}, timestamp={ts_dt}")
                    
                    insert_sql = text("""
                    INSERT INTO silver.helius_txns_clean 
                    (signature, raw_id, user_address, swapfromtoken, swapfromamount, 
                    swaptotoken, swaptoamount, source, timestamp, processed, notification_sent)
                    VALUES 
                    (:signature, :raw_id, :user_address, :swapfromtoken, :swapfromamount,
                    :swaptotoken, :swaptoamount, :source, :timestamp, :processed, :notification_sent)
                    ON CONFLICT (signature) DO NOTHING
                    """)
                    
                    # Prepare parameters with explicit types
                    params = {
                        "signature": signature,
                        "raw_id": raw_id,
                        "user_address": first_tt.get("fromUserAccount"),
                        "swapfromtoken": first_tt.get("mint"),
                        "swapfromamount": float(first_tt.get("tokenAmount", 0)),
                        "swaptotoken": last_tt.get("mint"),
                        "swaptoamount": float(last_tt.get("tokenAmount", 0)),
                        "source": tx.get("source"),
                        "timestamp": ts_dt,
                        "processed": False,
                        "notification_sent": False
                    }
                    
                    try:
                        result = conn.execute(insert_sql, params)
                        if result.rowcount > 0:
                            context.log.info(f"Successfully inserted record with signature {signature}")
                            processed_records.append(params)
                        else:
                            context.log.info(f"No rows affected when inserting {signature}, possible duplicate")
                    except exc.SQLAlchemyError as e:
                        context.log.error(f"SQLAlchemy error inserting record: {e}")
                        # Try to diagnose the specific issue
                        if 'duplicate key' in str(e).lower():
                            context.log.error(f"Duplicate key error for signature: {signature}")
                        elif 'violates not-null constraint' in str(e).lower():
                            null_fields = []
                            for key, value in params.items():
                                if value is None:
                                    null_fields.append(key)
                            context.log.error(f"Not-null constraint violation. Null fields: {null_fields}")
                        else:
                            context.log.error(f"Other SQLAlchemy error: {e}")
                    except Exception as e:
                        context.log.error(f"Unexpected error inserting record: {e}")
                        context.log.error(traceback.format_exc())
            except Exception as e:
                context.log.error(f"Error in connection handling: {e}")
                context.log.error(traceback.format_exc())
        
        # Mark webhook records as processed if we processed any records
        if processed_records:
            try:
                with db_manager.get_connection() as conn:
                    for raw_id in raw_ids:
                        update_query = text(f"""
                        UPDATE bronze.helius_hook
                        SET processed = true,
                            processed_at = NOW()
                        WHERE id = :raw_id
                        """)
                        conn.execute(update_query, {"raw_id": raw_id})
                    context.log.info(f"Marked {len(raw_ids)} webhook records as processed")
            except Exception as e:
                context.log.error(f"Error updating webhook records as processed: {e}")
                context.log.error(traceback.format_exc())
    
    except Exception as e:
        context.log.error(f"Unexpected error during transaction processing: {e}")
        context.log.error(traceback.format_exc())
    
    # Return raw dict
    return {
        "processed_count": len(processed_records), 
        "raw_ids": raw_ids
    }




