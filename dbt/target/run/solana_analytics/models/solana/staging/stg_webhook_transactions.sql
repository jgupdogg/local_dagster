
  create view "solana_pipeline"."public_silver"."stg_webhook_transactions__dbt_tmp"
    
    
  as (
    

WITH parsed_transactions AS (
    SELECT 
        ws.raw_id,
        ws.created_at as staging_created_at,
        -- Extract first transaction from payload array
        (payload->0)::jsonb as tx,
        -- Extract token transfers array
        (payload->0->'tokenTransfers')::jsonb as token_transfers
    FROM bronze.webhook_staging ws
    WHERE 
        -- Ensure payload is an array with at least one element
        jsonb_typeof(payload) = 'array' 
        AND jsonb_array_length(payload) > 0
        -- Ensure tokenTransfers exists and is an array
        AND (payload->0->'tokenTransfers') IS NOT NULL
        AND jsonb_typeof(payload->0->'tokenTransfers') = 'array'
        AND jsonb_array_length(payload->0->'tokenTransfers') > 0
),

transaction_details AS (
    SELECT
        raw_id,
        staging_created_at,
        -- Transaction fields
        tx->>'signature' as signature,
        tx->>'source' as source,
        (tx->>'timestamp')::bigint as timestamp_unix,
        TO_TIMESTAMP((tx->>'timestamp')::bigint) as timestamp,
        
        -- First token transfer (from)
        token_transfers->0->>'fromUserAccount' as user_address,
        token_transfers->0->>'mint' as swap_from_token,
        (token_transfers->0->>'tokenAmount')::numeric as swap_from_amount,
        
        -- Last token transfer (to)
        token_transfers->-1->>'mint' as swap_to_token,
        (token_transfers->-1->>'tokenAmount')::numeric as swap_to_amount,
        
        -- Array length for validation
        jsonb_array_length(token_transfers) as transfer_count
    FROM parsed_transactions
)

SELECT
    raw_id,
    signature,
    user_address,
    swap_from_token,
    swap_from_amount,
    swap_to_token,
    swap_to_amount,
    source,
    timestamp,
    staging_created_at,
    transfer_count
FROM transaction_details
WHERE 
    -- Basic validation
    signature IS NOT NULL
    AND user_address IS NOT NULL
    AND swap_from_token IS NOT NULL
    AND swap_to_token IS NOT NULL
    -- Filter out PUMP_FUN
    AND source != 'PUMP_FUN'
  );