

WITH staged_transactions AS (
    SELECT * FROM "solana_pipeline"."public_silver"."stg_webhook_transactions"
),

-- Deduplicate by signature, keeping the earliest record
deduped_transactions AS (
    SELECT DISTINCT ON (signature)
        raw_id,
        signature,
        user_address,
        swap_from_token as swapfromtoken,  -- Match existing column names
        swap_from_amount as swapfromamount,
        swap_to_token as swaptotoken,
        swap_to_amount as swaptoamount,
        source,
        timestamp,
        staging_created_at
    FROM staged_transactions
    ORDER BY signature, staging_created_at
)

SELECT
    signature,
    raw_id,
    user_address,
    swapfromtoken,
    swapfromamount,
    swaptotoken,
    swaptoamount,
    source,
    timestamp,
    FALSE as processed,  -- Default values
    FALSE as notification_sent,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM deduped_transactions


  -- Only process new signatures
  WHERE signature NOT IN (
    SELECT signature FROM "solana_pipeline"."public_silver"."mart_txns_clean"
  )
