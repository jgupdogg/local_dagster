{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='signature',
    on_schema_change='fail',
    indexes=[
      {'columns': ['signature'], 'unique': True},
      {'columns': ['timestamp'], 'type': 'btree'},
      {'columns': ['user_address'], 'type': 'hash'},
      {'columns': ['processed', 'notification_sent'], 'type': 'btree'}
    ]
  )
}}

WITH staged_transactions AS (
    SELECT * FROM {{ ref('stg_webhook_transactions') }}
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

{% if is_incremental() %}
  -- Only process new signatures
  WHERE signature NOT IN (
    SELECT signature FROM {{ this }}
  )
{% endif %}