{{
  config(
    materialized='view'
  )
}}

WITH recent_txns AS (
    SELECT *
    FROM {{ source('silver', 'helius_txns_clean') }}
    ORDER BY timestamp DESC
    LIMIT {{ var('transaction_limit', 1000) }}
),

token_actions AS (
    -- Buy actions
    SELECT
        swaptotoken AS token_address,
        user_address,
        'buy' AS action_type,
        timestamp
    FROM recent_txns
    WHERE swaptotoken IS NOT NULL
    
    UNION ALL
    
    -- Sell actions
    SELECT
        swapfromtoken AS token_address,
        user_address,
        'sell' AS action_type,
        timestamp
    FROM recent_txns
    WHERE swapfromtoken IS NOT NULL
)

SELECT * FROM token_actions