

WITH token_stats AS (
    SELECT
        token_address,
        COUNT(DISTINCT CASE WHEN action_type = 'buy' THEN user_address END) AS unique_buyers,
        COUNT(DISTINCT CASE WHEN action_type = 'sell' THEN user_address END) AS unique_sellers,
        COUNT(DISTINCT CASE WHEN action_type = 'buy' THEN user_address END) - 
        COUNT(DISTINCT CASE WHEN action_type = 'sell' THEN user_address END) AS net_buyers,
        MAX(timestamp) AS latest_activity
    FROM "solana_pipeline"."public_silver"."stg_recent_transactions"
    GROUP BY token_address
    HAVING COUNT(DISTINCT user_address) > 0
),

scored_tokens AS (
    SELECT
        token_address,
        unique_buyers,
        unique_sellers,
        net_buyers,
        latest_activity,
        CASE
            WHEN unique_buyers >= unique_sellers AND unique_buyers >= ABS(net_buyers) THEN 'most_unique_buyers'
            WHEN unique_sellers >= unique_buyers AND unique_sellers >= ABS(net_buyers) THEN 'most_unique_sellers'
            ELSE 'most_net_buyers'
        END AS signal_type,
        CASE
            WHEN unique_buyers >= unique_sellers AND unique_buyers >= ABS(net_buyers) THEN unique_buyers
            WHEN unique_sellers >= unique_buyers AND unique_sellers >= ABS(net_buyers) THEN unique_sellers
            ELSE ABS(net_buyers)
        END AS score
    FROM token_stats
),

-- Fix the x_followers subquery to ensure uniqueness
latest_followers AS (
    SELECT DISTINCT ON (token_address)
        token_address,
        COUNT(DISTINCT follower_handle) AS follower_count
    FROM "solana_pipeline"."bronze"."x_followers"
    GROUP BY token_address, scraped_at
    ORDER BY token_address, scraped_at DESC
),

enriched_tokens AS (
    SELECT
        st.*,
        COALESCE(tm.symbol, 'Unknown') AS symbol,
        COALESCE(tm.name, 'Unknown Token') AS name,
        tm.website,
        tm.twitter,
        tm.logo_uri,
        tci.block_human_time AS creation_time,
        CASE 
            WHEN tci.block_human_time IS NOT NULL 
            THEN EXTRACT(DAY FROM (CURRENT_TIMESTAMP - TO_TIMESTAMP(tci.block_human_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')))
            ELSE NULL
        END AS token_age_days,
        COALESCE(ss.snifscore, 0) AS snifscore,
        COALESCE(lf.follower_count, 0) AS twitter_followers
    FROM scored_tokens st
    LEFT JOIN "solana_pipeline"."bronze"."token_metadata" tm 
        ON st.token_address = tm.token_address
    LEFT JOIN "solana_pipeline"."bronze"."token_creation_info" tci 
        ON st.token_address = tci.token_address
    LEFT JOIN "solana_pipeline"."bronze"."solsniffer_token_data" ss 
        ON st.token_address = ss.token_address
    LEFT JOIN latest_followers lf 
        ON st.token_address = lf.token_address
)

SELECT 
    *,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM enriched_tokens
ORDER BY score DESC
LIMIT 15