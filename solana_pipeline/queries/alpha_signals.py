# File: solana_pipeline/queries/alpha_signals.py

"""SQL queries for alpha signal detection."""

ACTIVE_TOKENS_QUERY = """
WITH recent_txns AS (
    -- Get the most recent 1000 transactions
    SELECT *
    FROM silver.helius_txns_clean
    ORDER BY timestamp DESC
    LIMIT 1000
),

token_actions AS (
    -- Track buying actions
    SELECT
        swaptotoken AS token_address,
        user_address,
        'buy' AS action_type
    FROM recent_txns
    WHERE swaptotoken IS NOT NULL
    
    UNION ALL
    
    -- Track selling actions
    SELECT
        swapfromtoken AS token_address,
        user_address,
        'sell' AS action_type  
    FROM recent_txns
    WHERE swapfromtoken IS NOT NULL
),

token_stats AS (
    -- For each token, count unique buyers and sellers
    SELECT
        token_address,
        COUNT(DISTINCT CASE WHEN action_type = 'buy' THEN user_address END) AS unique_buyers,
        COUNT(DISTINCT CASE WHEN action_type = 'sell' THEN user_address END) AS unique_sellers,
        COUNT(DISTINCT CASE WHEN action_type = 'buy' THEN user_address END) - 
        COUNT(DISTINCT CASE WHEN action_type = 'sell' THEN user_address END) AS net_buyers
    FROM token_actions
    GROUP BY token_address
    HAVING COUNT(DISTINCT user_address) > 0
),

unique_token_data AS (
    -- Assign categories and select only one category per token (strongest signal)
    SELECT DISTINCT ON (token_address)
        token_address,
        unique_buyers,
        unique_sellers,
        net_buyers,
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
    ORDER BY token_address, score DESC
),

-- Get unique metadata entries 
token_metadata_unique AS (
    SELECT DISTINCT ON (token_address)
        token_address,
        symbol,
        token_name,
        website,
        twitter,
        logo_uri
    FROM bronze.token_metadata
    ORDER BY token_address, symbol NULLS LAST
),

-- Get unique creation info
token_creation_unique AS (
    SELECT DISTINCT ON (token_address)
        token_address,
        block_human_time
    FROM bronze.token_creation_info
    ORDER BY token_address, block_human_time NULLS LAST
),

-- Get unique solsniffer data (most recent per token)
solsniffer_unique AS (
    SELECT DISTINCT ON (token_address)
        token_address,
        snifscore
    FROM bronze.solsniffer_token_data
    ORDER BY token_address, scraped_at DESC NULLS LAST
),

-- Get the most recent scraped_at for each token
latest_scrape_dates AS (
    SELECT 
        token_address,
        MAX(scraped_at) AS latest_scraped_at
    FROM bronze.x_followers
    GROUP BY token_address
),

-- Count followers for each token at their latest scrape date
twitter_followers AS (
    SELECT
        lsd.token_address,
        COUNT(DISTINCT xf.follower_handle) AS follower_count
    FROM latest_scrape_dates lsd
    JOIN bronze.x_followers xf ON 
        lsd.token_address = xf.token_address AND
        lsd.latest_scraped_at = xf.scraped_at
    GROUP BY lsd.token_address
)

-- Final result with unique token addresses
SELECT
    utd.token_address,
    COALESCE(tmu.token_symbol, 'Unknown') AS symbol,
    COALESCE(tmu.token_name, 'Unknown Token') AS name,
    tmu.website,
    tmu.twitter,
    tmu.logo_uri,
    utd.unique_buyers,
    utd.unique_sellers,
    utd.net_buyers,
    utd.signal_type,
    utd.score,
    tcu.block_human_time AS creation_time,
    -- Calculate token age in days
    CASE 
        WHEN tcu.block_human_time IS NOT NULL 
        THEN EXTRACT(DAY FROM (CURRENT_TIMESTAMP - TO_TIMESTAMP(tcu.block_human_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')))
        ELSE NULL
    END AS token_age_days,
    -- Add snifscore
    COALESCE(su.snifscore, 0) AS snifscore,
    -- Add Twitter follower count
    COALESCE(tf.follower_count, 0) AS twitter_followers
FROM unique_token_data utd
LEFT JOIN token_metadata_unique tmu ON utd.token_address = tmu.token_address
LEFT JOIN token_creation_unique tcu ON utd.token_address = tcu.token_address
LEFT JOIN solsniffer_unique su ON utd.token_address = su.token_address
LEFT JOIN twitter_followers tf ON utd.token_address = tf.token_address
ORDER BY utd.score DESC
LIMIT 15
"""

# Helper function to execute the query
def get_active_tokens(conn):
    """Execute the active tokens query and return results as dictionaries."""
    from sqlalchemy import text
    
    result = conn.execute(text(ACTIVE_TOKENS_QUERY))
    return [dict(row._mapping) for row in result]