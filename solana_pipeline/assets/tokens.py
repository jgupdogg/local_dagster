# Standard library imports
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Third-party imports
from dagster import (
    asset, AssetExecutionContext, Config, Field, Definitions, define_asset_job, AssetSelection
)
from sqlalchemy import text
from sqlmodel import select, or_, and_, not_, exists, func

# Application imports
from solana_pipeline.models.solana_models import (
    TrendingToken, TrackedToken, TokenListV3
)

# Configuration classes
class TokenListConfig(Config):
    limit: int = 100
    min_liquidity: int = 200000
    max_liquidity: int = 1000000
    min_volume_1h_usd: int = 200000
    min_price_change_2h_percent: int = 10
    min_price_change_24h_percent: int = 30

class FilterConfig(Config):
    limit: int = 50
    min_liquidity: float = 10000
    min_volume: float = 50000
    min_volume_mcap_ratio: float = 0.05
    min_price_change: float = 30

class TrendingTokenConfig(Config):
    limit: int = 100

# Asset definitions
@asset(
    required_resource_keys={"birdeye_api", "db"},
    group_name="bronze"
)
def token_list_v3(context: AssetExecutionContext, config: TokenListConfig) -> None:
    """
    Extract tokens from BirdEye API v3 endpoint with specific filters and store in bronze layer.
    This asset represents data in the bronze.token_list_v3 table.
    """
    # Get resources
    birdeye_client = context.resources.birdeye_api
    db_manager = context.resources.db
    
    # Get parameters from config
    total_limit = config.limit
    min_liquidity = config.min_liquidity
    max_liquidity = config.max_liquidity
    min_volume_1h_usd = config.min_volume_1h_usd
    min_price_change_2h_percent = config.min_price_change_2h_percent
    min_price_change_24h_percent = config.min_price_change_24h_percent
    
    context.log.info(f"Fetching tokens from BirdEye API v3 with filters: "
                    f"liquidity between {min_liquidity} and {max_liquidity}, "
                    f"1h volume≥{min_volume_1h_usd}, "
                    f"2h price change≥{min_price_change_2h_percent}%, "
                    f"24h price change≥{min_price_change_24h_percent}%")
    
    try:
        # Verify database connection
        with db_manager.get_connection() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            context.log.info(f"Database connection verified")
        
        # Ensure tables exist
        db_manager.create_all_tables()
        
        # Initialize variables for pagination
        offset = 0
        limit_per_call = 100  # API maximum per call for v3
        all_tokens = []
        has_more = True
        sleep_between_calls = 0.5  # Time to sleep between API calls
        
        # Fetch tokens with pagination
        while has_more and len(all_tokens) < total_limit:
            remaining = total_limit - len(all_tokens)
            current_limit = min(limit_per_call, remaining)
            
            context.log.info(f"Calling BirdEye API v3 (offset={offset}, limit={current_limit})...")
            
            # Create a parameters dictionary with ONLY the 5 specific filtering parameters
            filter_params = {
                "min_liquidity": min_liquidity,
                "max_liquidity": max_liquidity,
                "min_volume_1h_usd": min_volume_1h_usd, 
                "min_price_change_2h_percent": min_price_change_2h_percent,
                "min_price_change_24h_percent": min_price_change_24h_percent
            }
            
            # Make API call with pagination and the 5 specific filtering parameters
            # Using _get directly to have full control over parameters
            endpoint = "/defi/v3/token/list"
            full_params = {
                "sort_by": "liquidity",  # Sort by liquidity
                "sort_type": "desc",     # Highest first
                "offset": offset,
                "limit": current_limit,
                **filter_params  # Include only our 5 specific parameters
            }
            
            response = birdeye_client._get(endpoint, params=full_params)
            
            context.log.info(f"API response received: {str(response)[:200]}...")
            
            # Extract tokens data from response
            if isinstance(response, dict) and "data" in response and "items" in response["data"]:
                tokens_data = response["data"]["items"]
            else:
                context.log.warning(f"Unexpected response structure: {type(response)}")
                tokens_data = []
                
            # If no tokens in this batch, stop pagination
            if not tokens_data:
                has_more = False
                break
                
            # Add tokens to accumulated list
            all_tokens.extend(tokens_data)
            
            # Update offset for next batch
            offset += len(tokens_data)
            
            # If we got fewer tokens than requested, assume no more are available
            if len(tokens_data) < current_limit:
                has_more = False
            
            # Sleep between calls to respect rate limits
            if has_more and len(all_tokens) < total_limit:
                time.sleep(sleep_between_calls)
        
        context.log.info(f"Found {len(all_tokens)} tokens across {(offset // limit_per_call) + 1} API calls")
        
        # Ensure we don't exceed the total limit
        all_tokens = all_tokens[:total_limit]
        
        # Create SQLModel instances for each token
        token_models = []
        for token_data in all_tokens:
            try:
                # Extract all available fields
                token_obj = TokenListV3(
                    token_address=token_data.get("address"),
                    symbol=token_data.get("symbol"),
                    name=token_data.get("name"),
                    decimals=token_data.get("decimals"),
                    logo_uri=token_data.get("logo_uri"),
                    
                    # Market data
                    market_cap=token_data.get("market_cap"),
                    fdv=token_data.get("fdv"),
                    liquidity=token_data.get("liquidity"),
                    last_trade_unix_time=token_data.get("last_trade_unix_time"),
                    holder=token_data.get("holder"),
                    recent_listing_time=token_data.get("recent_listing_time"),
                    
                    # Price data
                    price=token_data.get("price"),
                    price_change_1h_percent=token_data.get("price_change_1h_percent"),
                    price_change_2h_percent=token_data.get("price_change_2h_percent"),
                    price_change_4h_percent=token_data.get("price_change_4h_percent"),
                    price_change_8h_percent=token_data.get("price_change_8h_percent"),
                    price_change_24h_percent=token_data.get("price_change_24h_percent"),
                    
                    # Volume data
                    volume_1h_usd=token_data.get("volume_1h_usd"),
                    volume_2h_usd=token_data.get("volume_2h_usd"),
                    volume_4h_usd=token_data.get("volume_4h_usd"),
                    volume_8h_usd=token_data.get("volume_8h_usd"),
                    volume_24h_usd=token_data.get("volume_24h_usd"),
                    
                    # Volume change percentages
                    volume_1h_change_percent=token_data.get("volume_1h_change_percent"),
                    volume_2h_change_percent=token_data.get("volume_2h_change_percent"),
                    volume_4h_change_percent=token_data.get("volume_4h_change_percent"),
                    volume_8h_change_percent=token_data.get("volume_8h_change_percent"),
                    volume_24h_change_percent=token_data.get("volume_24h_change_percent"),
                    
                    # Trade counts
                    trade_1h_count=token_data.get("trade_1h_count"),
                    trade_2h_count=token_data.get("trade_2h_count"),
                    trade_4h_count=token_data.get("trade_4h_count"),
                    trade_8h_count=token_data.get("trade_8h_count"),
                    trade_24h_count=token_data.get("trade_24h_count"),
                    
                    # Extensions data
                    extensions=token_data.get("extensions"),
                    
                    # Standard fields
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                    
                    # State tracking
                    tracked=None,
                    processed_at=None,
                    processing_status="pending"
                )
                
                # Only add valid tokens
                if token_obj.token_address:
                    token_models.append(token_obj)
                else:
                    context.log.warning(f"Skipping token with no address")
                    
            except Exception as e:
                context.log.error(f"Error processing token data: {e}")
                continue
        
        # Store in database
        successful_inserts = 0
        if token_models:
            context.log.info(f"Storing {len(token_models)} tokens in database")
            
            for token in token_models:
                try:
                    success = db_manager.insert_model(token)
                    if success:
                        successful_inserts += 1
                except Exception as e:
                    context.log.error(f"Error inserting token {token.token_address}: {e}")
            
            context.log.info(f"Successfully stored {successful_inserts}/{len(token_models)} tokens")
        else:
            context.log.warning("No tokens to store")
            
        # Return materialization metadata
        return_metadata = {
            "tokens_found": len(all_tokens),
            "tokens_stored": successful_inserts
        }
        context.add_output_metadata(metadata=return_metadata)
    
    except Exception as e:
        context.log.error(f"Extract tokens failed: {e}")
        raise


@asset(
    required_resource_keys={"birdeye_api", "db"},
    group_name="bronze"
)
def trending_tokens(context: AssetExecutionContext, config: TrendingTokenConfig) -> None:
    """
    Extract trending tokens from BirdEye API and store in bronze layer with state tracking.
    This asset represents data in the bronze.trending_tokens table.
    """
    # Get resources
    birdeye_client = context.resources.birdeye_api
    db_manager = context.resources.db
    
    # Get total limit from config
    total_limit = config.limit
    
    context.log.info(f"Fetching trending tokens from BirdEye API with total limit {total_limit}...")
    
    try:
        # Verify database connection
        with db_manager.get_connection() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            context.log.info(f"Database connection verified")
        
        # Ensure tables exist
        db_manager.create_all_tables()
        
        # Add debug for API client
        context.log.info(f"BirdEye API client: {birdeye_client}")
        
        # Initialize variables for pagination
        offset = 0
        limit_per_call = 20  # API maximum per call
        all_tokens = []
        has_more = True
        sleep_between_calls = 0.5  # Time to sleep between API calls
        
        # Fetch tokens with pagination
        while has_more and len(all_tokens) < total_limit:
            remaining = total_limit - len(all_tokens)
            current_limit = min(limit_per_call, remaining)
            
            context.log.info(f"Calling BirdEye API (offset={offset}, limit={current_limit})...")
            
            # Create a parameters dictionary with ONLY the 5 specific filtering parameters
            # This ensures no default parameters are accidentally included
            params = {
                "min_liquidity": 200000,           # Minimum liquidity threshold
                "max_liquidity": 1000000,          # Maximum liquidity threshold
                "min_volume_1h_usd": 200000,       # Minimum 1-hour volume in USD
                "min_price_change_2h_percent": 10, # Minimum 2-hour price change percentage
                "min_price_change_24h_percent": 30 # Minimum 24-hour price change percentage
            }
            
            # Make API call with pagination and the 5 specific filtering parameters
            # Using _get directly to have full control over parameters
            endpoint = "/defi/v3/token/list"
            full_params = {
                "sort_by": "volume_1h_change_percent",  # Sort by liquidity
                "sort_type": "desc",     # Highest first
                "offset": offset,
                "limit": current_limit,
                **params  # Include only our 5 specific parameters
            }
            
            trending_response = birdeye_client._get(endpoint, params=full_params)
            
            context.log.info(f"API response received: {trending_response[:200] if isinstance(trending_response, str) else str(trending_response)[:200]}...")
            
            # Extract tokens data from response
            if isinstance(trending_response, dict):
                if "data" in trending_response and "tokens" in trending_response["data"]:
                    tokens_data = trending_response["data"]["tokens"]
                elif "data" in trending_response:
                    tokens_data = trending_response["data"]
                elif "tokens" in trending_response:
                    tokens_data = trending_response["tokens"]
                else:
                    context.log.warning(f"Unexpected response structure")
                    tokens_data = []
            else:
                tokens_data = trending_response
                
            # Ensure tokens_data is a list
            if not isinstance(tokens_data, list):
                context.log.warning(f"Tokens data is not a list: {type(tokens_data)}")
                tokens_data = []
                
            # If no tokens in this batch, stop pagination
            if not tokens_data:
                has_more = False
                break
                
            # Add tokens to accumulated list
            all_tokens.extend(tokens_data)
            
            # Update offset for next batch
            offset += len(tokens_data)
            
            # If we got fewer tokens than requested, assume no more are available
            if len(tokens_data) < current_limit:
                has_more = False
            
            # Sleep between calls to respect rate limits
            if has_more and len(all_tokens) < total_limit:
                time.sleep(sleep_between_calls)
        
        context.log.info(f"Found {len(all_tokens)} trending tokens across {offset // limit_per_call + 1} API calls")
        
        # Ensure we don't exceed the total limit
        all_tokens = all_tokens[:total_limit]
        
        # Create SQLModel instances for each token
        trending_tokens = []
        for token_data in all_tokens:
            try:
                # Extract all available fields dynamically
                token_obj = TrendingToken(
                    token_address=token_data.get("address"),
                    symbol=token_data.get("symbol"),
                    name=token_data.get("name"),
                    decimals=token_data.get("decimals"),
                    liquidity=token_data.get("liquidity"),
                    logoURI=token_data.get("logoURI"),
                    volume24hUSD=token_data.get("volume24hUSD"),
                    volume24hChangePercent=token_data.get("volume24hChangePercent"),
                    rank=token_data.get("rank"),
                    price=token_data.get("price"),
                    price24hChangePercent=token_data.get("price24hChangePercent"),
                    fdv=token_data.get("fdv"),
                    marketcap=token_data.get("marketcap"),
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                    # State tracking fields are NULL by default
                    tracked=None,
                    processed_at=None,
                    processing_status="pending"  # Initial status is pending
                )
                
                # Only add valid tokens
                if token_obj.token_address:
                    trending_tokens.append(token_obj)
                else:
                    context.log.warning(f"Skipping token with no address")
                    
            except Exception as e:
                context.log.error(f"Error processing token data: {e}")
                continue
        
        # Store in database
        successful_inserts = 0
        if trending_tokens:
            context.log.info(f"Storing {len(trending_tokens)} tokens in database")
            
            for token in trending_tokens:
                try:
                    success = db_manager.insert_model(token)
                    if success:
                        successful_inserts += 1
                except Exception as e:
                    context.log.error(f"Error inserting token {token.token_address}: {e}")
            
            context.log.info(f"Successfully stored {successful_inserts}/{len(trending_tokens)} trending tokens")
        else:
            context.log.warning("No trending tokens to store")
            
        # Return materialization metadata
        return_metadata = {
            "tokens_found": len(all_tokens),
            "tokens_stored": successful_inserts
        }
        context.add_output_metadata(metadata=return_metadata)
    
    except Exception as e:
        context.log.error(f"Extract trending tokens failed: {e}")
        raise


@asset(
    required_resource_keys={"db"},
    deps=["token_list_v3"],  # Explicit dependency on token_list_v3 asset
    group_name="silver"  # This is in the silver tier
)
def tracked_tokens(context: AssetExecutionContext, config: FilterConfig) -> None:
    """
    Filter tokens from bronze.token_list_v3 based on criteria and create tracked tokens 
    in the silver.tracked_tokens table. This asset represents the filtered token data.
    """
    db_manager = context.resources.db
    
    # Get parameters from config
    limit = config.limit
    min_liquidity = config.min_liquidity
    min_volume = config.min_volume
    min_volume_mcap_ratio = config.min_volume_mcap_ratio
    min_price_change = config.min_price_change
    
    context.log.info(f"Filtering up to {limit} unprocessed tokens from token_list_v3")
    now = datetime.utcnow()
    yesterday = now - timedelta(days=1)
    
    time.sleep(1)
    
    # Force connection pool to return a fresh connection
    db_manager.engine.dispose()
    
    try:
        with db_manager.get_session() as session:
            with session.begin():
                # Find tokens that meet our criteria and haven't been processed
                unprocessed_query = (
                    select(TokenListV3)
                    .where(
                        and_(
                            # Processing status check
                            or_(
                                TokenListV3.processed_at.is_(None),
                                TokenListV3.processing_status == 'pending'
                            ),
                            # Must have logo and extensions
                            TokenListV3.logo_uri.is_not(None),
                            TokenListV3.extensions.is_not(None),
                            # All price changes must be positive
                            TokenListV3.price_change_1h_percent > 0,
                            TokenListV3.price_change_2h_percent > 0,
                            TokenListV3.price_change_4h_percent > 0,
                            TokenListV3.price_change_8h_percent > 0,
                            TokenListV3.price_change_24h_percent > 0,
                            # Not recently added to tracked_tokens
                            not_(
                                exists().where(
                                    and_(
                                        TrackedToken.token_address == TokenListV3.token_address,
                                        TrackedToken.created_at > yesterday
                                    )
                                )
                            )
                        )
                    )
                    .order_by(TokenListV3.created_at.desc())
                    .limit(limit)
                )
                
                unprocessed_tokens = session.exec(unprocessed_query).all()
                context.log.info(f"Found {len(unprocessed_tokens)} unprocessed tokens that meet initial criteria")
                
                if not unprocessed_tokens:
                    # Try a simpler query to debug
                    all_tokens = session.exec(select(TokenListV3).limit(5)).all()
                    context.log.info(f"Debug - All tokens sample: {all_tokens}")
                    
                    # Add metadata for the asset
                    context.add_output_metadata(metadata={"tokens_processed": 0})
                    return
                
                # Apply additional filter criteria
                filtered_tokens = []
                for token in unprocessed_tokens:
                    # Apply filter criteria (note field name differences between models)
                    if (token.logo_uri and token.logo_uri.strip() and
                        token.liquidity is not None and token.liquidity >= min_liquidity and
                        token.volume_24h_usd is not None and token.volume_24h_usd >= min_volume and
                        token.price_change_24h_percent is not None and token.price_change_24h_percent >= min_price_change and
                        (token.market_cap is None or token.market_cap <= 0 or 
                         (token.volume_24h_usd / token.market_cap) >= min_volume_mcap_ratio)):
                        
                        # Calculate additional fields for silver tier
                        volume_mcap_ratio = None
                        if token.market_cap is not None and token.market_cap > 0:
                            volume_mcap_ratio = token.volume_24h_usd / token.market_cap
                        
                        # Create TrackedToken instance
                        tracked_token = TrackedToken(
                            # Map fields from TokenListV3 to TrackedToken with proper field mapping
                            token_address=token.token_address,
                            symbol=token.symbol,
                            name=token.name,
                            decimals=token.decimals,
                            liquidity=token.liquidity,
                            logoURI=token.logo_uri,  # Field name mapping
                            volume24hUSD=token.volume_24h_usd,  # Field name mapping
                            volume24hChangePercent=token.volume_24h_change_percent,  # Field name mapping
                            rank=None,  # No rank in TokenListV3
                            price=token.price,
                            price24hChangePercent=token.price_change_24h_percent,  # Field name mapping
                            fdv=token.fdv,
                            marketcap=token.market_cap,  # Field name mapping
                            volume_mcap_ratio=volume_mcap_ratio,
                            quality_score=None,
                            bronze_id=token.id,
                            created_at=now,
                            updated_at=now
                            # All tracking timestamps default to NULL
                        )
                        filtered_tokens.append((token, tracked_token))
                        context.log.info(f"Token {token.symbol} ({token.token_address}) passed filters")
                    else:
                        context.log.info(f"Token {token.symbol} ({token.token_address}) filtered out")
                
                # Update all tokens' status at once and insert TrackedTokens
                filtered_token_ids = [pair[0].id for pair in filtered_tokens]
                
                # Mark all tokens as processed
                for token in unprocessed_tokens:
                    token.processed_at = now
                    token.updated_at = now
                    token.processing_status = 'completed'
                    token.tracked = token.id in filtered_token_ids
                
                # Insert all tracked tokens at once
                if filtered_tokens:
                    tracked_tokens = [pair[1] for pair in filtered_tokens]
                    session.add_all(tracked_tokens)
                    context.log.info(f"Inserting {len(tracked_tokens)} tokens into silver tier")
                else:
                    context.log.info("No tokens passed filters")
                
                # Add metadata for the asset
                context.add_output_metadata(metadata={
                    "tokens_processed": len(unprocessed_tokens),
                    "tokens_tracked": len(filtered_tokens)
                })
    
    except Exception as e:
        context.log.error(f"Filter trending tokens operation failed: {e}")
        raise



