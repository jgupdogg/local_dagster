"""Whale processing assets for Solana Pipeline."""
from dagster import asset, AssetExecutionContext, Config, Definitions, define_asset_job
from typing import Dict, Any, List
from datetime import datetime, timedelta
import logging
from sqlalchemy import text
import time

# Configuration classes for assets
class WhaleParamsConfig(Config):
    max_whales_per_token: int = 500
    limit: int = 50
    sleep_seconds: int = 2
    refresh_days: int = 7

class TransactionParamsConfig(Config):
    limit: int = 10000
    max_txns_per_whale: int = 100
    sleep_seconds: int = 2

class PnLParamsConfig(Config):
    limit: int = 10000
    min_transactions: int = 2
    sleep_seconds: int = 1
    retry_attempts: int = 3

class TraderParamsConfig(Config):
    min_win_rate: float = 50.0
    min_realized_pnl: float = 0.01
    min_trades: int = 50
    max_trades_per_day: float = 100.0
    min_trades_per_day: float = 1.0
    recent_days: int = 7
    ranked_limit: int = 100

# Asset definitions
@asset(
    required_resource_keys={"db", "birdeye_api"},
    deps=["tracked_tokens"],  # Explicit dependency on tracked_tokens asset
    group_name="bronze"
)
def token_whales(context: AssetExecutionContext, config: WhaleParamsConfig) -> None:
    """Extract whales (large holders) for silver tier tokens."""
    from sqlmodel import select, or_, and_
    from solana_pipeline.models.solana_models import TrackedToken, TokenWhale
    from solana_pipeline.classes.token import Token
    
    # Get parameters from config
    max_whales_per_token = config.max_whales_per_token
    limit = config.limit
    sleep_seconds = config.sleep_seconds
    refresh_days = config.refresh_days
    
    # Resources
    db_manager = context.resources.db
    birdeye_api = context.resources.birdeye_api
    
    # Time thresholds
    now = datetime.utcnow()
    refresh_threshold = now - timedelta(days=refresh_days)
    
    # Metrics
    tokens_processed = 0
    whales_stored = 0
    
    try:
        # Get tokens needing whale data
        token_data_list = []
        with db_manager.get_session() as session:
            statement = (
                select(
                    TrackedToken.id, 
                    TrackedToken.token_address,
                    TrackedToken.symbol
                )
                .where(
                    or_(
                        TrackedToken.latest_whales.is_(None),
                        TrackedToken.latest_whales <= refresh_threshold
                    )
                )
                .order_by(TrackedToken.latest_whales.asc().nullsfirst(), TrackedToken.created_at.desc())
                .limit(limit)
            )
            
            for token_id, token_address, symbol in session.exec(statement):
                token_data_list.append({
                    "id": token_id,
                    "token_address": token_address,
                    "symbol": symbol or token_address[:8]
                })
            
            context.log.info(f"Found {len(token_data_list)} tokens that need whale data")
            
            if not token_data_list:
                context.add_output_metadata(metadata={"tokens_processed": 0, "whales_stored": 0})
                return
        
        # Process each token
        for token_data in token_data_list:
            token_id = token_data["id"]
            token_address = token_data["token_address"]
            token_symbol = token_data["symbol"]
            
            context.log.info(f"Processing token: {token_symbol}")
            
            try:
                # Get whales
                token_analyzer = Token(address=token_address, api_client=birdeye_api)
                whales_result = token_analyzer.get_whales(max_whales=max_whales_per_token)
                
                # Process whale data
                whale_objs = []
                if isinstance(whales_result, dict):
                    holders = whales_result.get("holders", [])
                    for holder in holders:
                        wallet_address = holder.get('owner')
                        ui_amount = holder.get('ui_amount')
                        
                        # Calculate percentage if supply info available
                        holdings_pct = None
                        supply_info = whales_result.get("supply")
                        if supply_info and supply_info.get("ui_amount") and ui_amount and supply_info.get("ui_amount") > 0:
                            try:
                                holdings_pct = (ui_amount / supply_info.get("ui_amount")) * 100
                            except (TypeError, ZeroDivisionError):
                                pass
                        
                        whale_obj = TokenWhale(
                            token_address=token_address,
                            wallet_address=wallet_address,
                            holdings_amount=ui_amount,
                            holdings_value=None,
                            holdings_pct=holdings_pct,
                            created_at=now,
                            updated_at=now,
                            fetched_txns=None
                        )
                        whale_objs.append(whale_obj)
                else:
                    context.log.warning(f"Unexpected return type from get_whales(): {type(whales_result)}")
                
                # Store whales and update token
                with db_manager.get_session() as session:
                    with session.begin():
                        # Update token timestamp
                        db_token = session.get(TrackedToken, token_id)
                        if db_token:
                            db_token.latest_whales = now
                            db_token.updated_at = now
                            
                            # Add whales
                            if whale_objs:
                                for whale in whale_objs:
                                    session.add(whale)
                                whales_stored += len(whale_objs)
                                context.log.info(f"Stored {len(whale_objs)} whales for {token_symbol}")
                
                tokens_processed += 1
                
            except Exception as e:
                context.log.error(f"Error processing whales for {token_symbol}: {str(e)}")
                # Continue to next token
            
            # Rate limiting
            time.sleep(sleep_seconds)
        
        context.log.info(f"Processed {tokens_processed} tokens, stored {whales_stored} whales")
        context.add_output_metadata(metadata={"tokens_processed": tokens_processed, "whales_stored": whales_stored})
        
    except Exception as e:
        context.log.error(f"Extract token whales operation failed: {str(e)}")
        raise


@asset(
    required_resource_keys={"db", "birdeye_api"},
    deps=["token_whales"],  # Explicit dependency on token_whales asset
    group_name="bronze"
)
def wallet_trade_history(context: AssetExecutionContext, config: TransactionParamsConfig) -> None:
    """Fetch trading history for whales using BirdEye API."""
    from datetime import datetime, timezone
    from sqlmodel import select, text
    from solana_pipeline.models.solana_models import TokenWhale, WalletTradeHistory
    from solana_pipeline.classes.holder import Holder
    
    # Get parameters
    limit = config.limit
    max_txns_per_whale = config.max_txns_per_whale
    sleep_seconds = config.sleep_seconds
    
    # Resources
    db_manager = context.resources.db
    birdeye_api = context.resources.birdeye_api
    
    # Timestamp for updates
    now = datetime.now(timezone.utc)
    
    # Metrics
    whales_processed = 0
    transactions_stored = 0
    
    # sleep to allow previous operations to complete
    time.sleep(5)
    
    def transform_trade(trade, wallet_address, token_address, timestamp):
        """Transform raw API trade data to WalletTradeHistory model."""
        tx_hash = trade.get('tx_hash', '')
        if not tx_hash:
            return None
            
        base = trade.get('base', {})
        quote = trade.get('quote', {})
        base_address = base.get('address', '')
        quote_address = quote.get('address', '')
        
        # Determine which is "from" and "to" based on type_swap
        if base.get('type_swap') == 'from' and quote.get('type_swap') == 'to':
            from_token, to_token = base, quote
        elif base.get('type_swap') == 'to' and quote.get('type_swap') == 'from':
            from_token, to_token = quote, base
        else:
            from_token, to_token = quote, base
        
        # Determine transaction type relative to our token
        transaction_type = 'UNKNOWN'
        if token_address == to_token.get('address'):
            transaction_type = 'BUY'
        elif token_address == from_token.get('address'):
            transaction_type = 'SELL'
            
        # Calculate value in USD if possible
        value_usd = None
        if token_address == base_address and base.get('ui_amount') is not None:
            price = trade.get('base_price') or base.get('nearest_price')
            if price:
                try:
                    value_usd = float(base.get('ui_amount')) * float(price)
                except (ValueError, TypeError):
                    pass
        elif token_address == quote_address and quote.get('ui_amount') is not None:
            price = trade.get('quote_price') or quote.get('nearest_price')
            if price:
                try:
                    value_usd = float(quote.get('ui_amount')) * float(price)
                except (ValueError, TypeError):
                    pass
        
        # Create timestamp from block_unix_time
        trade_timestamp = timestamp
        if trade.get('block_unix_time'):
            try:
                trade_timestamp = datetime.fromtimestamp(trade.get('block_unix_time'), tz=timezone.utc)
            except (ValueError, TypeError):
                pass
        
        return WalletTradeHistory(
            wallet_address=wallet_address,
            token_address=token_address,
            transaction_hash=tx_hash,
            source=trade.get('source', ''),
            block_unix_time=trade.get('block_unix_time'),
            tx_type=trade.get('tx_type', ''),
            timestamp=trade_timestamp,
            transaction_type=transaction_type,
            
            from_symbol=from_token.get('symbol', ''),
            from_address=from_token.get('address', ''),
            from_decimals=from_token.get('decimals'),
            from_amount=from_token.get('ui_amount'),
            from_raw_amount=str(from_token.get('amount', '')),
            
            to_symbol=to_token.get('symbol', ''),
            to_address=to_token.get('address', ''),
            to_decimals=to_token.get('decimals'),
            to_amount=to_token.get('ui_amount'),
            to_raw_amount=str(to_token.get('amount', '')),
            
            base_price=trade.get('base_price'),
            quote_price=trade.get('quote_price'),
            value_usd=value_usd,
            created_at=timestamp,
            processed_for_pnl=None,
            processed_at=None,
            processing_status='pending'
        )
    
    try:
        # Get whales needing transaction history
        whale_data_list = []
        with db_manager.get_session() as session:
            # Query for whales with NULL fetched_txns
            statement = (
                select(
                    TokenWhale.id, 
                    TokenWhale.wallet_address, 
                    TokenWhale.token_address
                )
                .where(TokenWhale.fetched_txns.is_(None))
                .order_by(TokenWhale.created_at.desc())
                .limit(limit)
            )
            
            for whale_id, wallet_address, token_address in session.exec(statement):
                whale_data_list.append({
                    'id': whale_id,
                    'wallet_address': wallet_address,
                    'token_address': token_address
                })
            
            context.log.info(f"Found {len(whale_data_list)} whales needing transaction history")
        
        # Process each whale
        for whale_data in whale_data_list:
            whale_id = whale_data['id']
            wallet_address = whale_data['wallet_address']
            token_address = whale_data['token_address']
            
            try:
                # Get raw trade history
                holder = Holder(address=wallet_address, api_client=birdeye_api)
                all_trades = holder.get_raw_trade_history(total_trades=max_txns_per_whale)
                
                # Transform trades to model objects
                txn_objs = []
                for trade in all_trades:
                    txn_obj = transform_trade(trade, wallet_address, token_address, now)
                    if txn_obj:
                        txn_objs.append(txn_obj)
                
                # Store transactions in a single DB transaction
                with db_manager.get_session() as session:
                    with session.begin():
                        # Update whale status
                        db_whale = session.get(TokenWhale, whale_id)
                        if db_whale:
                            db_whale.fetched_txns = now
                            db_whale.updated_at = now
                        
                        # Store transactions with duplicate detection
                        batch_stored = 0
                        for txn in txn_objs:
                            existing = session.exec(
                                select(WalletTradeHistory).where(
                                    WalletTradeHistory.wallet_address == txn.wallet_address,
                                    WalletTradeHistory.token_address == txn.token_address,
                                    WalletTradeHistory.transaction_hash == txn.transaction_hash
                                )
                            ).first()
                            
                            if not existing:
                                session.add(txn)
                                batch_stored += 1
                        
                        transactions_stored += batch_stored
                
                whales_processed += 1
                context.log.info(f"Processed whale {wallet_address[:10]}... - stored {batch_stored} transactions")
                
            except Exception as e:
                context.log.error(f"Error processing whale {wallet_address[:10]}...: {str(e)}")
            
            # Rate limiting between whales
            time.sleep(sleep_seconds)
        
        context.log.info(f"Processed {whales_processed} whales, stored {transactions_stored} transactions")
        context.add_output_metadata(metadata={"whales_processed": whales_processed, "transactions_stored": transactions_stored})
        
    except Exception as e:
        context.log.error(f"Operation failed: {str(e)}")
        raise


@asset(
    required_resource_keys={"db"},
    deps=["wallet_trade_history"],  # Explicit dependency on wallet_trade_history asset
    group_name="silver"
)
def wallet_pnl(context: AssetExecutionContext, config: PnLParamsConfig) -> None:
    """Calculate PnL metrics for wallets with unprocessed transactions."""
    from datetime import datetime, timedelta
    from sqlmodel import select, or_, and_, func, text
    from solana_pipeline.models.solana_models import TokenWhale, WalletTradeHistory, WalletPnL
    from solana_pipeline.classes.pnl_calculator import PnLCalculator
    
    # Get parameters
    limit = config.limit
    min_transactions = config.min_transactions
    sleep_seconds = config.sleep_seconds
    retry_attempts = config.retry_attempts
    
    # Resources and constants
    db_manager = context.resources.db
    pnl_calculator = PnLCalculator(db_manager)
    now = datetime.utcnow()
    time_period = 'all'
    all_tokens_marker = "ALL_TOKENS"
    
    # Metrics
    wallets_processed = 0
    successful_wallets = 0
    
    try:
        # Get wallets with unprocessed transactions
        unique_wallets = []
        with db_manager.get_session() as session:
            # Find wallets with unprocessed transaction data
            statement = (
                select(WalletTradeHistory.wallet_address)
                .where(
                    or_(
                        WalletTradeHistory.processed_for_pnl.is_(None),
                        WalletTradeHistory.processing_status == 'pending'
                    )
                )
                .group_by(WalletTradeHistory.wallet_address)
                .order_by(func.count(WalletTradeHistory.id).desc())  # Prioritize wallets with more transactions
                .limit(limit)
            )
            
            unique_wallets = [wallet for wallet in session.exec(statement).all()]
            context.log.info(f"Found {len(unique_wallets)} wallets with unprocessed transactions")
            
            if not unique_wallets:
                context.add_output_metadata(metadata={"wallets_processed": 0, "successful_wallets": 0})
                return
        
        # Process each wallet
        for wallet_address in unique_wallets:
            context.log.info(f"Processing wallet {wallet_address[:10]}...")
            
            # Verify transaction count
            with db_manager.get_session() as session:
                txn_count = session.exec(
                    select(func.count(WalletTradeHistory.id))
                    .where(WalletTradeHistory.wallet_address == wallet_address)
                ).one()
                
                if txn_count < min_transactions:
                    context.log.info(f"Insufficient transactions: {txn_count}/{min_transactions}")
                    continue
            
            # Check transaction data
            diagnostics = pnl_calculator.diagnose_transaction_data(wallet_address)
            if "error" in diagnostics:
                context.log.error(f"Diagnostic error: {diagnostics['error']}")
                continue
            
            # Calculate PnL with retry logic
            for attempt in range(retry_attempts):
                try:
                    # Calculate metrics
                    pnl_metrics = pnl_calculator.calculate_wallet_pnl(
                        wallet_address=wallet_address,
                        token_address=None,  # Uses ALL_TOKENS internally
                        time_period=time_period
                    )
                    
                    if not pnl_metrics:
                        context.log.warning(f"No PnL metrics calculated")
                        break
                    
                    # Calculate trade frequency (trades per day)
                    if 'first_transaction' in pnl_metrics and 'last_transaction' in pnl_metrics and pnl_metrics['first_transaction'] and pnl_metrics['last_transaction']:
                        first_tx = pnl_metrics['first_transaction']
                        last_tx = pnl_metrics['last_transaction']
                        trade_count = pnl_metrics.get('trade_count', 0)
                        
                        # Calculate days between transactions (minimum 1 day to avoid division by zero)
                        days_between = max(1, (last_tx - first_tx).total_seconds() / 86400)
                        trade_frequency = trade_count / days_between
                        
                        # Add to metrics
                        pnl_metrics['trade_frequency_daily'] = trade_frequency
                    
                    # Store PnL metrics and update transaction status
                    with db_manager.get_session() as session:
                        with session.begin():
                            # Check for existing PnL record
                            existing_pnl = session.exec(
                                select(WalletPnL).where(
                                    WalletPnL.wallet_address == wallet_address,
                                    WalletPnL.token_address == all_tokens_marker,
                                    WalletPnL.time_period == time_period
                                )
                            ).first()
                            
                            if existing_pnl:
                                # Update existing record
                                for key, value in pnl_metrics.items():
                                    if hasattr(existing_pnl, key):
                                        setattr(existing_pnl, key, value)
                                existing_pnl.processed_at = now  # Updated from "updated_at" to "processed_at"
                            else:
                                # Create new record
                                new_pnl = WalletPnL(
                                    wallet_address=wallet_address,
                                    token_address=all_tokens_marker,
                                    time_period=time_period,
                                    realized_pnl=pnl_metrics.get('realized_pnl', 0),
                                    unrealized_pnl=pnl_metrics.get('unrealized_pnl', 0),
                                    total_pnl=pnl_metrics.get('total_pnl', 0),
                                    win_rate=pnl_metrics.get('win_rate', 0),
                                    trade_count=pnl_metrics.get('trade_count', 0),
                                    avg_holding_time=pnl_metrics.get('avg_holding_time', 0),
                                    total_bought=pnl_metrics.get('total_bought', 0),
                                    total_sold=pnl_metrics.get('total_sold', 0),
                                    roi=pnl_metrics.get('roi', 0),
                                    trade_frequency_daily=pnl_metrics.get('trade_frequency_daily'),
                                    first_transaction=pnl_metrics.get('first_transaction'),
                                    last_transaction=pnl_metrics.get('last_transaction'),
                                    created_at=now,
                                    processed_at=now,  # Updated from "updated_at" to "processed_at"
                                    top_trader=False   # Initialize as not a top trader
                                )
                                
                                session.add(new_pnl)
                            
                            # Mark transactions as processed
                            session.execute(
                                text("""
                                    UPDATE bronze.wallet_trade_history 
                                    SET processed_for_pnl = TRUE, 
                                        processed_at = :now, 
                                        processing_status = 'completed' 
                                    WHERE wallet_address = :wallet
                                """),
                                {"now": now, "wallet": wallet_address}
                            )
                    
                    successful_wallets += 1
                    context.log.info(f"PnL calculated: {pnl_metrics.get('realized_pnl', 0):.2f} USD, "
                                  f"Freq: {pnl_metrics.get('trade_frequency_daily', 0):.1f} trades/day")
                    break
                    
                except Exception as e:
                    context.log.error(f"PnL calculation error: {str(e)}")
                    if attempt < retry_attempts - 1:
                        time.sleep(sleep_seconds * (attempt + 1))
            
            wallets_processed += 1
            time.sleep(sleep_seconds)
        
        context.log.info(f"Processed {wallets_processed} wallets, {successful_wallets} successful")
        context.add_output_metadata(metadata={"wallets_processed": wallets_processed, "successful_wallets": successful_wallets})
        
    except Exception as e:
        context.log.error(f"Operation failed: {str(e)}")
        raise


@asset(
    required_resource_keys={"db"},
    deps=["wallet_pnl"],  # Explicit dependency on wallet_pnl asset
    group_name="gold"
)
def top_traders(context: AssetExecutionContext, config: TraderParamsConfig) -> None:
    """Identify top traders based on PnL metrics and performance criteria."""
    from datetime import datetime, timedelta
    from sqlmodel import select, and_, func
    from sqlalchemy import text
    from solana_pipeline.models.solana_models import WalletPnL, TopTrader
    
    # Get parameters
    min_win_rate = config.min_win_rate
    min_realized_pnl = config.min_realized_pnl
    min_trades = config.min_trades
    max_trades_per_day = config.max_trades_per_day
    min_trades_per_day = config.min_trades_per_day
    recent_days = config.recent_days
    ranked_limit = config.ranked_limit
    
    # Resources and constants
    db_manager = context.resources.db
    now = datetime.utcnow()
    time_period = 'all'
    all_tokens_marker = "ALL_TOKENS"
    recent_threshold = now - timedelta(days=recent_days)
    
    # Metrics
    total_top_traders = 0
    
    try:
        # Process in a single transaction
        with db_manager.get_session() as session:
            with session.begin():
                # First reset all top_trader flags to false
                session.execute(
                    text("""
                        UPDATE silver.wallet_pnl 
                        SET top_trader = FALSE
                        WHERE time_period = :time_period
                    """),
                    {"time_period": time_period}
                )
                
                # Clear existing top traders 
                delete_stmt = select(TopTrader).where(TopTrader.time_period == time_period)
                existing_traders = session.exec(delete_stmt).all()
                for trader in existing_traders:
                    session.delete(trader)
                
                context.log.info(f"Cleared {len(existing_traders)} existing top trader records")
                
                # Get filtered wallets
                wallet_query = (
                    select(WalletPnL)
                    .where(
                        WalletPnL.token_address == all_tokens_marker,
                        WalletPnL.time_period == time_period,
                        WalletPnL.win_rate >= min_win_rate,
                        WalletPnL.realized_pnl > min_realized_pnl,
                        WalletPnL.roi > 0,
                        WalletPnL.trade_count >= min_trades,
                        WalletPnL.last_transaction >= recent_threshold,
                        WalletPnL.trade_frequency_daily <= max_trades_per_day,
                        WalletPnL.trade_frequency_daily >= min_trades_per_day
                    )
                    .order_by(WalletPnL.realized_pnl.desc())
                    .limit(ranked_limit)
                )
                
                qualifying_traders = session.exec(wallet_query).all()
                context.log.info(f"Found {len(qualifying_traders)} qualifying traders")
                
                # Collect wallet addresses for SQL update
                top_trader_wallets = []
                
                # Create TopTrader records with rankings
                top_traders = []
                for rank, trader in enumerate(qualifying_traders, 1):
                    # Create tags based on trader characteristics
                    tags = []
                    if trader.roi > 100:
                        tags.append("high_roi")
                    if trader.win_rate > 75:
                        tags.append("high_win_rate")
                    if trader.trade_count > 200:
                        tags.append("experienced")
                    if trader.trade_frequency_daily > 10:
                        tags.append("active_trader")
                    if trader.avg_holding_time and trader.avg_holding_time < 24:
                        tags.append("short_term")
                    elif trader.avg_holding_time and trader.avg_holding_time > 168:  # > 7 days
                        tags.append("long_term")
                    
                    top_trader = TopTrader(
                        wallet_address=trader.wallet_address,
                        realized_pnl=trader.realized_pnl,
                        unrealized_pnl=trader.unrealized_pnl,
                        total_pnl=trader.total_pnl,
                        win_rate=trader.win_rate,
                        trade_count=trader.trade_count,
                        roi=trader.roi,
                        time_period=time_period,
                        rank=rank,
                        first_transaction=trader.first_transaction,
                        last_transaction=trader.last_transaction,
                        avg_holding_time=trader.avg_holding_time,
                        total_bought=trader.total_bought,
                        total_sold=trader.total_sold,
                        trade_frequency_daily=trader.trade_frequency_daily,
                        tags=tags,
                        silver_pnl_id=trader.id,
                        created_at=now,
                        # Add these new fields with their defaults
                        added_to_webhook=False,
                        added_at=None
                    )
                                    
                    top_traders.append(top_trader)
                    top_trader_wallets.append(trader.wallet_address)
                
                # Add all top traders to the session
                for trader in top_traders:
                    session.add(trader)
                
                # Update the top_trader flag in WalletPnL
                if top_trader_wallets:
                    # Update the top_trader flag and processed_at timestamp
                    wallet_list = "', '".join(top_trader_wallets)
                    session.execute(
                        text(f"""
                            UPDATE silver.wallet_pnl 
                            SET top_trader = TRUE, 
                                processed_at = :now
                            WHERE wallet_address IN ('{wallet_list}')
                            AND token_address = :token
                            AND time_period = :time_period
                        """),
                        {"now": now, "token": all_tokens_marker, "time_period": time_period}
                    )
                
                total_top_traders = len(top_traders)
                context.log.info(f"Added {total_top_traders} top traders and updated wallet_pnl records")
                
                # Log top 5 traders for verification
                if top_traders:
                    context.log.info("Top 5 traders:")
                    for i, trader in enumerate(top_traders[:5], 1):
                        tag_str = ', '.join(trader.tags) if trader.tags else 'none'
                        context.log.info(f"#{i}: {trader.wallet_address[:10]}... - "
                                     f"PnL: ${trader.realized_pnl:.2f}, "
                                     f"Win Rate: {trader.win_rate:.1f}%, "
                                     f"Trades: {trader.trade_count} ({trader.trade_frequency_daily:.1f}/day), "
                                     f"Tags: {tag_str}")
        
        context.add_output_metadata(metadata={"total_top_traders": total_top_traders})
        
    except Exception as e:
        context.log.error(f"Operation failed: {str(e)}")
        raise