from datetime import datetime, timezone, timedelta
import pandas as pd
import logging
from typing import Dict, Any, List, Optional
from sqlmodel import select, and_, func, Session
from sqlalchemy import text

# Set up logging
logger = logging.getLogger(__name__)

class PnLCalculator:
    """
    Standalone PnL calculator that works directly with the database.
    Tracks token positions and calculates PnL for token swaps on Solana.
    """
    
    def __init__(self, db_manager):
        """
        Initialize the PnL calculator.
        
        Args:
            db_manager: Database manager instance for querying transaction history
        """
        self.db = db_manager
        # SOL token address - common in many swaps
        self.SOL_ADDRESS = "So11111111111111111111111111111111111111112"
    
    def calculate_wallet_pnl(self, wallet_address: str, 
                          token_address: Optional[str] = None, 
                          time_period: str = 'all') -> Dict[str, Any]:
        """
        Calculate PnL metrics for a wallet address, optionally for a specific token.
        
        Args:
            wallet_address (str): The wallet address to calculate PnL for
            token_address (str, optional): Address of specific token to calculate PnL for
                                          If None, calculates across all tokens
            time_period (str): 'all', 'week', 'month', etc.
            
        Returns:
            Dict[str, Any]: PnL metrics
        """
        # Define time filters
        current_time = datetime.now(timezone.utc)
        time_filters = {
            'all': None,
            'week': current_time - timedelta(days=7),
            'month': current_time - timedelta(days=30),
            'quarter': current_time - timedelta(days=90)
        }
        start_time = time_filters.get(time_period.lower())
        
        try:
            # Build query for the transaction history
            query_str = """
            SELECT * FROM bronze.wallet_trade_history
            WHERE wallet_address = :wallet_address
            """
            params = {"wallet_address": wallet_address}
            
            if token_address and token_address != "ALL_TOKENS":
                query_str += " AND (token_address = :token_address OR from_address = :token_address OR to_address = :token_address)"
                params["token_address"] = token_address
                
            if start_time:
                query_str += " AND timestamp >= :start_time"
                params["start_time"] = start_time
                
            query_str += " ORDER BY timestamp ASC"
            
            # Convert to SQLAlchemy text object
            query = text(query_str)
            
            # Execute query
            with self.db.get_session() as session:
                result = session.execute(query, params)
                trades = result.all()
                
                if not trades:
                    logger.info(f"No transactions found for {wallet_address}")
                    return self._create_empty_metrics(wallet_address, token_address, time_period)
                
                # Convert to DataFrame
                columns = result.keys()
                txn_df = pd.DataFrame(trades, columns=columns)
                
                # Process transactions and calculate metrics
                token_positions, pnl_data = self._process_token_swaps(txn_df, token_address)
                
                # If we're tracking all tokens, calculate combined metrics
                if token_address is None or token_address == "ALL_TOKENS":
                    # Sum up metrics across all tokens
                    realized_pnl = sum(data['realized_pnl'] for data in pnl_data.values())
                    unrealized_pnl = sum(data['unrealized_pnl'] for data in pnl_data.values())
                    total_pnl = realized_pnl + unrealized_pnl
                    
                    # Combine all trade history
                    all_trade_history = []
                    for token, data in pnl_data.items():
                        if 'trade_history' in data:
                            for trade in data['trade_history']:
                                trade['token_address'] = token
                                all_trade_history.append(trade)
                    
                    trade_history_df = pd.DataFrame(all_trade_history) if all_trade_history else pd.DataFrame()
                else:
                    # Use metrics for the specific token
                    if token_address in pnl_data:
                        realized_pnl = pnl_data[token_address]['realized_pnl']
                        unrealized_pnl = pnl_data[token_address]['unrealized_pnl']
                        total_pnl = realized_pnl + unrealized_pnl
                        
                        trade_history = pnl_data[token_address].get('trade_history', [])
                        for trade in trade_history:
                            trade['token_address'] = token_address
                        trade_history_df = pd.DataFrame(trade_history) if trade_history else pd.DataFrame()
                    else:
                        realized_pnl = 0
                        unrealized_pnl = 0
                        total_pnl = 0
                        trade_history_df = pd.DataFrame()
                
                # Calculate basic metrics
                metrics = {
                    'wallet_address': wallet_address,
                    'token_address': token_address or "ALL_TOKENS",
                    'time_period': time_period,
                    'realized_pnl': realized_pnl,
                    'unrealized_pnl': unrealized_pnl,
                    'total_pnl': total_pnl
                }
                
                # Calculate additional metrics from trade history
                if not trade_history_df.empty:
                    metrics.update(self._calculate_trade_metrics(trade_history_df, txn_df))
                    metrics['first_transaction'] = pd.to_datetime(txn_df['timestamp'].min())
                    metrics['last_transaction'] = pd.to_datetime(txn_df['timestamp'].max())
                else:
                    metrics.update(self._create_empty_trade_metrics())
                    
                logger.info(f"Final PnL metrics for {wallet_address}: {metrics}")
                
                return metrics
                
        except Exception as e:
            logger.error(f"Error calculating PnL for {wallet_address} and token {token_address}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return self._create_empty_metrics(wallet_address, token_address, time_period)

    def _process_token_swaps(self, txn_df: pd.DataFrame, specific_token: Optional[str]) -> tuple:
        """
        Process token swap transactions to track token positions and calculate PnL.
        
        Args:
            txn_df (pd.DataFrame): DataFrame of transactions
            specific_token (str, optional): Specific token to focus on, or None for all tokens
            
        Returns:
            tuple: (positions dict, pnl_data dict)
        """
        # Sort by timestamp
        txn_df = txn_df.sort_values('timestamp')
        
        # Initialize data structures
        token_positions = {}  # Track token lots by token address
        pnl_data = {}  # Track PnL data by token address
        
        # Process each transaction
        for idx, row in txn_df.iterrows():
            try:
                tx_hash = row.get('transaction_hash', f"tx-{idx}")
                timestamp = row['timestamp'].timestamp() if hasattr(row['timestamp'], 'timestamp') else row['timestamp']
                
                # Get transaction details
                from_token = row['from_address']
                to_token = row['to_address']
                from_amount = float(row['from_amount'] or 0)
                to_amount = float(row['to_amount'] or 0)
                
                # Skip transactions with zero amounts
                if from_amount <= 0 and to_amount <= 0:
                    logger.info(f"Skipping transaction {tx_hash} - zero amounts")
                    continue
                
                # Process outgoing token (selling)
                if from_amount > 0:
                    # If this token is not in our positions yet, initialize it
                    if from_token not in token_positions:
                        token_positions[from_token] = []
                        pnl_data[from_token] = {
                            'realized_pnl': 0.0,
                            'unrealized_pnl': 0.0,
                            'trade_history': []
                        }
                    
                    # Calculate the value of tokens being sold
                    # For price determination, use value_usd when available
                    if row.get('value_usd') is not None and row['value_usd'] > 0:
                        # If value_usd is set, use it directly
                        sale_value = float(row['value_usd'])
                        sale_price = sale_value / from_amount
                    else:
                        # Otherwise use base_price or quote_price
                        sale_price = float(row.get('base_price') or row.get('quote_price') or 0)
                        if sale_price <= 0:
                            # If we don't have a price, try to derive it from the other side
                            if 'value_usd' in row and row['value_usd'] and to_amount > 0:
                                # Derive from total value and other token amount
                                sale_value = float(row['value_usd'])
                                sale_price = sale_value / from_amount
                            else:
                                logger.info(f"Cannot determine price for selling {from_token} in tx {tx_hash}")
                                continue
                        sale_value = from_amount * sale_price
                    
                    # Use FIFO to match this sale with previous purchases
                    remaining_to_sell = from_amount
                    total_cost_basis = 0
                    new_lots = []
                    realized_pnl = 0
                    
                    # If we have positions in this token, calculate realized PnL
                    if token_positions[from_token]:
                        for lot in token_positions[from_token]:
                            if remaining_to_sell <= 0:
                                # Keep the rest of the lots
                                new_lots.append(lot)
                                continue
                            
                            if lot['amount'] <= remaining_to_sell:
                                # Sell entire lot
                                lot_sale_value = lot['amount'] * sale_price
                                lot_realized_pnl = lot_sale_value - lot['cost_basis']
                                realized_pnl += lot_realized_pnl
                                total_cost_basis += lot['cost_basis']
                                
                                # Log the trade
                                pnl_data[from_token]['trade_history'].append({
                                    'tx_hash': tx_hash,
                                    'timestamp': row['timestamp'],
                                    'amount': lot['amount'],
                                    'buy_price': lot['price'],
                                    'sell_price': sale_price,
                                    'buy_cost': lot['cost_basis'],
                                    'sell_value': lot_sale_value,
                                    'pnl': lot_realized_pnl,
                                    'held_duration': timestamp - lot['timestamp']
                                })
                                
                                remaining_to_sell -= lot['amount']
                            else:
                                # Sell partial lot
                                sell_fraction = remaining_to_sell / lot['amount']
                                lot_cost_basis = lot['cost_basis'] * sell_fraction
                                lot_sale_value = remaining_to_sell * sale_price
                                lot_realized_pnl = lot_sale_value - lot_cost_basis
                                realized_pnl += lot_realized_pnl
                                total_cost_basis += lot_cost_basis
                                
                                # Log the trade
                                pnl_data[from_token]['trade_history'].append({
                                    'tx_hash': tx_hash,
                                    'timestamp': row['timestamp'],
                                    'amount': remaining_to_sell,
                                    'buy_price': lot['price'],
                                    'sell_price': sale_price,
                                    'buy_cost': lot_cost_basis,
                                    'sell_value': lot_sale_value,
                                    'pnl': lot_realized_pnl,
                                    'held_duration': timestamp - lot['timestamp']
                                })
                                
                                # Keep the remaining lot
                                new_lot = {
                                    'amount': lot['amount'] - remaining_to_sell,
                                    'price': lot['price'],
                                    'cost_basis': lot['cost_basis'] - lot_cost_basis,
                                    'timestamp': lot['timestamp'],
                                    'tx_hash': lot['tx_hash']
                                }
                                new_lots.append(new_lot)
                                remaining_to_sell = 0
                        
                        # Update the token position with the remaining lots
                        token_positions[from_token] = new_lots
                        
                        # Update realized PnL
                        pnl_data[from_token]['realized_pnl'] += realized_pnl
                        
                        logger.info(f"Sold {from_amount - remaining_to_sell} of {from_token} "
                                 f"for {sale_value:.2f} USD, realized PnL: {realized_pnl:.2f} USD")
                    
                    # If we sold more than we had recorded positions for, that's unusual but log it
                    if remaining_to_sell > 0:
                        logger.warning(f"Sold {remaining_to_sell} of {from_token} with no recorded purchase")
                
                # Process incoming token (buying)
                if to_amount > 0:
                    # If this token is not in our positions yet, initialize it
                    if to_token not in token_positions:
                        token_positions[to_token] = []
                        pnl_data[to_token] = {
                            'realized_pnl': 0.0,
                            'unrealized_pnl': 0.0,
                            'trade_history': []
                        }
                    
                    # Calculate the cost basis of tokens being bought
                    if row.get('value_usd') is not None and row['value_usd'] > 0:
                        # If value_usd is set, use it directly
                        cost_basis = float(row['value_usd'])
                        price = cost_basis / to_amount
                    else:
                        # Otherwise use base_price or quote_price
                        price = float(row.get('base_price') or row.get('quote_price') or 0)
                        if price <= 0:
                            # If we don't have a price, try to derive it from the other side
                            if 'value_usd' in row and row['value_usd'] and from_amount > 0:
                                # Derive from total value and other token amount
                                cost_basis = float(row['value_usd'])
                                price = cost_basis / to_amount
                            else:
                                logger.info(f"Cannot determine price for buying {to_token} in tx {tx_hash}")
                                continue
                        cost_basis = to_amount * price
                    
                    # Add the new lot to our position
                    new_lot = {
                        'amount': to_amount,
                        'price': price,
                        'cost_basis': cost_basis,
                        'timestamp': timestamp,
                        'tx_hash': tx_hash
                    }
                    token_positions[to_token].append(new_lot)
                    
                    logger.info(f"Bought {to_amount} of {to_token} for {cost_basis:.2f} USD at {price:.6f} per token")
                
            except Exception as e:
                logger.error(f"Error processing transaction {tx_hash}: {e}")
                continue
        
        # Calculate unrealized PnL using the latest known price for each token
        for token, lots in token_positions.items():
            if not lots:
                continue
            
            # Try to find the latest price
            latest_price = None
            
            # Look for the most recent transaction involving this token
            for idx in reversed(range(len(txn_df))):
                row = txn_df.iloc[idx]
                if (row['from_address'] == token or row['to_address'] == token) and row.get('base_price'):
                    latest_price = float(row['base_price'])
                    break
                if (row['from_address'] == token or row['to_address'] == token) and row.get('quote_price'):
                    latest_price = float(row['quote_price'])
                    break
            
            if latest_price:
                # Calculate unrealized PnL for this token
                total_amount = sum(lot['amount'] for lot in lots)
                total_cost_basis = sum(lot['cost_basis'] for lot in lots)
                current_value = total_amount * latest_price
                unrealized_pnl = current_value - total_cost_basis
                
                pnl_data[token]['unrealized_pnl'] = unrealized_pnl
                
                logger.info(f"Token {token}: {total_amount} tokens with cost basis {total_cost_basis:.2f} USD, "
                         f"current value {current_value:.2f} USD, unrealized PnL: {unrealized_pnl:.2f} USD")
        
        # Filter to specific token if requested
        if specific_token and specific_token != "ALL_TOKENS":
            filtered_positions = {k: v for k, v in token_positions.items() if k == specific_token}
            filtered_pnl = {k: v for k, v in pnl_data.items() if k == specific_token}
            return filtered_positions, filtered_pnl
        
        return token_positions, pnl_data

    def _calculate_trade_metrics(self, trade_history_df: pd.DataFrame, txn_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate aggregated trade metrics from trade history.
        
        Args:
            trade_history_df (pd.DataFrame): DataFrame of trade history records
            txn_df (pd.DataFrame): DataFrame of all transactions
            
        Returns:
            Dict[str, Any]: Calculated metrics
        """
        # Get timestamp range for frequency calculation
        first_trade = pd.to_datetime(txn_df['timestamp'].min())
        last_trade = pd.to_datetime(txn_df['timestamp'].max())
        trading_period_days = (last_trade - first_trade).total_seconds() / 86400
        
        # Calculate metrics
        metrics = {
            'trade_count': len(trade_history_df),
            'total_bought': sum(trade_history_df['buy_cost']) if 'buy_cost' in trade_history_df.columns else 0,
            'total_sold': sum(trade_history_df['sell_value']) if 'sell_value' in trade_history_df.columns else 0,
            'win_rate': 0,
            'roi': 0,
            'avg_holding_time': 0,
            'avg_transaction_amount_usd': 0,
            'trade_frequency_daily': 0
        }
        
        # Calculate average transaction amount (in USD)
        all_values = []
        if 'buy_cost' in trade_history_df.columns:
            all_values.extend(trade_history_df['buy_cost'].dropna().tolist())
        if 'sell_value' in trade_history_df.columns:
            all_values.extend(trade_history_df['sell_value'].dropna().tolist())
            
        metrics['avg_transaction_amount_usd'] = sum(all_values) / len(all_values) if all_values else 0
        
        # Win rate
        win_count = len(trade_history_df[trade_history_df['pnl'] > 0]) if 'pnl' in trade_history_df.columns else 0
        metrics['win_rate'] = (win_count / len(trade_history_df) * 100) if not trade_history_df.empty else 0
        
        # ROI
        if metrics['total_bought'] > 0:
            metrics['roi'] = (metrics['total_sold'] - metrics['total_bought']) / metrics['total_bought'] * 100
        
        # Average holding time (in hours)
        if 'held_duration' in trade_history_df.columns:
            holding_hours = [duration / 3600 for duration in trade_history_df['held_duration'] if duration is not None]
            metrics['avg_holding_time'] = sum(holding_hours) / len(holding_hours) if holding_hours else 0
        
        # Trade frequency
        if trading_period_days > 0:
            metrics['trade_frequency_daily'] = metrics['trade_count'] / trading_period_days
        else:
            metrics['trade_frequency_daily'] = metrics['trade_count']  # All trades in same day
        
        return metrics
    
    def _create_empty_metrics(self, wallet_address: str, token_address: Optional[str], time_period: str) -> Dict[str, Any]:
        """Create empty metrics dictionary for cases with no transactions."""
        return {
            'wallet_address': wallet_address,
            'token_address': token_address or "ALL_TOKENS",
            'time_period': time_period,
            'realized_pnl': 0,
            'unrealized_pnl': 0,
            'total_pnl': 0,
            'win_rate': 0,
            'trade_count': 0,
            'avg_holding_time': 0,
            'total_bought': 0,
            'total_sold': 0,
            'roi': 0,
            'avg_transaction_amount_usd': 0,
            'trade_frequency_daily': 0,
            'first_transaction': None,
            'last_transaction': None
        }
    
    def _create_empty_trade_metrics(self) -> Dict[str, Any]:
        """Create empty trade metrics dictionary."""
        return {
            'trade_count': 0,
            'total_bought': 0,
            'total_sold': 0,
            'win_rate': 0,
            'roi': 0,
            'avg_holding_time': 0,
            'avg_transaction_amount_usd': 0,
            'trade_frequency_daily': 0
        }
        
    def diagnose_transaction_data(self, wallet_address: str) -> Dict[str, Any]:
        """
        Diagnose transaction data for a wallet to identify issues affecting PnL calculation.
        
        Args:
            wallet_address (str): The wallet address to analyze
            
        Returns:
            Dict[str, Any]: Diagnostic information
        """
        try:
            # Build query to get all transactions for this wallet
            query = text("""
            SELECT * FROM bronze.wallet_trade_history
            WHERE wallet_address = :wallet_address
            ORDER BY timestamp ASC
            """)
            params = {"wallet_address": wallet_address}
            
            # Execute query
            with self.db.get_session() as session:
                result = session.execute(query, params)
                trades = result.all()
                
                if not trades:
                    return {"error": f"No transactions found for wallet {wallet_address}"}
                
                # Convert to DataFrame
                columns = result.keys()
                txn_df = pd.DataFrame(trades, columns=columns)
                
                # Analyze transactions
                swaps_with_value = 0
                swaps_without_value = 0
                
                for _, row in txn_df.iterrows():
                    from_amount = float(row.get('from_amount') or 0)
                    to_amount = float(row.get('to_amount') or 0)
                    has_value = row.get('value_usd') is not None and float(row.get('value_usd') or 0) > 0
                    
                    if from_amount > 0 and to_amount > 0:
                        if has_value:
                            swaps_with_value += 1
                        else:
                            swaps_without_value += 1
                
                # Analyze token types
                token_count = len(set(txn_df['token_address'].tolist()))
                from_tokens = set(txn_df['from_address'].tolist())
                to_tokens = set(txn_df['to_address'].tolist())
                unique_tokens = from_tokens.union(to_tokens)
                
                # Generate diagnostics
                diagnostics = {
                    "total_transactions": len(txn_df),
                    "transaction_types": txn_df['transaction_type'].value_counts().to_dict() if 'transaction_type' in txn_df.columns else {},
                    "token_stats": {
                        "tracked_tokens": token_count,
                        "unique_from_tokens": len(from_tokens),
                        "unique_to_tokens": len(to_tokens),
                        "total_unique_tokens": len(unique_tokens)
                    },
                    "swap_stats": {
                        "swaps_with_value": swaps_with_value,
                        "swaps_without_value": swaps_without_value
                    },
                    "time_range": {
                        "first": txn_df['timestamp'].min().isoformat() if not txn_df.empty else None,
                        "last": txn_df['timestamp'].max().isoformat() if not txn_df.empty else None
                    },
                    "value_stats": {
                        "value_usd_mean": float(txn_df['value_usd'].mean()) if 'value_usd' in txn_df.columns else None,
                        "value_usd_min": float(txn_df['value_usd'].min()) if 'value_usd' in txn_df.columns else None,
                        "value_usd_max": float(txn_df['value_usd'].max()) if 'value_usd' in txn_df.columns else None,
                        "value_usd_null_count": int(txn_df['value_usd'].isnull().sum()) if 'value_usd' in txn_df.columns else None,
                        "base_price_null_count": int(txn_df['base_price'].isnull().sum()) if 'base_price' in txn_df.columns else None,
                        "quote_price_null_count": int(txn_df['quote_price'].isnull().sum()) if 'quote_price' in txn_df.columns else None
                    },
                    "amount_stats": {
                        "from_amount_null_count": int(txn_df['from_amount'].isnull().sum()) if 'from_amount' in txn_df.columns else None,
                        "to_amount_null_count": int(txn_df['to_amount'].isnull().sum()) if 'to_amount' in txn_df.columns else None,
                        "zero_from_amount_count": int((txn_df['from_amount'] == 0).sum()) if 'from_amount' in txn_df.columns else None,
                        "zero_to_amount_count": int((txn_df['to_amount'] == 0).sum()) if 'to_amount' in txn_df.columns else None
                    }
                }
                
                # Identify potential issues
                issues = []
                
                # Check for transactions with no value information
                if diagnostics["value_stats"]["value_usd_null_count"] > 0:
                    issues.append({
                        "issue": "missing_value_usd",
                        "count": diagnostics["value_stats"]["value_usd_null_count"],
                        "percentage": (diagnostics["value_stats"]["value_usd_null_count"] / len(txn_df)) * 100
                    })
                
                # Check for transactions with zero amounts
                zero_amount_count = diagnostics["amount_stats"]["zero_from_amount_count"] + diagnostics["amount_stats"]["zero_to_amount_count"]
                if zero_amount_count > 0:
                    issues.append({
                        "issue": "zero_amounts",
                        "count": zero_amount_count,
                        "percentage": (zero_amount_count / (2 * len(txn_df))) * 100  # Divide by 2*len since we're counting both from and to
                    })
                
                diagnostics["issues"] = issues
                
                # Sample transactions
                sample_txns = []
                for i, row in txn_df.head(5).iterrows():
                    sample_txns.append({
                        "tx_hash": row.get('transaction_hash', ''),
                        "timestamp": row.get('timestamp').isoformat() if hasattr(row.get('timestamp'), 'isoformat') else str(row.get('timestamp')),
                        "token_address": row.get('token_address', ''),
                        "from_address": row.get('from_address', ''),
                        "to_address": row.get('to_address', ''),
                        "from_amount": float(row.get('from_amount', 0) or 0),
                        "to_amount": float(row.get('to_amount', 0) or 0),
                        "value_usd": float(row.get('value_usd', 0) or 0),
                        "base_price": float(row.get('base_price', 0) or 0),
                        "quote_price": float(row.get('quote_price', 0) or 0)
                    })
                
                diagnostics["sample_transactions"] = sample_txns
                
                return diagnostics
                
        except Exception as e:
            logger.error(f"Error diagnosing transaction data for wallet {wallet_address}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"error": str(e)}