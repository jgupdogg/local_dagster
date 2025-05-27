import logging
from typing import Dict, Any, List, Optional
import time

# Configure logging
logger = logging.getLogger("HolderAnalyzer")

class Holder:
    """
    A minimal class to fetch raw data about a wallet (holder) on the Solana blockchain.
    Only provides raw fetch methods that directly return API responses without any database operations.
    """
    
    def __init__(self, address: str, blockchain: str = "solana", api_client=None):
        """
        Initialize a Holder object.
        
        Args:
            address (str): The wallet address
            blockchain (str): The blockchain name (default: "solana")
            api_client: BirdEye API client instance
        """
        self.address = address
        self.blockchain = blockchain
        self.birdeye = api_client

    def get_raw_trade_history(self, total_trades: int = 100, 
                         sleep_between_calls: float = 0.5) -> List[Dict[str, Any]]:
        """
        Get raw trading history for this wallet address from BirdEye API with pagination.
        
        Args:
            tx_type (str): Type of transaction (default: "swap")
            total_trades (int): Total number of trades to retrieve
            sleep_between_calls (float): Time to sleep between API calls to avoid rate limiting
                
        Returns:
            List[Dict[str, Any]]: Raw list of trade data dictionaries as returned by the API
        """
        try:
            # Initialize variables for pagination
            offset = 0
            limit_per_call = min(100, total_trades)  # API maximum is 100
            all_trades = []
            has_next = True
            max_offset = 10000  # API constraint
            
            # Fetch trades with pagination
            while has_next and len(all_trades) < total_trades and offset < max_offset:
                remaining = total_trades - len(all_trades)
                current_limit = min(limit_per_call, remaining, max_offset - offset)
                
                # Prepare API call parameters
                params = {
                    "address": self.address,
                    "offset": offset,
                    "limit": current_limit
                }
                
                # Call API
                response = self.birdeye.trader.get_trades_seek_by_time(**params)
                
                # Check for valid response
                if not (response and 'data' in response and 'items' in response['data'] and response.get('success', False)):
                    break
                
                # Extract trades data
                trades_data = response['data']['items']
                has_next = response['data'].get('hasNext', False)
                
                # If no trades in this batch, stop
                if not trades_data:
                    break
                
                # Add raw trade data to our list
                all_trades.extend(trades_data)
                
                # Update offset for next batch
                offset += len(trades_data)
                
                # Sleep between calls to respect rate limits
                if has_next and len(all_trades) < total_trades and offset < max_offset:
                    time.sleep(sleep_between_calls)
            
            # Return the raw list of trades
            return all_trades
                    
        except Exception as e:
            # Log error but don't raise to allow graceful handling
            if hasattr(self, 'logger'):
                self.logger.error(f"Error getting trade history for {self.address}: {str(e)}")
            return []
            
    def get_raw_token_list(self) -> list:
        """
        Get raw list of tokens held by this wallet address from BirdEye API.
        
        Returns:
            list: List of token dictionaries as returned by the BirdEye API
        """
        try:
            # Call API
            response = self.birdeye.wallet.get_token_list(wallet=self.address)
            
            if response and 'data' in response and 'items' in response['data'] and response.get('success', False):
                # Extract token list from the response
                tokens_data = response['data']['items']
                
                if not tokens_data:
                    logger.warning(f"No tokens found for wallet {self.address}")
                    return []
                
                logger.info(f"Retrieved {len(tokens_data)} tokens for wallet {self.address}")
                return tokens_data
            else:
                logger.warning(f"Failed to get token list from BirdEye API for {self.address}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting token list for wallet {self.address}: {e}")
            return []