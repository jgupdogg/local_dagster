"""Token class for Solana Pipeline."""
import logging
from typing import Dict, Any, List, Optional
import time

# Configure logging
logger = logging.getLogger("TokenAnalyzer")

class Token:
    """
    A minimal class to fetch data about a token on the Solana blockchain.
    Only provides fetch methods that directly return API responses without any database operations.
    """
    
    def __init__(self, address: str, blockchain: str = "solana", api_client=None):
        """
        Initialize a Token object.
        
        Args:
            address (str): The token address
            blockchain (str): The blockchain name (default: "solana")
            api_client: BirdEye API client instance
        """
        self.address = address
        self.blockchain = blockchain
        self.api_client = api_client
        self.logger = logging.getLogger(__name__)

    def get_whales(self, limit: int = 100, max_whales: int = 100) -> dict:
        """
        Get raw list of token whale holders from BirdEye API, handling pagination if needed.
        
        Args:
            limit (int): Maximum number of holders per API call (1-100)
            max_whales (int): Maximum total number of holders to return
                
        Returns:
            dict: Dictionary containing holders list, supply info, and token address
        """
        try:
            if not self.api_client:
                self.logger.error("API client is required to fetch whale data")
                return {"holders": [], "supply": None, "token_address": self.address}
            
            self.logger.info(f"Fetching up to {max_whales} holders for {self.address}")
            
            # Ensure limit is within API constraints
            api_limit = min(limit, 100)  # API maximum is 100
            all_holders = []
            supply_info = None
            offset = 0
            max_offset = 10000  # API constraint
            
            # Paginate through results until we reach max_whales or run out of data
            while len(all_holders) < max_whales and offset < max_offset:
                # Calculate how many records to fetch in this iteration
                current_limit = min(api_limit, max_whales - len(all_holders))
                
                self.logger.info(f"Fetching holders batch: offset={offset}, limit={current_limit}")
                response = self.api_client.token.get_token_top_holders(
                    address=self.address,
                    limit=current_limit,
                    offset=offset
                )
                
                # Extract holders data
                if isinstance(response, dict) and 'data' in response:
                    if 'items' in response['data'] and isinstance(response['data']['items'], list):
                        batch_holders = response['data']['items']
                        all_holders.extend(batch_holders)
                        
                        # Store supply info from first response
                        if supply_info is None and 'supply' in response['data']:
                            supply_info = response['data']['supply']
                        
                        self.logger.info(f"Added {len(batch_holders)} holders, total so far: {len(all_holders)}")
                        
                        # If we got fewer items than requested, we've reached the end
                        if len(batch_holders) < current_limit:
                            self.logger.info(f"Reached end of available holders at {len(all_holders)} records")
                            break
                    else:
                        self.logger.warning("Expected 'items' not found in response['data']")
                        break
                else:
                    self.logger.warning(f"Invalid response format when fetching holders")
                    break
                
                # Update offset for next iteration
                offset += current_limit
            
            self.logger.info(f"Completed fetching {len(all_holders)} holders for {self.address}")
            
            return {
                "holders": all_holders,
                "supply": supply_info,
                "token_address": self.address
            }
                    
        except Exception as e:
            self.logger.error(f"Error getting token holders for {self.address}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return {"holders": [], "supply": None, "token_address": self.address}

    def get_raw_token_overview(self) -> dict:
        """
        Get raw token overview data from BirdEye API.
        
        Returns:
            dict: Token overview dictionary as returned by the BirdEye API
        """
        try:
            if not self.api_client:
                self.logger.error("API client is required to fetch token overview")
                return {}
            
            # Fetch token overview
            self.logger.info(f"Fetching token overview for {self.address}")
            response = self.api_client.token.get_token_overview(
                address=self.address,
                blockchain=self.blockchain
            )
            
            if isinstance(response, dict) and 'data' in response:
                self.logger.info(f"Successfully retrieved token overview for {self.address}")
                return response['data']
            else:
                self.logger.warning(f"Invalid response format for token overview: {self.address}")
                return {}
                
        except Exception as e:
            self.logger.error(f"Error getting token overview for {self.address}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return {}
    
    def get_raw_token_metadata(self) -> dict:
        """
        Get raw token metadata from BirdEye API.
        
        Returns:
            dict: Token metadata dictionary as returned by the BirdEye API
        """
        try:
            if not self.api_client:
                self.logger.error("API client is required to fetch token metadata")
                return {}
            
            # Fetch token metadata
            self.logger.info(f"Fetching token metadata for {self.address}")
            response = self.api_client.token.get_token_metadata(
                token=self.address,
                blockchain=self.blockchain
            )
            
            if isinstance(response, dict) and 'data' in response:
                self.logger.info(f"Successfully retrieved token metadata for {self.address}")
                return response['data']
            else:
                self.logger.warning(f"Invalid response format for token metadata: {self.address}")
                return {}
                
        except Exception as e:
            self.logger.error(f"Error getting token metadata for {self.address}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return {}
    
    def get_raw_price_history(self, time_period: str = "1d") -> list:
        """
        Get raw price history data from BirdEye API.
        
        Args:
            time_period (str): Time period for price history (e.g., "1d", "7d", "30d")
            
        Returns:
            list: Price history data as returned by the BirdEye API
        """
        try:
            if not self.api_client:
                self.logger.error("API client is required to fetch price history")
                return []
            
            # Fetch price history
            self.logger.info(f"Fetching price history for {self.address} over {time_period}")
            response = self.api_client.token.get_price_history(
                token=self.address,
                time_period=time_period,
                blockchain=self.blockchain
            )
            
            if isinstance(response, dict) and 'data' in response and 'items' in response['data']:
                price_data = response['data']['items']
                self.logger.info(f"Retrieved {len(price_data)} price points for {self.address}")
                return price_data
            else:
                self.logger.warning(f"Invalid response format for price history: {self.address}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error getting price history for {self.address}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return []
