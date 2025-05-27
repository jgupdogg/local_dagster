"""API resources for Solana Pipeline."""
from dagster import resource, Field, StringSource
import os
import requests
from typing import Optional, Dict, Any, List
import logging
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import datetime

# Configure logging
logger = logging.getLogger(__name__)

class BirdEyeClient:
    """Client for BirdEye API."""
    BASE_URL = "https://public-api.birdeye.so"
    
    def __init__(self, api_key):
        """Initialize BirdEye client."""
        self.api_key = api_key
        self.chain = "solana"
        self.headers = {
            "accept": "application/json",
            "X-API-KEY": self.api_key,
            "x-chain": self.chain
        }
        self.logger = logging.getLogger(__name__)
        
        # Initialize endpoints
        self.defi = self.DefiEndpoints(self)
        self.token = self.TokenEndpoints(self)
        self.wallet = self.WalletEndpoints(self)
        self.trader = self.TraderEndpoints(self)
        self.search = self.SearchEndpoints(self)
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout))
    )
    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, base_url: Optional[str] = None, timeout: int = 10) -> Dict[str, Any]:
        """Make a GET request to the BirdEye API with retry logic."""
        url = f"{base_url or self.BASE_URL}{endpoint}"
        self.logger.info(f"Making GET request to {url} with params {params}")
        start_time = time.time()
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=timeout)
            response.raise_for_status()
            result = response.json()
            
            elapsed = time.time() - start_time
            self.logger.info(f"GET request to {url} completed in {elapsed:.2f}s with status {response.status_code}")
            
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            self.logger.error(f"GET request to {url} failed after {elapsed:.2f}s: {str(e)}")
            raise
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout))
    )
    def _post(self, endpoint: str, data: Dict[str, Any], params: Optional[Dict[str, Any]] = None, base_url: Optional[str] = None) -> Dict[str, Any]:
        """Make a POST request to the BirdEye API with retry logic."""
        url = f"{base_url or self.BASE_URL}{endpoint}"
        headers = self.headers.copy()
        headers["content-type"] = "application/json"
        self.logger.info(f"Making POST request to {url} with params {params}")
        start_time = time.time()
        
        try:
            response = requests.post(url, headers=headers, json=data, params=params, timeout=10)
            response.raise_for_status()
            result = response.json()
            
            elapsed = time.time() - start_time
            self.logger.info(f"POST request to {url} completed in {elapsed:.2f}s with status {response.status_code}")
            
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            self.logger.error(f"POST request to {url} failed after {elapsed:.2f}s: {str(e)}")
            raise
    
    
    class DefiEndpoints:
        def __init__(self, client):
            self.client = client
            self.base_url = f"{client.BASE_URL}/defi"
        
        def get_price(self, address: str, check_liquidity: Optional[float] = None, include_liquidity: Optional[bool] = None) -> Dict[str, Any]:
            """Get token price."""
            endpoint = "/price"
            params = {"address": address}
            if check_liquidity is not None:
                params['check_liquidity'] = check_liquidity
            if include_liquidity is not None:
                params['include_liquidity'] = str(include_liquidity).lower()
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_multi_price(self, list_address: str, check_liquidity: Optional[float] = None, include_liquidity: Optional[bool] = None) -> Dict[str, Any]:
            """Get prices for multiple tokens."""
            endpoint = "/multi_price"
            data = {"list_address": list_address}
            params = {}
            if check_liquidity is not None:
                params['check_liquidity'] = check_liquidity
            if include_liquidity is not None:
                params['include_liquidity'] = str(include_liquidity).lower()
            response = self.client._post(endpoint, data, params, base_url=self.base_url)
            return response

        def get_historical_price(self, address: str, address_type: str = "token", type: str = "15m",
                               time_from: Optional[int] = 0, time_to: Optional[int] = 10000000000) -> Dict[str, Any]:
            """Get historical price data."""
            endpoint = "/history_price"
            params = {"address": address, "address_type": address_type, "type": type}
            if time_from is not None:
                params['time_from'] = time_from
            if time_to is not None:
                params['time_to'] = time_to
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_historical_price_unix(self, address: str, unixtime: Optional[int] = None) -> Dict[str, Any]:
            """Get historical price at specific unix timestamp."""
            endpoint = "/historical_price_unix"
            params = {"address": address}
            if unixtime is not None:
                params['unixtime'] = unixtime
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_trades_token(self, address: str, offset: int = 0, limit: int = 50,
                           tx_type: str = "swap", sort_type: str = "desc") -> Dict[str, Any]:
            """Get trades for a token."""
            endpoint = "/txs/token"
            params = {"address": address, "offset": offset, "limit": limit, "tx_type": tx_type, "sort_type": sort_type}
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_trades_pair(self, address: str, offset: int = 0, limit: int = 50,
                          tx_type: str = "swap", sort_type: str = "desc") -> Dict[str, Any]:
            """Get trades for a pair."""
            endpoint = "/txs/pair"
            params = {"address": address, "offset": offset, "limit": limit, "tx_type": tx_type, "sort_type": sort_type}
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_ohlcv(self, address: str, type: str = "15m",
                    time_from: Optional[int] = None, time_to: Optional[int] = None) -> Dict[str, Any]:
            """Get OHLCV data for a token."""
            endpoint = "/ohlcv"
            params = {"address": address, "type": type}
            if time_from is not None:
                params['time_from'] = time_from
            if time_to is not None:
                params['time_to'] = time_to
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_ohlcv_pair(self, address: str, type: str = "15m",
                         time_from: Optional[int] = None, time_to: Optional[int] = None) -> Dict[str, Any]:
            """Get OHLCV data for a pair."""
            endpoint = "/ohlcv/pair"
            params = {"address": address, "type": type}
            if time_from is not None:
                params['time_from'] = time_from
            if time_to is not None:
                params['time_to'] = time_to
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_ohlcv_base_quote(self, base_address: str, quote_address: str, type: str = "15m",
                               time_from: Optional[int] = None, time_to: Optional[int] = None) -> Dict[str, Any]:
            """Get OHLCV data for a base/quote pair."""
            endpoint = "/ohlcv/base_quote"
            params = {"base_address": base_address, "quote_address": quote_address, "type": type}
            if time_from is not None:
                params['time_from'] = time_from
            if time_to is not None:
                params['time_to'] = time_to
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_price_volume_multi(self, list_address: str, type: str = "24h") -> Dict[str, Any]:
            """Get price and volume for multiple tokens."""
            endpoint = "/price_volume/multi"
            data = {"list_address": list_address, "type": type}
            response = self.client._post(endpoint, data, base_url=self.base_url)
            return response
    
    class TokenEndpoints:
        def __init__(self, client):
            self.client = client
            self.base_url = f"{client.BASE_URL}/defi"
        
        def get_token_list_v3(self, sort_by: str = "market_cap", sort_type: str = "desc", 
                            offset: int = 0, limit: int = 100, min_liquidity: Optional[int] = None,
                            min_market_cap: Optional[int] = None, min_holder: Optional[int] = None,
                            min_price_change_4h_percent: Optional[float] = None, 
                            min_price_change_24h_percent: Optional[float] = None,
                            **additional_filters) -> Dict[str, Any]:
            """
            Fetch the token list using the V3 API endpoint with advanced filtering options.
            
            Args:
                sort_by (str): Field to sort by (market_cap, liquidity, etc.)
                sort_type (str): Sort direction (asc or desc)
                offset (int): Pagination offset (0-10000)
                limit (int): Results per page (1-100)
                min_liquidity (int, optional): Minimum token liquidity
                min_market_cap (int, optional): Minimum market capitalization
                min_holder (int, optional): Minimum number of holders
                min_price_change_4h_percent (float, optional): Minimum 4h price change percentage
                min_price_change_24h_percent (float, optional): Minimum 24h price change percentage
                **additional_filters: Any other filter parameters supported by the API
                
            Returns:
                Dict containing token data
            """
            endpoint = "/v3/token/list"
            
            # Build parameters dictionary with non-None values
            params = {
                "sort_by": sort_by,
                "sort_type": sort_type,
                "offset": offset,
                "limit": limit
            }
            
            # Add optional filters only if they have values
            if min_liquidity is not None:
                params["min_liquidity"] = min_liquidity
            if min_market_cap is not None:
                params["min_market_cap"] = min_market_cap
            if min_holder is not None:
                params["min_holder"] = min_holder
            if min_price_change_4h_percent is not None:
                params["min_price_change_4h_percent"] = min_price_change_4h_percent
            if min_price_change_24h_percent is not None:
                params["min_price_change_24h_percent"] = min_price_change_24h_percent
            
            # Add any additional filters
            params.update({k: v for k, v in additional_filters.items() if v is not None})
            
            # Make the API call
            return self.client._get(endpoint, params, base_url=self.client.BASE_URL + "/defi")
        
        def get_token_trending(self, sort_by: str = "rank", sort_type: str = "asc",
                             offset: int = 0, limit: int = 20) -> Dict[str, Any]:
            """Get trending tokens."""
            endpoint = "/token_trending"
            params = {"sort_by": sort_by, "sort_type": sort_type, "offset": offset, "limit": limit}
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response
        
        def get_token_overview(self, address: str) -> Dict[str, Any]:
            """Get token overview."""
            endpoint = "/token_overview"
            params = {"address": address}
            return self.client._get(endpoint, params, base_url=self.base_url)
        
        def get_token_security(self, address: str) -> Dict[str, Any]:
            """Get token security information."""
            endpoint = "/token_security"
            params = {"address": address}
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_creation_info(self, address: str) -> Dict[str, Any]:
            """Get token creation information."""
            endpoint = "/token_creation_info"
            params = {"address": address}
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_new_listing(self, time_to: Optional[int] = None, limit: int = 20,
                          meme_platform_enabled: bool = True) -> Dict[str, Any]:
            """Get newly listed tokens."""
            endpoint = "/v2/tokens/new_listing"
            params = {
                "limit": limit,
                "meme_platform_enabled": str(meme_platform_enabled).lower()
            }
            if time_to is not None:
                params['time_to'] = time_to
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_top_traders(self, address: str, time_frame: str = "24h", sort_type: str = "desc",
                          sort_by: str = "volume", offset: int = 0, limit: int = 10) -> Dict[str, Any]:
            """Get top traders for a token."""
            endpoint = "/v2/tokens/top_traders"
            params = {
                "address": address,
                "time_frame": time_frame,
                "sort_type": sort_type,
                "sort_by": sort_by,
                "offset": offset,
                "limit": limit
            }
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_metadata_multiple(self, list_address: str) -> Dict[str, Any]:
            """Get metadata for multiple tokens."""
            endpoint = "/v3/token/meta-data/multiple"
            params = {"list_address": list_address}
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_metadata(self, address: str) -> Dict[str, Any]:
            """Get token metadata."""
            endpoint = "/v3/token/meta-data/single"
            params = {"address": address}
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response
        
        def get_token_market_data(self, address: str) -> Dict[str, Any]:
            """Get token market data."""
            endpoint = "/v3/token/market-data"
            params = {"address": address}
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_trade_data_multiple(self, list_address: str) -> Dict[str, Any]:
            """Get trade data for multiple tokens."""
            endpoint = "/v3/token/trade-data/multiple"
            params = {"list_address": list_address}
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_trade_data(self, address: str) -> Dict[str, Any]:
            """Get token trade data."""
            endpoint = "/v3/token/trade-data/single"
            params = {"address": address}
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_top_holders(self, address: str, offset: int = 0, limit: int = 100) -> Dict[str, Any]:
            """Fetch top holders for a given token address."""
            endpoint = "/v3/token/holder"
            params = {
                "address": address,
                "offset": offset,
                "limit": limit
            }
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response
    
    class WalletEndpoints:
        def __init__(self, client):
            self.client = client
            self.base_url = client.BASE_URL
        
        def get_token_list(self, wallet: str) -> Dict[str, Any]:
            """Get token list for a wallet."""
            endpoint = "/v1/wallet/token_list"
            params = {"wallet": wallet}
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_balance(self, address: str, token_address: str) -> Dict[str, Any]:
            """Get token balance for a wallet."""
            endpoint = "/v1/wallet/token_balance"
            params = {"wallet": address, "token_address": token_address}
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response

        def get_transaction_history(self, wallet: str, limit: int = 100, before: Optional[str] = None) -> Dict[str, Any]:
            """Get transaction history for a wallet."""
            endpoint = "/v1/wallet/tx_list"
            params = {"wallet": wallet, "limit": limit}
            if before is not None:
                params['before'] = before
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response
    
    class TraderEndpoints:
        def __init__(self, client):
            self.client = client
            self.base_url = client.BASE_URL
        
        def get_gainers_losers(
            self,
            type: str = "1W",
            sort_by: str = "PnL",
            sort_type: str = "desc",
            offset: int = 0,
            limit: int = 10
        ) -> Dict[str, Any]:
            """
            Fetch top gainers or losers.
            """
            endpoint = "/trader/gainers-losers"
            params = {
                "type": type,
                "sort_by": sort_by,
                "sort_type": sort_type,
                "offset": offset,
                "limit": limit
            }

            # Validate parameters
            if not (0 <= offset <= 10000):
                raise ValueError("Offset must be between 0 and 10000.")
            if not (1 <= limit <= 10):
                raise ValueError("Limit must be between 1 and 10.")
            if sort_type not in ["asc", "desc"]:
                raise ValueError("sort_type must be 'asc' or 'desc'.")
            if sort_by not in ["PnL", "Volume", "Trade_Count"]:
                raise ValueError("Invalid sort_by value.")

            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response
        
        def get_trades_seek_by_time(
            self,
            address: str,
            offset: int = 0,
            limit: int = 100,
            before_time: int = 0,
            after_time: int = 0
        ) -> Dict[str, Any]:
            """
            Fetches a list of trades for a trader with optional time bounds.
            """
            endpoint = "/trader/txs/seek_by_time"
            params = {
                "address": address,
                "offset": offset,
                "limit": limit,
                "before_time": before_time,
                "after_time": after_time
            }
            response = self.client._get(endpoint, params, base_url=self.base_url)
            return response
    
    class SearchEndpoints:
        def __init__(self, client):
            self.client = client
            self.base_url = f"{client.BASE_URL}/defi/v3"
        
        def search_tokens(
            self,
            chain: str = "all",
            keyword: str = "AI",
            target: str = "token",
            sort_by: str = "volume_24h_usd",
            sort_type: str = "desc",
            verify_token: Optional[bool] = None,
            markets: Optional[str] = None,
            offset: int = 0,
            limit: int = 20
        ) -> Dict[str, Any]:
            """Search for tokens."""
            endpoint = "/search"
            params = {
                "chain": chain,
                "keyword": keyword,
                "target": target,
                "sort_by": sort_by,
                "sort_type": sort_type,
                "offset": offset,
                "limit": limit
            }
            if verify_token is not None:
                params["verify_token"] = verify_token
            if markets is not None:
                params["markets"] = markets
            return self.client._get(endpoint, params=params, base_url=self.base_url)

class HeliusClient:
    """Client for Helius API."""
    
    def __init__(self, api_key):
        """Initialize Helius client."""
        self.api_key = api_key
        self.base_url = "https://api.helius.xyz"
        self.logger = logging.getLogger(__name__)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout))
    )
    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a GET request to the Helius API."""
        url = f"{self.base_url}{endpoint}"
        # Add API key to params
        params = params or {}
        params["api-key"] = self.api_key
        
        self.logger.info(f"Making GET request to Helius API: {url}")
        start_time = time.time()
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            result = response.json()
            
            elapsed = time.time() - start_time
            self.logger.info(f"Helius API request completed in {elapsed:.2f}s")
            
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            self.logger.error(f"Helius API request failed after {elapsed:.2f}s: {str(e)}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout))
    )
    def _post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make a POST request to the Helius API."""
        url = f"{self.base_url}{endpoint}"
        # Add API key to data
        data = data or {}
        data["apiKey"] = self.api_key
        
        self.logger.info(f"Making POST request to Helius API: {url}")
        start_time = time.time()
        
        try:
            response = requests.post(url, json=data, timeout=10)
            response.raise_for_status()
            result = response.json()
            
            elapsed = time.time() - start_time
            self.logger.info(f"Helius API request completed in {elapsed:.2f}s")
            
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            self.logger.error(f"Helius API request failed after {elapsed:.2f}s: {str(e)}")
            raise
    
    def get_webhook_status(self, webhook_url):
        """Get webhook status."""
        endpoint = "/v0/webhooks"
        response = self._get(endpoint)
        
        # Find the webhook with the matching URL
        for webhook in response:
            if webhook.get("webhookURL") == webhook_url:
                return webhook
        
        # If no matching webhook found
        return {"status": "not_found"}
    
    def update_webhook(self, webhook_url, addresses):
        """Update webhook with addresses."""
        endpoint = "/v0/webhooks"
        
        # First, get existing webhooks
        webhooks = self._get(endpoint)
        webhook_id = None
        
        # Find the webhook with the matching URL
        for webhook in webhooks:
            if webhook.get("webhookURL") == webhook_url:
                webhook_id = webhook.get("webhookID")
                break
        
        if webhook_id:
            # Update existing webhook
            update_endpoint = f"/v0/webhooks/{webhook_id}"
            data = {
                "accountAddresses": addresses,
                "webhookURL": webhook_url,
                "transactionTypes": ["SWAP"],  # You might want to make this configurable
                "webhookType": "raw"
            }
            return self._post(update_endpoint, data)
        else:
            # Create new webhook
            data = {
                "accountAddresses": addresses,
                "webhookURL": webhook_url,
                "transactionTypes": ["SWAP"],
                "webhookType": "raw"
            }
            return self._post(endpoint, data)

@resource(
    config_schema={
        "api_key": Field(
            StringSource,
            description="API key for BirdEye.",
            is_required=False,
        ),
    }
)
def birdeye_api_resource(context):
    """Resource for BirdEye API."""
    # Get API key from config or environment variables
    api_key = context.resource_config.get("api_key") or os.getenv("BIRDSEYE_API_KEY")
    
    if not api_key:
        raise ValueError("BirdEye API key not provided")
    
    return BirdEyeClient(api_key)

