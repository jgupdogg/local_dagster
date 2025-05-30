# ========== UPDATED SCRAPER (unified_scraper.py) ==========

import asyncio
import os
import logging
import time
import json
import re
import shutil
import tempfile
from typing import List, Dict, Union, Optional, Any, Tuple
from datetime import datetime
from urllib.parse import urlparse, quote

import pandas as pd
import nodriver as uc
from nodriver import Config as NodriverConfig
from nodriver import cdp
network = cdp.network

from dagster import ConfigurableResource, InitResourceContext, Config, resource
from pydantic import PrivateAttr

logger = logging.getLogger(__name__)


class SimpleNetworkCapture:
    """Enhanced network capture class with better API response detection."""
    
    def __init__(self, tab, url_pattern):
        self.tab = tab
        self.url_pattern = url_pattern
        self.requests = []
        self.responses = []
        self.captured_bodies = {}
        
    async def start_monitoring(self):
        await self.tab.send(network.enable())
        self.tab.add_handler(network.RequestWillBeSent, self._on_request)
        self.tab.add_handler(network.ResponseReceived, self._on_response)
        self.tab.add_handler(network.LoadingFinished, self._on_loading_finished)
    
    async def stop_monitoring(self):
        try:
            await self.tab.send(network.disable())
        except Exception as e:
            logger.warning(f"Error disabling network monitoring: {e}")
    
    def _on_request(self, event):
        request = event.request
        # Check if URL matches pattern (both regex and simple string matching)
        matches = False
        try:
            if re.search(self.url_pattern, request.url):
                matches = True
        except:
            # If regex fails, try simple string matching
            if self.url_pattern in request.url:
                matches = True
        
        if matches:
            logger.info(f"Captured matching request: {request.url}")
            self.requests.append({
                'requestId': event.request_id,
                'url': request.url,
                'method': request.method,
                'headers': request.headers
            })
    
    def _on_response(self, event):
        request_match = next((req for req in self.requests if req['requestId'] == event.request_id), None)
        if request_match:
            logger.info(f"Captured matching response: {event.response.url} (status: {event.response.status})")
            self.responses.append({
                'requestId': event.request_id,
                'url': event.response.url,
                'status': event.response.status,
                'headers': event.response.headers,
                'mimeType': event.response.mime_type
            })
    
    def _on_loading_finished(self, event):
        if any(resp['requestId'] == event.request_id for resp in self.responses):
            logger.debug(f"Loading finished for captured request: {event.request_id}")
    
    async def wait_for_response(self, timeout=30):
        start_time = time.time()
        attempts = 0
        max_attempts = timeout * 2  # Check every 0.5 seconds
        
        while attempts < max_attempts:
            # Check all responses, not just the latest
            for response in self.responses:
                request_id = response['requestId']
                
                if request_id not in self.captured_bodies:
                    try:
                        body_data = await self.tab.send(
                            network.get_response_body(request_id=request_id)
                        )
                        
                        if isinstance(body_data, tuple):
                            body_content = body_data[0]
                        elif hasattr(body_data, 'body'):
                            body_content = body_data.body
                        else:
                            body_content = str(body_data)
                        
                        self.captured_bodies[request_id] = body_content
                        response['body'] = body_content
                        logger.info(f"Successfully captured response body for {response['url']}")
                        return response
                        
                    except Exception as e:
                        logger.debug(f"Error getting response body for {request_id}: {e}")
                        # Continue to next response
                        continue
                else:
                    # Body already captured
                    response['body'] = self.captured_bodies[request_id]
                    return response
            
            await asyncio.sleep(0.5)
            attempts += 1
        
        logger.warning(f"Timeout waiting for API response matching pattern: {self.url_pattern}")
        return None


class DisplaySizeConfig(Config):
    width: int = 1920
    height: int = 1080


class UnifiedScraperClient:
    """
    Client class that holds the state and performs scraping operations.
    """
    def __init__(self, config: 'UnifiedScraperResource'):
        self.config = config
        self.browser: Optional[uc.Browser] = None
        self.main_tab: Optional[uc.Tab] = None
        self.event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._initialized: bool = False
        self.browser_id: str = f"browser_{id(self)}"
        self._user_data_dir: Optional[str] = None
        self._is_temp_dir: bool = False
        
        # Initialize cookies_file based on config logic
        if self.config.cookies_file is None and self.config.profile_name:
            self._cookies_file_internal = None
        elif self.config.cookies_file is None:
            self._cookies_file_internal = '.session.dat'
        else:
            self._cookies_file_internal = self.config.cookies_file

    async def setup(self):
        logger.info(f"Setting up UnifiedScraperClient {self.browser_id}")
        
        try:
            await self._setup_browser_with_retry()
            self._initialized = True
            logger.info(f"UnifiedScraperClient {self.browser_id} setup completed successfully")
        except Exception as e:
            logger.error(f"Failed to setup browser: {e}")
            await self._cleanup_on_error()
            raise

    async def _setup_browser_with_retry(self, max_retries=3):
        """Setup browser with retry logic for connection issues"""
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Browser setup attempt {attempt + 1}/{max_retries}")
                await self._async_setup_browser()
                await self._test_browser_connection()
                logger.info(f"Browser setup successful on attempt {attempt + 1}")
                return
                
            except Exception as e:
                last_exception = e
                logger.warning(f"Browser setup attempt {attempt + 1} failed: {e}")
                await self._cleanup_browser()
                
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    logger.info(f"Waiting {wait_time} seconds before retry...")
                    await asyncio.sleep(wait_time)
        
        raise RuntimeError(f"Failed to setup browser after {max_retries} attempts. Last error: {last_exception}")

    async def _test_browser_connection(self):
        """Test that browser connection is working properly"""
        if not self.browser or not self.main_tab:
            raise RuntimeError("Browser or main tab not available")
        
        try:
            await asyncio.wait_for(self.main_tab.get('about:blank'), timeout=10)
            content = await asyncio.wait_for(self.main_tab.get_content(), timeout=5)
            if not content:
                raise RuntimeError("Could not retrieve page content")
            logger.info("Browser connection test passed")
            
        except asyncio.TimeoutError:
            raise RuntimeError("Browser connection test timed out")
        except Exception as e:
            raise RuntimeError(f"Browser connection test failed: {e}")

    async def teardown(self):
        logger.info(f"Tearing down UnifiedScraperClient {self.browser_id}")
        
        if self._initialized:
            try:
                await self._async_teardown_browser()
            except Exception as e:
                logger.error(f"Error during async teardown browser: {e}")
        
        self._initialized = False

    async def _cleanup_on_error(self):
        """Cleanup resources when setup fails"""
        await self._cleanup_browser()

    async def _cleanup_browser(self):
        """Clean up browser resources"""
        if self.browser:
            try:
                await self.browser.stop()
            except Exception as e:
                logger.warning(f"Error stopping browser during cleanup: {e}")
            finally:
                self.browser = None
                self.main_tab = None

    def _get_comprehensive_browser_args(self):
        """Get comprehensive browser arguments for stability"""
        args = [
            f'--user-data-dir={self._user_data_dir}',
            f'--window-size={self.config.display_size.width},{self.config.display_size.height}',
            '--no-first-run',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--disable-features=TranslateUI',
            '--disable-ipc-flooding-protection',
            '--disable-background-networking',
            '--disable-sync',
            '--disable-extensions',
            '--disable-default-apps',
            '--disable-plugins',
            '--disable-hang-monitor',
            '--disable-prompt-on-repost',
            '--disable-web-security',
            '--memory-pressure-off',
            '--max_old_space_size=4096',
            '--disable-dev-shm-usage',
            '--disable-gpu-sandbox',
            '--disable-software-rasterizer',
            '--mute-audio',
            '--disable-audio-output',
            '--ignore-certificate-errors',
            '--ignore-ssl-errors',
            '--ignore-certificate-errors-spki-list',
            '--ignore-ssl-errors-on-localhost',
        ]
        
        if self.config.no_sandbox:
            args.extend(['--no-sandbox', '--disable-setuid-sandbox'])
        
        if self.config.headless:
            args.extend(['--headless=new', '--disable-gpu', '--hide-scrollbars'])
        
        return args

    async def _async_setup_browser(self):
        # Setup user data directory
        if self.config.profile_name and self.config.user_data_dir:
            profile_dir = os.path.join(self.config.user_data_dir, self.config.profile_name)
            os.makedirs(profile_dir, exist_ok=True)
            user_data_dir = profile_dir
        else:
            timestamp = int(time.time())
            user_data_dir = self.config.user_data_dir or os.path.join(
                tempfile.gettempdir(), f"nodriver_{timestamp}_{os.getpid()}"
            )
            os.makedirs(user_data_dir, exist_ok=True)
        
        self._user_data_dir = user_data_dir
        self._is_temp_dir = not (self.config.profile_name or self.config.user_data_dir)

        if self.config.cookies_file is None and self.config.profile_name:
            self._cookies_file_internal = os.path.join(self._user_data_dir, '.session.dat')
        
        browser_args = self._get_comprehensive_browser_args()
        
        nodriver_config = NodriverConfig(
            headless=self.config.headless,
            user_data_dir=self._user_data_dir,
            browser_args=browser_args,
            lang='en-US',
        )
        
        nodriver_config.connection_timeout = max(self.config.timeout, 60)
        
        logger.info(f"Starting browser: user_data_dir={self._user_data_dir}, headless={self.config.headless}")
        
        try:
            self.browser = await asyncio.wait_for(
                uc.start(config=nodriver_config),
                timeout=self.config.timeout + 30
            )
            
            await asyncio.sleep(2)
            
            self.main_tab = await asyncio.wait_for(
                self.browser.get('about:blank'),
                timeout=15
            )
            
            logger.info(f"Browser started successfully (profile: {self._user_data_dir})")
            
        except asyncio.TimeoutError:
            raise RuntimeError(f"Browser startup timed out after {self.config.timeout + 30} seconds")
        except Exception as e:
            logger.error(f"Browser start failed: {str(e)}")
            raise RuntimeError(f"Browser initialization failed: {e}")

    async def _async_teardown_browser(self):
        if self.browser:
            try:
                if self.main_tab:
                    try:
                        await asyncio.wait_for(self.main_tab.close(), timeout=5)
                    except:
                        pass
                
                await asyncio.wait_for(self.browser.stop(), timeout=10)
                logger.info("Browser stopped successfully")
                
            except asyncio.TimeoutError:
                logger.warning("Browser stop operation timed out")
            except Exception as e:
                logger.warning(f"Error stopping browser: {e}")
            finally:
                self.browser = None
                self.main_tab = None
        
        if self._is_temp_dir and self._user_data_dir:
            if os.path.exists(self._user_data_dir) and os.path.basename(self._user_data_dir).startswith("nodriver_"):
                try:
                    shutil.rmtree(self._user_data_dir, ignore_errors=True)
                    logger.info(f"Removed temporary user data directory: {self._user_data_dir}")
                except Exception as e:
                    logger.warning(f"Error removing user data directory: {e}")
    
    def scrape_api_data(
        self, 
        url: str, 
        url_pattern: str, 
        timeout: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Scrape API data with improved pattern matching and timing.
        Returns tuple of (response_data, dataframe) like the original implementation.
        """
        if not self._initialized:
            logger.error("Scraper client not initialized.")
            return None
        
        if timeout is None:
            timeout = self.config.timeout
        
        try:
            return asyncio.run_coroutine_threadsafe(
                self._async_scrape_api_data(url, url_pattern, timeout),
                self.event_loop
            ).result(timeout=timeout + 15)
            
        except Exception as e:
            logger.error(f"Error in scrape_api_data: {e}")
            return None
    
    async def _async_scrape_api_data(
        self, 
        url: str, 
        url_pattern: str, 
        timeout: int
    ) -> Optional[Dict[str, Any]]:
        net_capture = None
        tab = None
        
        try:
            # Create new tab for this operation
            tab = await asyncio.wait_for(self.browser.get('about:blank'), timeout=10)
            logger.info(f"Created new tab for scraping: {url}")
            
            # Setup network capture
            net_capture = SimpleNetworkCapture(tab, url_pattern)
            await net_capture.start_monitoring()
            logger.info(f"Started network monitoring for pattern: {url_pattern}")
            
            # Navigate to URL
            logger.info(f"Navigating to: {url}")
            await asyncio.wait_for(tab.get(url), timeout=timeout)
            
            # Wait for page to load completely
            logger.info("Waiting for page to load...")
            await asyncio.sleep(3)
            
            # Try to wait for specific elements that might trigger API calls
            try:
                # Wait for body and any dynamic content
                await asyncio.wait_for(tab.wait_for('body'), timeout=10)
                logger.info("Page body loaded, waiting for API calls...")
                
                # Additional wait for dynamic content and API calls
                await asyncio.sleep(8)
                
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for page elements, continuing...")
            
            # Wait for API response
            logger.info(f"Waiting for API response matching pattern: {url_pattern}")
            response_metadata = await net_capture.wait_for_response(timeout=timeout)
            
            if not response_metadata:
                logger.error(f"No API response captured for pattern: {url_pattern}")
                logger.info(f"Captured {len(net_capture.requests)} requests and {len(net_capture.responses)} responses")
                
                # Log all captured URLs for debugging
                for i, req in enumerate(net_capture.requests):
                    logger.info(f"Request {i+1}: {req['method']} {req['url']}")
                
                # Also log any responses even if they don't have bodies
                for i, resp in enumerate(net_capture.responses):
                    logger.info(f"Response {i+1}: {resp['status']} {resp['url']}")
                
                return None
            
            logger.info(f"Successfully captured API response: {response_metadata['url']}")
            return response_metadata
                
        except Exception as e:
            logger.error(f"Scraping error: {e}")
            return None
            
        finally:
            if net_capture:
                await net_capture.stop_monitoring()
            if tab and tab != self.main_tab:
                try:
                    await asyncio.wait_for(tab.close(), timeout=5)
                except Exception as e:
                    logger.warning(f"Error closing temporary tab: {e}")
                    
    def scrape_api_data_multiple(
        self,
        url: str,
        url_pattern: str,
        timeout: Optional[int] = None,
        capture_count: int = 1,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Capture up to `capture_count` network responses matching `url_pattern`.
        Returns a list of response metadata dicts, each containing:
        { 'requestId', 'url', 'status', 'headers', 'body' }
        """
        if not self._initialized:
            logger.error("Scraper client not initialized.")
            return None
        if timeout is None:
            timeout = self.config.timeout

        try:
            return asyncio.run_coroutine_threadsafe(
                self._async_scrape_api_data_multiple(url, url_pattern, timeout, capture_count),
                self.event_loop
            ).result(timeout=timeout + 15)
        except Exception as e:
            logger.error(f"Error in scrape_api_data_multiple: {e}")
            return None

    async def _async_scrape_api_data_multiple(
        self,
        url: str,
        url_pattern: str,
        timeout: int,
        capture_count: int
    ) -> Optional[List[Dict[str, Any]]]:
        net_capture = None
        tab = None
        try:
            tab = await asyncio.wait_for(self.browser.get('about:blank'), timeout=10)
            net_capture = SimpleNetworkCapture(tab, url_pattern)
            await net_capture.start_monitoring()

            # Navigate and allow JS to fire
            await asyncio.wait_for(tab.get(url), timeout=timeout)
            await asyncio.sleep(3)
            try:
                await asyncio.wait_for(tab.wait_for('body'), timeout=10)
                await asyncio.sleep(5)
            except asyncio.TimeoutError:
                pass

            collected = []
            start_ts = time.time()
            while len(collected) < capture_count and (time.time() - start_ts) < timeout:
                for resp in list(net_capture.responses):
                    rid = resp['requestId']
                    if rid not in net_capture.captured_bodies:
                        try:
                            body_data = await tab.send(network.get_response_body(request_id=rid))
                            # Extract body
                            if hasattr(body_data, 'body'):
                                body = body_data.body
                            else:
                                body = body_data[0] if isinstance(body_data, tuple) else str(body_data)
                            resp['body'] = body
                            net_capture.captured_bodies[rid] = body
                        except Exception:
                            continue
                    if 'body' in resp and resp not in collected:
                        collected.append(resp)
                        if len(collected) >= capture_count:
                            break
                await asyncio.sleep(0.5)
            return collected

        except Exception as e:
            logger.error(f"Error scraping multiple responses: {e}")
            return None

        finally:
            if net_capture:
                await net_capture.stop_monitoring()
            if tab and tab != self.main_tab:
                try:
                    await tab.close()
                except Exception:
                    pass


class UnifiedScraperResource(ConfigurableResource):
    """
    Unified Dagster resource for web scraping using nodriver.
    """
    
    headless: bool = True  # Changed to True to match working implementation
    visible: bool = True
    profile_name: Optional[str] = None
    user_data_dir: Optional[str] = None
    cookies_file: Optional[str] = None
    no_sandbox: bool = True
    timeout: int = 60
    display_size: DisplaySizeConfig = DisplaySizeConfig(width=1920, height=1080)
    
    _client: Optional[UnifiedScraperClient] = PrivateAttr(default=None)
    _event_loop: Optional[asyncio.AbstractEventLoop] = PrivateAttr(default=None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        logger.info(f"Setting up UnifiedScraperResource")
        
        try:
            self._setup_event_loop()
            self._client = UnifiedScraperClient(self)
            self._client.event_loop = self._event_loop
            
            future = asyncio.run_coroutine_threadsafe(
                self._client.setup(),
                self._event_loop
            )
            future.result(timeout=self.timeout + 30)
            
            logger.info("UnifiedScraperResource setup completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup UnifiedScraperResource: {e}")
            self._cleanup_on_error()
            raise

    def _setup_event_loop(self):
        """Setup event loop for the resource"""
        try:
            self._event_loop = asyncio.get_event_loop()
            if self._event_loop.is_closed():
                raise RuntimeError("Current event loop is closed")
        except RuntimeError:
            self._event_loop = asyncio.new_event_loop()
            
        if not self._event_loop.is_running():
            import threading
            
            def run_loop():
                asyncio.set_event_loop(self._event_loop)
                self._event_loop.run_forever()
            
            self._loop_thread = threading.Thread(target=run_loop, daemon=True)
            self._loop_thread.start()
            time.sleep(0.1)

    def _cleanup_on_error(self):
        """Cleanup resources when setup fails"""
        if self._client:
            try:
                if self._event_loop and not self._event_loop.is_closed():
                    future = asyncio.run_coroutine_threadsafe(
                        self._client.teardown(),
                        self._event_loop
                    )
                    try:
                        future.result(timeout=10)
                    except:
                        pass
            except:
                pass
            finally:
                self._client = None

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        logger.info("Tearing down UnifiedScraperResource")
        
        if self._client and self._event_loop and not self._event_loop.is_closed():
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._client.teardown(),
                    self._event_loop
                )
                future.result(timeout=15)
            except Exception as e:
                logger.error(f"Error during client teardown: {e}")
            finally:
                self._client = None
        
        if self._event_loop and hasattr(self, '_loop_thread'):
            try:
                self._event_loop.call_soon_threadsafe(self._event_loop.stop)
                self._loop_thread.join(timeout=5)
            except Exception as e:
                logger.warning(f"Error stopping event loop: {e}")
            finally:
                self._event_loop = None
    
    @property
    def client(self) -> UnifiedScraperClient:
        if not self._client:
            raise Exception("UnifiedScraperClient not initialized. Resource may not be set up.")
        return self._client


# Create a pre-configured instance for common use
unified_scraper = UnifiedScraperResource(
    headless=True,  # Changed to match working implementation
    visible=True,
    no_sandbox=True,
    timeout=60,
    display_size={"width": 1920, "height": 1080}
)
