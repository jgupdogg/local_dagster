"""
EMMA Public Contracts scraping asset
"""
from dagster import asset, AssetExecutionContext, MaterializeResult
import json
import time
from datetime import datetime
from pathlib import Path
import logging
import asyncio

from common.resources.unified_scraper import UnifiedScraperResource
from common.resources.database import DatabaseResource
from engineering_pipeline.models.engineering_models import EmmaPublicContracts

logger = logging.getLogger(__name__)


@asset(
    required_resource_keys={"unified_scraper", "db"},
    group_name="engineering_data",
    description="Scrapes EMMA Maryland public contracts data"
)
def emma_public_contracts(context: AssetExecutionContext) -> MaterializeResult:
    """Scrape EMMA public contracts page and save raw HTML to database."""
    scraper: UnifiedScraperResource = context.resources.unified_scraper
    db: DatabaseResource = context.resources.db
    
    base_url = "https://emma.maryland.gov/page.aspx/en/ctr/contract_browse_public"
    
    try:
        context.log.info("Starting EMMA public contracts scrape...")
        context.log.info(f"Target URL: {base_url}")
        
        # Simply navigate to the page and get the content
        client = scraper.client
        if not client._initialized:
            raise ValueError("Scraper client not initialized")
        
        # Navigate to the page and get the HTML content
        async def get_page_content():
            """Navigate to page and get HTML content"""
            import asyncio
            
            # Get a tab
            tab = await asyncio.wait_for(client.browser.get('about:blank'), timeout=10)
            
            try:
                # Navigate to the URL
                context.log.info(f"Navigating to {base_url}")
                await asyncio.wait_for(tab.get(base_url), timeout=30)
                
                # Wait for page to load
                context.log.info("Waiting for page to load...")
                await asyncio.sleep(5)
                
                # Get the page content
                page_content = await tab.get_content()
                
                return {
                    'url': base_url,
                    'status': 200,
                    'body': page_content,
                    'method': 'page_content'
                }
                
            finally:
                if tab and tab != client.main_tab:
                    try:
                        await asyncio.wait_for(tab.close(), timeout=5)
                    except Exception as e:
                        context.log.warning(f"Error closing tab: {e}")
        
        # Run the async function
        response_data = asyncio.run_coroutine_threadsafe(
            get_page_content(),
            client.event_loop
        ).result(timeout=60)
        
        if not response_data:
            raise ValueError("Failed to get page content")
        
        raw_body = response_data.get('body', '')
        
        context.log.info(f"Successfully retrieved page content")
        context.log.info(f"Response body length: {len(raw_body)} characters")
        
        # Create EmmaPublicContracts record for database storage
        scrape_time = datetime.now()
        emma_record = EmmaPublicContracts(
            timestamp=scrape_time,
            value={
                "raw_html": raw_body, 
                "url": base_url, 
                "status": response_data.get('status', 200),
                "body_length": len(raw_body)
            }
        )
        
        # Store in PostgreSQL database
        context.log.info("Storing raw HTML data in PostgreSQL database...")
        
        # Convert to dict and use bulk_insert_ignore_conflicts instead for simpler insert
        record_dict = emma_record.dict(exclude={'id'})
        
        with db.session_scope() as session:
            session.add(emma_record)
            records_saved = 1
        
        context.log.info(f"Saved {records_saved} record(s) to engineering.bronze.emma_public_contracts table")
        
        # Log basic info about the content
        if isinstance(raw_body, str):
            if '<table' in raw_body.lower():
                context.log.info("Page contains HTML table data")
                table_count = raw_body.lower().count('<table')
                row_count = raw_body.lower().count('<tr')
                context.log.info(f"Found {table_count} tables and {row_count} table rows")
            
            if 'contract' in raw_body.lower():
                context.log.info("Page contains contract data")
        
        return MaterializeResult(
            metadata={
                "records_saved": records_saved,
                "database_table": "engineering.bronze.emma_public_contracts",
                "response_status": response_data.get('status', 'unknown'),
                "response_url": response_data.get('url', base_url),
                "body_length": len(raw_body),
                "scrape_timestamp": scrape_time.isoformat(),
                "contains_table_data": '<table' in raw_body.lower() if isinstance(raw_body, str) else False
            }
        )
        
    except Exception as e:
        logger.error(f"Error scraping EMMA data: {e}")
        raise