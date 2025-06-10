"""
EMMA Contracts Gold transformation asset
Scrapes detailed contract pages and extracts comprehensive data
"""
from dagster import asset, AssetExecutionContext, MaterializeResult
from datetime import datetime
from bs4 import BeautifulSoup
import logging
import asyncio
import re

from common.resources.unified_scraper import UnifiedScraperResource
from common.resources.database import DatabaseResource
from engineering_pipeline.models.engineering_models import EmmaContractsGold, EmmaPublicContractsSilver
from sqlalchemy import and_, func
from datetime import timedelta

logger = logging.getLogger(__name__)


@asset(
    required_resource_keys={"unified_scraper", "db"},
    group_name="engineering_data",
    description="Scrape detailed EMMA contract pages and extract comprehensive data to Gold layer",
    deps=["emma_contracts_silver"]  # Ensure silver layer is processed first
)
def emma_contracts_gold(context: AssetExecutionContext) -> MaterializeResult:
    """Scrape contract detail pages and extract comprehensive data from unprocessed silver records."""
    scraper: UnifiedScraperResource = context.resources.unified_scraper
    db: DatabaseResource = context.resources.db
    
    try:
        context.log.info("Starting Contracts Gold layer processing from silver table...")
        
        # Get unprocessed contracts from silver table using processed flags (matching solicitations pattern)
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        
        with db.session_scope() as session:
            # Find silver records that need processing
            unprocessed_contracts = session.query(EmmaPublicContractsSilver).filter(
                and_(
                    EmmaPublicContractsSilver.detail_url.isnot(None),
                    EmmaPublicContractsSilver.detail_url != '',
                    # Either never processed or not processed in last 24 hours
                    (
                        (EmmaPublicContractsSilver.processed == False) |
                        (EmmaPublicContractsSilver.updated_at < cutoff_time)
                    )
                )
            ).order_by(EmmaPublicContractsSilver.created_at.desc()).limit(20).all()  # Increased limit for better throughput
            
            context.log.info(f"Found {len(unprocessed_contracts)} silver contracts to process")
            
            if not unprocessed_contracts:
                return MaterializeResult(metadata={
                    "records_processed": 0,
                    "message": "No unprocessed silver contracts found"
                })
            
            # Initialize scraper
            client = scraper.client
            if not client._initialized:
                raise ValueError("Scraper client not initialized")
            
            records_processed = 0
            records_created = 0
            records_updated = 0
            records_skipped = 0
            errors = 0
            
            # Process each silver record within the same session
            for silver_record in unprocessed_contracts:
                try:
                    # Access record attributes while session is active
                    contract_code = silver_record.contract_code
                    detail_url = silver_record.detail_url
                    silver_id = silver_record.id
                    
                    context.log.info(f"Processing contract {contract_code} - {detail_url}")
                    
                    # Check if gold record already exists and was recently processed
                    existing_gold = session.query(EmmaContractsGold).filter(
                        EmmaContractsGold.contract_id == contract_code
                    ).first()
                    
                    if existing_gold and existing_gold.processed_at and existing_gold.processed_at > cutoff_time:
                        context.log.info(f"Skipping {contract_code} - processed within 24 hours")
                        records_skipped += 1
                        continue
                    
                    # Scrape the contract detail page
                    html_content = asyncio.run_coroutine_threadsafe(
                        scrape_single_contract(client, detail_url, context),
                        client.event_loop
                    ).result(timeout=90)
                    
                    if not html_content:
                        context.log.warning(f"Failed to get content for {contract_code}")
                        errors += 1
                        continue
                    
                    # Parse and extract data
                    soup = BeautifulSoup(html_content, 'html.parser')
                    extracted_data = extract_contract_data(soup, context)
                    
                    # Add metadata
                    extracted_data.update({
                        'contract_id': contract_code,
                        'detail_url': detail_url,
                        'source_silver_id': silver_id
                    })
                    
                    # Store/update in Gold table using the same session
                    existing_record = session.query(EmmaContractsGold).filter(
                        EmmaContractsGold.contract_id == contract_code
                    ).first()
                    
                    if existing_record:
                        # Update existing record
                        for key, value in extracted_data.items():
                            if hasattr(existing_record, key):
                                setattr(existing_record, key, value)
                        existing_record.updated_at = datetime.utcnow()
                        existing_record.processed_at = datetime.utcnow()
                        context.log.info(f"Updated Gold record for {contract_code}")
                        records_updated += 1
                    else:
                        # Create new record
                        gold_record = EmmaContractsGold(
                            **extracted_data,
                            processed_at=datetime.utcnow(),
                            created_at=datetime.utcnow(),
                            updated_at=datetime.utcnow()
                        )
                        session.add(gold_record)
                        context.log.info(f"Created new Gold record for {contract_code}")
                        records_created += 1
                    
                    # Mark silver record as processed
                    silver_record.processed = True
                    silver_record.updated_at = datetime.utcnow()
                    
                    records_processed += 1
                    
                    # Add delay between requests to be respectful
                    import time
                    time.sleep(2)
                    
                except Exception as e:
                    context.log.error(f"Error processing {contract_code}: {e}")
                    errors += 1
                    continue
        
        context.log.info(f"Contracts Gold processing complete: {records_processed} processed, {records_created} created, {records_updated} updated, {records_skipped} skipped, {errors} errors")
        
        return MaterializeResult(
            metadata={
                "records_processed": records_processed,
                "records_created": records_created,
                "records_updated": records_updated,
                "records_skipped": records_skipped,
                "errors": errors,
                "cutoff_time": cutoff_time.isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"Error processing Contracts Gold layer: {e}")
        raise


async def scrape_single_contract(client, url: str, context: AssetExecutionContext) -> str:
    """Scrape a single contract detail page and return HTML content"""
    
    # Get a tab
    tab = await asyncio.wait_for(client.browser.get('about:blank'), timeout=10)
    
    try:
        # Navigate to the URL
        context.log.info(f"Navigating to {url}")
        await asyncio.wait_for(tab.get(url), timeout=30)
        
        # Wait for page to load
        context.log.info("Waiting for page to load...")
        await asyncio.sleep(8)
        
        # Get the page content
        page_content = await tab.get_content()
        
        context.log.info(f"Successfully retrieved page content ({len(page_content)} characters)")
        return page_content
        
    finally:
        if tab and tab != client.main_tab:
            try:
                await asyncio.wait_for(tab.close(), timeout=5)
            except Exception as e:
                context.log.warning(f"Error closing tab: {e}")


def extract_contract_data(soup: BeautifulSoup, context: AssetExecutionContext) -> dict:
    """Extract all contract data from the parsed HTML using precise CSS selectors."""
    
    data = {}
    
    context.log.info("Starting contract data extraction with precise CSS selectors...")
    
    # Define precise CSS selectors for each field based on provided notes
    css_selectors = {
        'contract_title': '#body_x_rdCtrPubLabel',
        'contract_id': '#body_x_rdCtrPubCode', 
        'alternate_id': '#body_x_txtCtrRef',
        'contract_type': '#body_x_rdCtrPubType',
        'effective_date': '#body_x_rdCtrPubEffDate',
        'expiration_date': '#body_x_rdCtrPubEndDate',
        'contract_amount': '#body_x_rdCtrCurrency',
        'currency': '#body_x_rdCtrPubAmount',
        'vendor_name': '#body_x_rdCtrPubSupplier',
        'procurement_officer': '#body_x_selContactId .text',  # Dropdown text
        'contact_email': '#body_x_txtContactEmail',
        'agency_org': '#body_x_rdCtrPubOrga',
        'commodities': '#body_x_rdCtrPubFam',
        'vsbe_goal_percentage': '#body_x_txtCtrVsbeGoalPercentage',
        'linked_solicitation': '#body_x_selBpmId',
        'contract_scope': '#body_x_placeholder_contract_190315141153_content',
        'documents_available': '#body_x_rdPublicDocuments'
    }
    
    # Extract fields using CSS selectors
    for field_name, selector in css_selectors.items():
        try:
            element = soup.select_one(selector)
            if element:
                # Handle different element types and extraction methods
                if element.name == 'input':
                    if element.get('type') == 'checkbox':
                        # For checkboxes, check if it's checked
                        value = 'Yes' if element.get('checked') else 'No'
                    else:
                        # For input fields, get the value attribute
                        value = element.get('value', '').strip()
                elif element.name == 'select':
                    # For select dropdowns, get the selected option's text
                    selected_option = element.find('option', selected=True)
                    if selected_option:
                        value = selected_option.get_text(strip=True)
                    else:
                        # Get text content of the select element itself
                        value = element.get_text(strip=True)
                elif '.text' in selector:
                    # For dropdown .text divs, get the text content directly
                    value = element.get_text(strip=True)
                else:
                    # Get text content, handling both text content and input values
                    value = element.get_text(strip=True) or element.get('value', '').strip()
                
                # Special processing for specific fields
                if field_name == 'contract_amount' and value:
                    # Validate numeric amount and clean formatting
                    try:
                        # Remove commas and validate it's a number
                        clean_amount = value.replace(',', '')
                        float(clean_amount)  # Test if it's a valid number
                        data[field_name] = value  # Store original formatted string
                        context.log.info(f"Extracted {field_name}: {value}")
                    except ValueError:
                        data[field_name] = value  # Store as-is if not numeric
                        context.log.warning(f"Non-numeric contract amount: {value}")
                elif field_name == 'vsbe_goal_percentage' and value:
                    # Validate percentage field
                    try:
                        float(value)
                        data[field_name] = value
                        context.log.info(f"Extracted {field_name}: {value}")
                    except ValueError:
                        data[field_name] = value
                        context.log.warning(f"Non-numeric VSBE percentage: {value}")
                elif field_name == 'contract_scope' and value:
                    # Extract plain text from rich content
                    if len(value) > 1000:  # Limit length
                        data[field_name] = value[:1000] + "..."
                    else:
                        data[field_name] = value
                    context.log.info(f"Extracted {field_name}: {value[:100]}...")
                elif field_name == 'procurement_officer' and value:
                    # Trim leading whitespace as noted in guide
                    data[field_name] = value.strip()
                    context.log.info(f"Extracted {field_name}: {value}")
                elif value and value.strip():
                    data[field_name] = value.strip()
                    context.log.info(f"Extracted {field_name}: {value}")
                else:
                    data[field_name] = None
                    context.log.debug(f"Found element for {field_name} but no content")
            else:
                # Try alternative selectors for complex dropdowns
                if field_name == 'procurement_officer':
                    # Fallback to different selector patterns
                    alt_selectors = [
                        '[data-selector="body_x_selContactId"] .text',
                        'div[data-selector="body_x_selContactId"] .text',
                        '#body_x_selContactId_search + .text'
                    ]
                    for alt_selector in alt_selectors:
                        alt_element = soup.select_one(alt_selector)
                        if alt_element:
                            value = alt_element.get_text(strip=True).strip()
                            if value:
                                data[field_name] = value
                                context.log.info(f"Extracted {field_name} (alt): {value}")
                                break
                    else:
                        data[field_name] = None
                        context.log.debug(f"No alternative found for {field_name}")
                else:
                    data[field_name] = None
                    context.log.debug(f"Element not found for {field_name} with selector: {selector}")
                
        except Exception as e:
            context.log.warning(f"Error extracting {field_name}: {e}")
            data[field_name] = None
    
    context.log.info(f"Contract data extraction complete. Extracted {len([v for v in data.values() if v is not None])} non-null fields")
    
    return data