"""
EMMA Solicitations Gold transformation asset
Scrapes detailed solicitation pages and extracts comprehensive data
"""
from dagster import asset, AssetExecutionContext, MaterializeResult
from datetime import datetime
from bs4 import BeautifulSoup
import logging
import asyncio
import json
import re

from common.resources.unified_scraper import UnifiedScraperResource
from common.resources.database import DatabaseResource
from engineering_pipeline.models.engineering_models import EmmaSolicitationsGold

logger = logging.getLogger(__name__)


@asset(
    required_resource_keys={"unified_scraper", "db"},
    group_name="engineering_data",
    description="Scrape detailed EMMA solicitation pages and extract comprehensive data to Gold layer"
)
def emma_solicitations_gold(context: AssetExecutionContext) -> MaterializeResult:
    """Scrape solicitation detail pages and extract comprehensive data."""
    scraper: UnifiedScraperResource = context.resources.unified_scraper
    db: DatabaseResource = context.resources.db
    
    # For testing, use hardcoded URL
    test_url = "https://emma.maryland.gov/page.aspx/en/bpm/process_manage_extranet/81788"
    test_solicitation_id = "BPM050845"  # Based on the URL pattern
    
    try:
        context.log.info(f"Starting Gold layer processing for test URL: {test_url}")
        
        # Get page content using scraper
        client = scraper.client
        if not client._initialized:
            raise ValueError("Scraper client not initialized")
        
        async def scrape_detail_page():
            """Scrape the detail page and extract data"""
            
            # Get a tab
            tab = await asyncio.wait_for(client.browser.get('about:blank'), timeout=10)
            
            try:
                # Navigate to the URL
                context.log.info(f"Navigating to {test_url}")
                await asyncio.wait_for(tab.get(test_url), timeout=30)
                
                # Wait for page to load
                context.log.info("Waiting for page to load...")
                await asyncio.sleep(8)
                
                # Get the page content
                page_content = await tab.get_content()
                
                return page_content
                
            finally:
                if tab and tab != client.main_tab:
                    try:
                        await asyncio.wait_for(tab.close(), timeout=5)
                    except Exception as e:
                        context.log.warning(f"Error closing tab: {e}")
        
        # Run the async function
        html_content = asyncio.run_coroutine_threadsafe(
            scrape_detail_page(),
            client.event_loop
        ).result(timeout=90)
        
        if not html_content:
            raise ValueError("Failed to get page content")
        
        context.log.info(f"Successfully retrieved page content ({len(html_content)} characters)")
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract data
        extracted_data = extract_solicitation_data(soup, context)
        
        # Add metadata
        extracted_data.update({
            'solicitation_id': test_solicitation_id,
            'detail_url': test_url,
            'source_silver_id': None  # No silver record for test
        })
        
        # Store in Gold table
        with db.session_scope() as session:
            # Check if record exists
            existing_record = session.query(EmmaSolicitationsGold).filter(
                EmmaSolicitationsGold.solicitation_id == test_solicitation_id
            ).first()
            
            if existing_record:
                # Update existing record
                for key, value in extracted_data.items():
                    if hasattr(existing_record, key):
                        setattr(existing_record, key, value)
                existing_record.updated_at = datetime.utcnow()
                existing_record.processed_at = datetime.utcnow()
                context.log.info(f"Updated existing Gold record for {test_solicitation_id}")
                action = "updated"
            else:
                # Create new record
                gold_record = EmmaSolicitationsGold(
                    **extracted_data,
                    processed_at=datetime.utcnow(),
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                session.add(gold_record)
                context.log.info(f"Created new Gold record for {test_solicitation_id}")
                action = "created"
        
        return MaterializeResult(
            metadata={
                "solicitation_id": test_solicitation_id,
                "detail_url": test_url,
                "action": action,
                "page_content_length": len(html_content),
                "extracted_fields": len([k for k, v in extracted_data.items() if v is not None])
            }
        )
        
    except Exception as e:
        logger.error(f"Error processing Gold layer: {e}")
        raise


def extract_solicitation_data(soup: BeautifulSoup, context: AssetExecutionContext) -> dict:
    """Extract all solicitation data from the parsed HTML using precise CSS selectors."""
    
    data = {}
    
    context.log.info("Starting data extraction with precise CSS selectors...")
    
    # Define precise CSS selectors for each field based on actual HTML structure
    css_selectors = {
        'solicitation_id': '#body_x_tabc_rfp_ext_prxrfp_ext_x_lblProcessCode',
        'title': '#body_x_tabc_rfp_ext_prxrfp_ext_x_lblLabel',
        'status': '[data-selector="body_x_tabc_rfp_ext_prxrfp_ext_x_selStatusCode"] .text',  # Dropdown text div
        'solicitation_type': '#body_x_tabc_rfp_ext_prxrfp_ext_x_selRfptypeCode',  # Select dropdown
        'lot_number': '#body_x_tabc_rfp_ext_prxrfp_ext_x_lblLot',
        'round_number': '#body_x_tabc_rfp_ext_prxrfp_ext_x_lblRound',
        'main_category': '#body_x_tabc_rfp_ext_prxrfp_ext_x_txtFamLabel',
        'issuing_agency': '#body_x_tabc_rfp_ext_prxrfp_ext_x_txtOrgaLabel',
        'due_date': '#body_x_tabc_rfp_ext_prxrfp_ext_x_txtRfpEndDateEst',
        'project_cost_class': '#body_x_tabc_rfp_ext_prxrfp_ext_x_selPcclasCode',  # Select dropdown
        'mbe_participation_pct': '#body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmMbeGoalPercentage',
        'procurement_officer': '[data-selector="body_x_tabc_rfp_ext_prxrfp_ext_x_selContactId_1"] .text',  # Dropdown text div
        'email': '#body_x_tabc_rfp_ext_prxrfp_ext_x_txtContactEmail_1',  # Corrected ID
        'solicitation_summary': '#body_x_tabc_rfp_ext_prxrfp_ext_x_lblSummary',
        'additional_instructions': '#body_x_tabc_rfp_ext_prxrfp_ext_x_lblProcess',
        'small_business_reserve': '#body_x_tabc_rfp_ext_prxrfp_ext_x_cbBpmSbrDesignation'  # Checkbox
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
                
                # Clean up empty strings
                if value and value.strip():
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
                        '#body_x_tabc_rfp_ext_prxrfp_ext_x_selContactId_1_search + .text',
                        '[data-selector="body_x_tabc_rfp_ext_prxrfp_ext_x_selContactId_1"] div.text',
                        'div[data-selector="body_x_tabc_rfp_ext_prxrfp_ext_x_selContactId_1"] .text'
                    ]
                    for alt_selector in alt_selectors:
                        alt_element = soup.select_one(alt_selector)
                        if alt_element:
                            value = alt_element.get_text(strip=True)
                            if value:
                                data[field_name] = value
                                context.log.info(f"Extracted {field_name} (alt): {value}")
                                break
                    else:
                        data[field_name] = None
                        context.log.debug(f"No alternative found for {field_name}")
                elif field_name == 'status':
                    # Fallback for status dropdown
                    alt_selectors = [
                        'div[data-selector="body_x_tabc_rfp_ext_prxrfp_ext_x_selStatusCode"] .text',
                        '#body_x_tabc_rfp_ext_prxrfp_ext_x_selStatusCode_search + .text'
                    ]
                    for alt_selector in alt_selectors:
                        alt_element = soup.select_one(alt_selector)
                        if alt_element:
                            value = alt_element.get_text(strip=True)
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
    
    # Extract additional MBE participation percentages using specific selectors from notes
    mbe_additional_selectors = {
        'asian_american_pct': '#body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmMbeSubAsianAmerican',
        'hispanic_american_pct': '#body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmMbeSubHispanicAmerican',
        'women_owned_pct': '#body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmMbeSubWomenOwned',
        'african_american_pct': '#body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmMbeSubAfricanAmerican',
        'dbe_participation_pct': '#body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmDbeGoalPercentage',
        'sbe_participation_pct': '#body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmSbeGoalPercentage',
        'vsbe_participation_pct': '#body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmVsbeGoalPercentage'
    }
    
    for field_name, selector in mbe_additional_selectors.items():
        try:
            element = soup.select_one(selector)
            if element:
                value = element.get_text(strip=True) or element.get('value', '').strip()
                if value and value.strip():
                    # Try to convert to float for percentage fields
                    try:
                        float_value = float(value)
                        data[field_name] = str(float_value)  # Store as string but validate it's numeric
                        context.log.info(f"Extracted {field_name}: {value}")
                    except ValueError:
                        # If not a valid number, store as is
                        data[field_name] = value
                        context.log.warning(f"Non-numeric value for {field_name}: {value}")
                else:
                    data[field_name] = None
            else:
                data[field_name] = None
                
        except Exception as e:
            context.log.warning(f"Error extracting {field_name}: {e}")
            data[field_name] = None
    
    # Extract BMP ID from the solicitation_id field if available
    try:
        if data.get('solicitation_id'):
            # The solicitation_id might be the BMP ID itself
            data['bmp_id'] = data['solicitation_id']
        else:
            # Fallback to regex search
            page_text = soup.get_text()
            bmp_match = re.search(r'BMP[#\s]*([A-Z0-9]+)', page_text, re.IGNORECASE)
            if bmp_match:
                data['bmp_id'] = bmp_match.group(1)
                context.log.info(f"Extracted BMP ID via regex: {data['bmp_id']}")
            else:
                data['bmp_id'] = None
    except Exception as e:
        context.log.warning(f"Error extracting BMP ID: {e}")
        data['bmp_id'] = None
    
    # Extract links section
    try:
        links_data = []
        links_section = soup.find(attrs={"data-context-id": "body_x_tabc_rfp_ext_prxrfp_ext_x_placeholder_rfp_160831232210"})
        
        if links_section:
            links = links_section.find_all('a')
            context.log.info(f"Found {len(links)} links in links section")
            
            for link in links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                if href and text:
                    link_type = "unknown"
                    if "rsvp" in text.lower():
                        link_type = "rsvp"
                    elif "rfp" in text.lower():
                        link_type = "rfp"
                    elif href.startswith('http'):
                        link_type = "external"
                    
                    links_data.append({
                        "text": text,
                        "url": href,
                        "type": link_type
                    })
        
        data['solicitation_links'] = json.dumps(links_data) if links_data else None
        context.log.info(f"Extracted {len(links_data)} navigation links")
        
    except Exception as e:
        context.log.warning(f"Error extracting links: {e}")
        data['solicitation_links'] = None
    
    # Extract attachments section
    try:
        attachments_data = []
        attachments_section = soup.find(attrs={"data-context-id": "body_x_tabc_rfp_ext_prxrfp_ext_x_phcDoc"})
        
        if attachments_section:
            attachments = attachments_section.find_all('a')
            context.log.info(f"Found {len(attachments)} attachments in attachments section")
            
            for attachment in attachments:
                href = attachment.get('href', '')
                text = attachment.get_text(strip=True)
                if href and text:
                    # Determine file type from URL or text
                    file_type = "unknown"
                    if '.pdf' in href.lower() or '.pdf' in text.lower():
                        file_type = "PDF"
                    elif '.xlsx' in href.lower() or '.xls' in href.lower():
                        file_type = "XLSX"
                    elif '.doc' in href.lower():
                        file_type = "DOC"
                    elif '.dwg' in href.lower():
                        file_type = "DWG"
                    
                    attachments_data.append({
                        "title": text,
                        "type": file_type,
                        "url": href,
                        "size": None  # Could be extracted if available
                    })
        
        data['attachments'] = json.dumps(attachments_data) if attachments_data else None
        context.log.info(f"Extracted {len(attachments_data)} attachments")
        
    except Exception as e:
        context.log.warning(f"Error extracting attachments: {e}")
        data['attachments'] = None
    
    # Note: solicitation_summary and additional_instructions are already extracted above via CSS selectors
    
    # Parse alternate ID from title
    try:
        if data.get('title'):
            # Look for patterns like "25-XXX" or "BPM-XXX" in title
            alt_id_match = re.search(r'(\d{2,4}-[A-Z0-9]+)', data['title'])
            if alt_id_match:
                data['alternate_id'] = alt_id_match.group(1)
                context.log.info(f"Extracted alternate ID: {data['alternate_id']}")
            else:
                data['alternate_id'] = None
        else:
            data['alternate_id'] = None
            
    except Exception as e:
        context.log.warning(f"Error extracting alternate ID: {e}")
        data['alternate_id'] = None
    
    context.log.info(f"Data extraction complete. Extracted {len([v for v in data.values() if v is not None])} non-null fields")
    
    return data