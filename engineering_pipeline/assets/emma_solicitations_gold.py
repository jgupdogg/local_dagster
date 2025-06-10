"""
EMMA Solicitations Gold transformation asset
Scrapes detailed solicitation pages and extracts comprehensive data
"""
from dagster import asset, AssetExecutionContext, MaterializeResult
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import logging
import asyncio
import json
import re
from sqlalchemy import and_

from common.resources.unified_scraper import UnifiedScraperResource
from common.resources.database import DatabaseResource
from engineering_pipeline.models.engineering_models import (
    EmmaSolicitationsGold, 
    EmmaPublicSolicitationsSilver
)

logger = logging.getLogger(__name__)


@asset(
    required_resource_keys={"unified_scraper", "db"},
    group_name="engineering_data",
    description="Scrape detailed EMMA solicitation pages and extract comprehensive data to Gold layer",
    deps=["emma_solicitations_silver"]
)
def emma_solicitations_gold(context: AssetExecutionContext) -> MaterializeResult:
    """Process all unprocessed silver records to create comprehensive gold data."""
    scraper: UnifiedScraperResource = context.resources.unified_scraper
    db: DatabaseResource = context.resources.db
    
    records_processed = 0
    records_created = 0
    records_updated = 0
    records_skipped = 0
    errors = 0
    
    try:
        context.log.info("Starting Gold layer processing for all unprocessed silver records")
        
        # Get unprocessed silver records or records not processed in last 24 hours
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        
        # Get scraper client
        client = scraper.client
        if not client._initialized:
            raise ValueError("Scraper client not initialized")
        
        # Process silver records within a single session
        with db.session_scope() as session:
            # Find silver records that need processing
            unprocessed_records = session.query(EmmaPublicSolicitationsSilver).filter(
                and_(
                    EmmaPublicSolicitationsSilver.detail_url.isnot(None),
                    EmmaPublicSolicitationsSilver.detail_url != '',
                    # Either never processed or not processed in last 24 hours
                    (
                        (EmmaPublicSolicitationsSilver.processed == False) |
                        (EmmaPublicSolicitationsSilver.updated_at < cutoff_time)
                    )
                )
            ).order_by(EmmaPublicSolicitationsSilver.created_at.desc()).limit(50).all()  # Increased limit for better throughput
            
            context.log.info(f"Found {len(unprocessed_records)} silver records to process")
            
            if not unprocessed_records:
                return MaterializeResult(metadata={
                    "records_processed": 0,
                    "message": "No unprocessed silver records found"
                })
            
            # Process each silver record within the same session
            for silver_record in unprocessed_records:
                try:
                    # Access record attributes while session is active
                    solicitation_id = silver_record.solicitation_id
                    detail_url = silver_record.detail_url
                    silver_id = silver_record.id
                    
                    context.log.info(f"Processing solicitation {solicitation_id} - {detail_url}")
                    
                    # Check if gold record already exists and was recently processed
                    existing_gold = session.query(EmmaSolicitationsGold).filter(
                        EmmaSolicitationsGold.solicitation_id == solicitation_id
                    ).first()
                    
                    if existing_gold and existing_gold.processed_at and existing_gold.processed_at > cutoff_time:
                        context.log.info(f"Skipping {solicitation_id} - processed within 24 hours")
                        records_skipped += 1
                        continue
                
                    # Scrape the detail page
                    html_content = asyncio.run_coroutine_threadsafe(
                        scrape_solicitation_detail(client, detail_url, context),
                        client.event_loop
                    ).result(timeout=90)
                    
                    if not html_content:
                        context.log.warning(f"Failed to get content for {solicitation_id}")
                        errors += 1
                        continue
                    
                    # Parse and extract data
                    soup = BeautifulSoup(html_content, 'html.parser')
                    extracted_data = extract_solicitation_data(soup, context)
                    
                    # Add metadata
                    extracted_data.update({
                        'solicitation_id': solicitation_id,
                        'detail_url': detail_url,
                        'source_silver_id': silver_id
                    })
                    
                    # Store/update in Gold table using the same session
                    existing_record = session.query(EmmaSolicitationsGold).filter(
                        EmmaSolicitationsGold.solicitation_id == solicitation_id
                    ).first()
                    
                    if existing_record:
                        # Update existing record
                        for key, value in extracted_data.items():
                            if hasattr(existing_record, key):
                                setattr(existing_record, key, value)
                        existing_record.updated_at = datetime.utcnow()
                        existing_record.processed_at = datetime.utcnow()
                        context.log.info(f"Updated Gold record for {solicitation_id}")
                        records_updated += 1
                    else:
                        # Create new record
                        gold_record = EmmaSolicitationsGold(
                            **extracted_data,
                            processed_at=datetime.utcnow(),
                            created_at=datetime.utcnow(),
                            updated_at=datetime.utcnow()
                        )
                        session.add(gold_record)
                        context.log.info(f"Created new Gold record for {solicitation_id}")
                        records_created += 1
                    
                    # Mark silver record as processed
                    silver_record.processed = True
                    silver_record.updated_at = datetime.utcnow()
                
                    records_processed += 1
                    
                    # Add delay between requests to be respectful
                    import time
                    time.sleep(2)
                    
                except Exception as e:
                    context.log.error(f"Error processing {solicitation_id}: {e}")
                    errors += 1
                    continue
        
        context.log.info(f"Gold processing complete: {records_processed} processed, {records_created} created, {records_updated} updated, {records_skipped} skipped, {errors} errors")
        
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
        logger.error(f"Error in Gold layer processing: {e}")
        raise


async def scrape_solicitation_detail(client, detail_url: str, context: AssetExecutionContext) -> str:
    """Scrape a single solicitation detail page and return the HTML content."""
    
    # Get a tab
    tab = await asyncio.wait_for(client.browser.get('about:blank'), timeout=10)
    
    try:
        # Navigate to the URL
        context.log.info(f"Navigating to {detail_url}")
        await asyncio.wait_for(tab.get(detail_url), timeout=30)
        
        # Wait for page to load
        context.log.info("Waiting for page to load...")
        await asyncio.sleep(5)
        
        # Get the page content
        page_content = await tab.get_content()
        
        context.log.info(f"Retrieved content ({len(page_content)} characters)")
        return page_content
        
    finally:
        if tab and tab != client.main_tab:
            try:
                await asyncio.wait_for(tab.close(), timeout=5)
            except Exception as e:
                context.log.warning(f"Error closing tab: {e}")


def extract_field_value(element, selector: str, field_name: str, context: AssetExecutionContext) -> str:
    """Enhanced field extraction based on verified field mapping notes."""
    if not element:
        return None
    
    try:
        # Handle checkboxes - look for checked attribute
        if element.name == 'input' and element.get('type') == 'checkbox':
            return 'Yes' if element.has_attr('checked') else 'No'
        
        # Handle input elements - get value attribute (for txtFields)
        elif element.name == 'input':
            value = element.get('value', '').strip()
            return value if value else None
        
        # Handle dropdown .text children (for selFields)
        elif '.text' in selector:
            # Look for nested div with class="text"
            text_div = element.find('div', class_='text')
            if text_div:
                return text_div.get_text(strip=True)
            # Fallback to element text
            return element.get_text(strip=True)
        
        # Handle label/span elements (for lblFields)
        elif element.name in ['span', 'label'] and 'lbl' in selector:
            return element.get_text(strip=True)
        
        # Handle div elements
        elif element.name == 'div':
            return element.get_text(strip=True)
        
        # Handle select dropdowns
        elif element.name == 'select':
            selected_option = element.find('option', selected=True)
            if selected_option:
                return selected_option.get_text(strip=True)
            # Fallback to first option that's not empty
            options = element.find_all('option')
            for option in options:
                text = option.get_text(strip=True)
                if text and text.lower() not in ['select', 'choose', '']:
                    return text
            return None
        
        # Default fallback
        else:
            text_value = element.get_text(strip=True)
            return text_value if text_value else None
                
    except Exception as e:
        context.log.warning(f"Error extracting value from element for {field_name}: {e}")
        return None


def extract_solicitation_data(soup: BeautifulSoup, context: AssetExecutionContext) -> dict:
    """Extract all solicitation data from the parsed HTML using data-iv-control attributes."""
    
    data = {}
    
    context.log.info("Starting data extraction with data-iv-control attribute selectors...")
    
    # Define field mappings using data-iv-control attributes based on recommendations
    field_mappings = {
        # Core Fields - using data-iv-control attributes for better reliability
        'title': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_lblLabel',
            'type': 'text',  # span/div - get direct text
            'element': 'span'
        },
        'solicitation_id': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_lblProcessCode',
            'type': 'text',  # div - get direct text
            'element': 'div'
        },
        'alternate_id': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmAlternateId',
            'type': 'value',  # input - get value attribute
            'element': 'input'
        },
        'lot_number': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_lblLot',
            'type': 'text',  # span - get direct text
            'element': 'span'
        },
        'round_number': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_lblRound',
            'type': 'text',  # span - get direct text
            'element': 'span'
        },
        'status': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_selStatusCode',
            'type': 'nested_text',  # container with child div.text
            'element': 'div'
        },
        'due_date': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_txtRfpEndDateEst',
            'type': 'value',  # input - get value attribute
            'element': 'input'
        },
        'close_date_est_2': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_txtRfpRfpEndDateEst',
            'type': 'value',  # input - get value attribute
            'element': 'input'
        },
        'solicitation_type': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_selRfptypeCode',
            'type': 'nested_text',  # container with child div.text
            'element': 'div'
        },
        'main_category': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_txtFamLabel',
            'type': 'value',  # input - get value attribute
            'element': 'input'
        },
        'issuing_agency': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_txtOrgaLabel',
            'type': 'value',  # input - get value attribute
            'element': 'input'
        },
        'procurement_officer': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_selContactId_1',
            'type': 'nested_text',  # container with child div.text
            'element': 'div'
        },
        'email': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_txtContactEmail_1',
            'type': 'value',  # input - get value attribute
            'element': 'input'
        },
        
        # Additional Important Fields
        'project_cost_class': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_selPcclasCode',
            'type': 'nested_text',  # container with child div.text
            'element': 'div'
        },
        'mbe_participation_pct': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmMbeGoalPercentage',
            'type': 'percentage',  # special handling for percentage fields
            'element': 'input'
        },
        'solicitation_summary': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_lblSummary',
            'type': 'text',  # span - get direct text
            'element': 'span'
        },
        'additional_instructions': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_lblProcess',
            'type': 'text',  # span - get direct text
            'element': 'span'
        },
        'small_business_reserve': {
            'control': 'body_x_tabc_rfp_ext_prxrfp_ext_x_cbBpmSbrDesignation',
            'type': 'checkbox',  # checkbox input
            'element': 'input'
        }
    }
    
    # Extract fields using data-iv-control attribute strategy
    for field_name, config in field_mappings.items():
        try:
            # Find element by data-iv-control attribute
            element = soup.find(attrs={'data-iv-control': config['control']})
            
            if element:
                value = None
                
                if config['type'] == 'text':
                    # Get direct text content
                    value = element.get_text(strip=True)
                    # For title field, remove "Title" prefix if it exists
                    if field_name == 'title' and value.startswith('Title'):
                        value = value[5:].strip()
                    # For ID field, remove "ID" prefix if it exists
                    elif field_name == 'solicitation_id' and value.startswith('ID'):
                        value = value[2:].strip()
                    # For summary fields, remove prefix labels
                    elif field_name == 'solicitation_summary' and value.startswith('Solicitation Summary'):
                        value = value[20:].strip()
                    elif field_name == 'additional_instructions' and value.startswith('Additional Instructions'):
                        value = value[23:].strip()
                    
                elif config['type'] == 'value':
                    # Get value attribute from input
                    value = element.get('value', '').strip()
                    
                elif config['type'] == 'nested_text':
                    # Find child div with class="text"
                    text_div = element.find('div', class_='text')
                    if text_div:
                        value = text_div.get_text(strip=True)
                    else:
                        # Fallback to direct text
                        value = element.get_text(strip=True)
                        
                elif config['type'] == 'checkbox':
                    # Check if checkbox is checked
                    value = 'Yes' if element.has_attr('checked') else 'No'
                
                elif config['type'] == 'percentage':
                    # Handle percentage fields - extract numeric value only
                    raw_value = element.get('value', '').strip()
                    if not raw_value:
                        raw_value = element.get_text(strip=True)
                    
                    if raw_value:
                        # Clean the value - remove any non-numeric characters except decimal point
                        cleaned_value = ''.join(c for c in raw_value if c.isdigit() or c == '.')
                        
                        if cleaned_value:
                            try:
                                float_value = float(cleaned_value)
                                # Only store if it's a reasonable percentage (0-100)
                                if 0 <= float_value <= 100:
                                    value = str(float_value)
                                else:
                                    value = None
                            except ValueError:
                                value = None
                        else:
                            value = None
                    else:
                        value = None
                
                if value and str(value).strip():
                    data[field_name] = str(value).strip()
                    context.log.info(f"✅ Extracted {field_name}: {str(value)[:50]}{'...' if len(str(value)) > 50 else ''}")
                else:
                    data[field_name] = None
                    context.log.debug(f"⚠️ Found element for {field_name} but no content")
            else:
                # Try fallback with ID selector for backwards compatibility
                fallback_selector = f"#{config['control']}"
                element = soup.select_one(fallback_selector)
                
                if element:
                    value = extract_field_value(element, fallback_selector, field_name, context)
                    if value and value.strip():
                        data[field_name] = value.strip()
                        context.log.info(f"✅ Extracted {field_name} via ID fallback: {value[:50]}{'...' if len(value) > 50 else ''}")
                    else:
                        data[field_name] = None
                else:
                    data[field_name] = None
                    context.log.debug(f"❌ Element not found for {field_name} with data-iv-control='{config['control']}'")
                
        except Exception as e:
            context.log.warning(f"Error extracting {field_name}: {e}")
            data[field_name] = None
    
    # Extract additional MBE participation percentages using data-iv-control attributes
    mbe_additional_fields = {
        'asian_american_pct': 'body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmMbeSubAsianAmerican',
        'hispanic_american_pct': 'body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmMbeSubHispanicAmerican',
        'women_owned_pct': 'body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmMbeSubWomenOwned',
        'african_american_pct': 'body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmMbeSubAfricanAmerican',
        'dbe_participation_pct': 'body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmDbeGoalPercentage',
        'sbe_participation_pct': 'body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmSbeGoalPercentage',
        'vsbe_participation_pct': 'body_x_tabc_rfp_ext_prxrfp_ext_x_txtBpmVsbeGoalPercentage'
    }
    
    for field_name, control_name in mbe_additional_fields.items():
        try:
            # Try data-iv-control attribute first
            element = soup.find(attrs={'data-iv-control': control_name})
            
            if not element:
                # Fallback to ID selector
                element = soup.select_one(f"#{control_name}")
            
            if element:
                # These are all input elements - get value attribute
                value = element.get('value', '').strip()
                if not value:
                    # Fallback to text content
                    value = element.get_text(strip=True)
                    
                if value and value.strip():
                    # Clean the value - remove any non-numeric characters except decimal point
                    cleaned_value = ''.join(c for c in value if c.isdigit() or c == '.')
                    
                    if cleaned_value:
                        try:
                            float_value = float(cleaned_value)
                            # Only store if it's a reasonable percentage (0-100)
                            if 0 <= float_value <= 100:
                                data[field_name] = str(float_value)
                                context.log.info(f"✅ Extracted {field_name}: {float_value}")
                            else:
                                data[field_name] = None
                                context.log.debug(f"⚠️ Out of range percentage for {field_name}: {float_value}")
                        except ValueError:
                            # If still not a valid number after cleaning, set to None
                            data[field_name] = None
                            context.log.debug(f"⚠️ Could not parse numeric value for {field_name}: {value}")
                    else:
                        # No numeric content found
                        data[field_name] = None
                        context.log.debug(f"⚠️ No numeric content in {field_name}: {value}")
                else:
                    data[field_name] = None
            else:
                data[field_name] = None
                context.log.debug(f"❌ Element not found for {field_name}: data-iv-control='{control_name}'")
                
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