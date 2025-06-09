"""
EMMA Public Solicitations Silver transformation asset
Transforms raw HTML from bronze to structured data in silver
"""
from dagster import asset, AssetExecutionContext, MaterializeResult
from datetime import datetime
from bs4 import BeautifulSoup
import logging
from sqlalchemy import text

from common.resources.database import DatabaseResource
from engineering_pipeline.models.engineering_models import (
    EmmaPublicSolicitations, 
    EmmaPublicSolicitationsSilver
)

logger = logging.getLogger(__name__)


@asset(
    required_resource_keys={"db"},
    group_name="engineering_data",
    description="Transform raw EMMA solicitations HTML to structured silver data"
)
def emma_solicitations_silver(context: AssetExecutionContext) -> MaterializeResult:
    """Parse HTML from bronze and create structured silver records with change detection."""
    db: DatabaseResource = context.resources.db
    
    try:
        # Get latest bronze data
        with db.session_scope() as session:
            bronze_record = session.query(EmmaPublicSolicitations).order_by(
                EmmaPublicSolicitations.timestamp.desc()
            ).first()
            
            if not bronze_record:
                context.log.warning("No bronze solicitations data found")
                return MaterializeResult(metadata={"records_processed": 0})
            
            # Extract values while in session
            bronze_id = bronze_record.id
            bronze_timestamp = bronze_record.timestamp
            raw_html = bronze_record.value.get('raw_html', '')
            
            context.log.info(f"Processing bronze record ID: {bronze_id}")
            
            if not raw_html:
                context.log.warning("No HTML content in bronze record")
                return MaterializeResult(metadata={"records_processed": 0})
        
        # Parse HTML using the same logic from the notebook
        soup = BeautifulSoup(raw_html, 'html.parser')
        tables = soup.find_all('table')
        
        if len(tables) < 11:
            context.log.warning(f"Expected at least 11 tables, found {len(tables)}")
            return MaterializeResult(metadata={"records_processed": 0})
        
        # Extract data from table 11 (index 10)
        target_table = tables[10]
        rows = target_table.find_all('tr')
        
        context.log.info(f"Found table 11 with {len(rows)} rows")
        
        if len(rows) <= 1:
            context.log.warning("Table 11 has no data rows")
            return MaterializeResult(metadata={"records_processed": 0})
        
        # Get headers
        header_row = rows[0]
        headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
        context.log.info(f"Headers: {headers}")
        
        # Process each row
        new_records = 0
        updated_records = 0
        skipped_records = 0
        
        with db.session_scope() as session:
            for i, row in enumerate(rows[1:], 1):
                cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
                
                if not cells or not any(cell.strip() for cell in cells):
                    continue
                
                # Pad cells to match headers
                while len(cells) < len(headers):
                    cells.append('')
                
                # Create record from cells - map to our schema fields
                solicitation_data = {}
                title_cell = None  # Store the actual cell element for link extraction
                
                for j, header in enumerate(headers):
                    if j < len(cells):
                        # Map header names to our schema fields
                        if header == 'ID':
                            solicitation_data['solicitation_id'] = cells[j]
                        elif header == 'Title':
                            # Store the text content
                            solicitation_data['title'] = cells[j]
                            # Store the actual cell element for link extraction
                            title_cell = row.find_all(['td', 'th'])[j] if j < len(row.find_all(['td', 'th'])) else None
                        elif header == 'Status':
                            solicitation_data['status'] = cells[j]
                        elif header == 'Due / Close Date':
                            solicitation_data['due_close_date'] = cells[j]
                        elif header == 'Publish Date UTC-4':
                            solicitation_data['publish_date'] = cells[j]
                        elif header == 'Main Category':
                            solicitation_data['main_category'] = cells[j]
                        elif header == 'Solicitation Type':
                            solicitation_data['solicitation_type'] = cells[j]
                        elif header == 'Issuing Agency':
                            solicitation_data['issuing_agency'] = cells[j]
                        elif header == 'Auto opening':
                            solicitation_data['auto_opening'] = cells[j]
                        elif header == 'Round #':
                            solicitation_data['round_number'] = cells[j]
                        elif header == 'Award Status':
                            solicitation_data['award_status'] = cells[j]
                        elif header == 'Procurement Officer / Buyer':
                            solicitation_data['procurement_officer'] = cells[j]
                        elif header == 'Authority':
                            solicitation_data['authority'] = cells[j]
                        elif header == 'Sub Agency':
                            solicitation_data['sub_agency'] = cells[j]
                
                # Extract detail URL from title cell if available
                if title_cell:
                    link = title_cell.find('a')
                    if link and link.get('href'):
                        href = link.get('href')
                        # Construct full URL
                        if href.startswith('/'):
                            detail_url = f"https://emma.maryland.gov{href}"
                        else:
                            detail_url = href
                        solicitation_data['detail_url'] = detail_url
                        context.log.info(f"Extracted URL for {solicitation_data.get('solicitation_id', 'Unknown')}: {detail_url}")
                    else:
                        solicitation_data['detail_url'] = None
                else:
                    solicitation_data['detail_url'] = None
                
                # Must have solicitation_id
                if not solicitation_data.get('solicitation_id'):
                    context.log.warning(f"Row {i} missing solicitation_id, skipping")
                    continue
                
                # Check if record exists and compare fields
                existing_record = session.query(EmmaPublicSolicitationsSilver).filter(
                    EmmaPublicSolicitationsSilver.solicitation_id == solicitation_data['solicitation_id']
                ).order_by(EmmaPublicSolicitationsSilver.created_at.desc()).first()
                
                # Field-by-field comparison
                has_changes = False
                if existing_record:
                    # Compare each business field
                    fields_to_compare = [
                        'title', 'detail_url', 'status', 'due_close_date', 'publish_date', 'main_category',
                        'solicitation_type', 'issuing_agency', 'auto_opening', 'round_number',
                        'award_status', 'procurement_officer', 'authority', 'sub_agency'
                    ]
                    
                    for field in fields_to_compare:
                        current_value = solicitation_data.get(field, '').strip()
                        existing_value = getattr(existing_record, field, '').strip() if getattr(existing_record, field) else ''
                        
                        if current_value != existing_value:
                            has_changes = True
                            context.log.info(f"Field '{field}' changed for {solicitation_data['solicitation_id']}: '{existing_value}' -> '{current_value}'")
                            break
                
                # Create new record if needed
                if not existing_record or has_changes:
                    new_silver_record = EmmaPublicSolicitationsSilver(
                        solicitation_id=solicitation_data['solicitation_id'],
                        title=solicitation_data.get('title'),
                        detail_url=solicitation_data.get('detail_url'),
                        status=solicitation_data.get('status'),
                        due_close_date=solicitation_data.get('due_close_date'),
                        publish_date=solicitation_data.get('publish_date'),
                        main_category=solicitation_data.get('main_category'),
                        solicitation_type=solicitation_data.get('solicitation_type'),
                        issuing_agency=solicitation_data.get('issuing_agency'),
                        auto_opening=solicitation_data.get('auto_opening'),
                        round_number=solicitation_data.get('round_number'),
                        award_status=solicitation_data.get('award_status'),
                        procurement_officer=solicitation_data.get('procurement_officer'),
                        authority=solicitation_data.get('authority'),
                        sub_agency=solicitation_data.get('sub_agency'),
                        processed=False,
                        source_timestamp=bronze_timestamp,
                        source_bronze_id=bronze_id,
                        updated_at=datetime.utcnow()
                    )
                    
                    session.add(new_silver_record)
                    
                    if existing_record:
                        updated_records += 1
                        context.log.info(f"Updated solicitation: {solicitation_data['solicitation_id']}")
                    else:
                        new_records += 1
                        context.log.info(f"New solicitation: {solicitation_data['solicitation_id']}")
                else:
                    skipped_records += 1
        
        total_processed = new_records + updated_records
        context.log.info(f"Processing complete: {new_records} new, {updated_records} updated, {skipped_records} skipped")
        
        return MaterializeResult(
            metadata={
                "records_processed": total_processed,
                "new_records": new_records,
                "updated_records": updated_records,
                "skipped_records": skipped_records,
                "source_bronze_id": bronze_id,
                "source_timestamp": bronze_timestamp.isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"Error processing solicitations silver: {e}")
        raise