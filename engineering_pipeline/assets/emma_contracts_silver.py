"""
EMMA Public Contracts Silver transformation asset
Transforms raw HTML from bronze to structured data in silver
"""
from dagster import asset, AssetExecutionContext, MaterializeResult
from datetime import datetime
from bs4 import BeautifulSoup
import logging

from common.resources.database import DatabaseResource
from engineering_pipeline.models.engineering_models import (
    EmmaPublicContracts, 
    EmmaPublicContractsSilver
)

logger = logging.getLogger(__name__)


@asset(
    required_resource_keys={"db"},
    group_name="engineering_data",
    description="Transform raw EMMA contracts HTML to structured silver data"
)
def emma_contracts_silver(context: AssetExecutionContext) -> MaterializeResult:
    """Parse HTML from bronze and create structured silver records with change detection."""
    db: DatabaseResource = context.resources.db
    
    try:
        # Get latest bronze data
        with db.session_scope() as session:
            bronze_record = session.query(EmmaPublicContracts).order_by(
                EmmaPublicContracts.timestamp.desc()
            ).first()
            
            if not bronze_record:
                context.log.warning("No bronze contracts data found")
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
        
        # Extract data from table 11 (index 10) - the contracts table
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
                contract_data = {}
                contract_title_cell = None  # Store the actual cell element for link extraction
                
                for j, header in enumerate(headers):
                    if j < len(cells):
                        # Map header names to our schema fields
                        if header == 'Code':
                            contract_data['contract_code'] = cells[j]
                        elif header == 'Contract Title':
                            # Store the text content
                            contract_data['contract_title'] = cells[j]
                            # Store the actual cell element for link extraction
                            contract_title_cell = row.find_all(['td', 'th'])[j] if j < len(row.find_all(['td', 'th'])) else None
                        elif header == 'Vendor':
                            contract_data['vendor'] = cells[j]
                        elif header == 'Contract Type':
                            contract_data['contract_type'] = cells[j]
                        elif header == 'Effective Date':
                            contract_data['effective_date'] = cells[j]
                        elif header == 'Expiration Date':
                            contract_data['expiration_date'] = cells[j]
                        elif header == 'Linked Solicitation':
                            contract_data['linked_solicitation'] = cells[j]
                        elif header == 'Publish Date':
                            contract_data['publish_date'] = cells[j]
                        elif header == 'Public Solicitation ID':
                            contract_data['public_solicitation_id'] = cells[j]
                
                # Extract detail URL from contract title cell if available
                if contract_title_cell:
                    link = contract_title_cell.find('a')
                    if link and link.get('href'):
                        href = link.get('href')
                        # Construct full URL
                        if href.startswith('/'):
                            detail_url = f"https://emma.maryland.gov{href}"
                        else:
                            detail_url = href
                        contract_data['detail_url'] = detail_url
                        context.log.info(f"Extracted URL for {contract_data.get('contract_code', 'Unknown')}: {detail_url}")
                    else:
                        contract_data['detail_url'] = None
                else:
                    contract_data['detail_url'] = None
                
                # Must have contract_code
                if not contract_data.get('contract_code'):
                    context.log.warning(f"Row {i} missing contract_code, skipping")
                    continue
                
                # Check if record exists and compare fields
                existing_record = session.query(EmmaPublicContractsSilver).filter(
                    EmmaPublicContractsSilver.contract_code == contract_data['contract_code']
                ).order_by(EmmaPublicContractsSilver.created_at.desc()).first()
                
                # Field-by-field comparison
                has_changes = False
                if existing_record:
                    # Compare each business field
                    fields_to_compare = [
                        'contract_title', 'detail_url', 'vendor', 'contract_type', 'effective_date',
                        'expiration_date', 'linked_solicitation', 'publish_date', 'public_solicitation_id'
                    ]
                    
                    for field in fields_to_compare:
                        current_value = contract_data.get(field, '').strip() if contract_data.get(field) else ''
                        existing_value = getattr(existing_record, field, '').strip() if getattr(existing_record, field) else ''
                        
                        if current_value != existing_value:
                            has_changes = True
                            context.log.info(f"Field '{field}' changed for {contract_data['contract_code']}: '{existing_value}' -> '{current_value}'")
                            break
                
                # Create new record if needed
                if not existing_record or has_changes:
                    new_silver_record = EmmaPublicContractsSilver(
                        contract_code=contract_data['contract_code'],
                        contract_title=contract_data.get('contract_title'),
                        detail_url=contract_data.get('detail_url'),
                        vendor=contract_data.get('vendor'),
                        contract_type=contract_data.get('contract_type'),
                        effective_date=contract_data.get('effective_date'),
                        expiration_date=contract_data.get('expiration_date'),
                        linked_solicitation=contract_data.get('linked_solicitation'),
                        publish_date=contract_data.get('publish_date'),
                        public_solicitation_id=contract_data.get('public_solicitation_id'),
                        processed=False,
                        source_timestamp=bronze_timestamp,
                        source_bronze_id=bronze_id,
                        updated_at=datetime.utcnow()
                    )
                    
                    session.add(new_silver_record)
                    
                    if existing_record:
                        updated_records += 1
                        context.log.info(f"Updated contract: {contract_data['contract_code']}")
                    else:
                        new_records += 1
                        context.log.info(f"New contract: {contract_data['contract_code']}")
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
        logger.error(f"Error processing contracts silver: {e}")
        raise