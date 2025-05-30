"""
CORRECTED: Bitcoin Fear & Greed Index Asset
"""
from dagster import asset, AssetExecutionContext, MaterializeResult
from sqlalchemy.orm import Session
import pandas as pd
import json
from typing import Dict, Any, Optional
from datetime import datetime

from btc_pipeline.models.btc_models import FearGreedIndex
from common.resources.unified_scraper import UnifiedScraperResource


def _parse_fear_and_greed_history(fear_and_greed_data: dict) -> list:
    """Convert the raw fear_and_greed_data dict into a list of dicts."""
    labels = fear_and_greed_data.get("labels", [])
    datasets = fear_and_greed_data.get("datasets", [])

    if not datasets:
        return []

    values = datasets[0].get("data", [])
    if not labels or not values:
        return []

    parsed_rows = []
    for label_str, val in zip(labels, values):
        dt_obj = datetime.strptime(label_str, "%d %b, %Y")
        dt_with_tz = dt_obj.astimezone()
        iso_str = dt_with_tz.isoformat(timespec='seconds')  
        timestamp_str = iso_str.replace('T', ' ')

        row = {
            "timestamp": timestamp_str,
            "value": val
        }
        parsed_rows.append(row)

    return parsed_rows


@asset(
    required_resource_keys={"db", "unified_scraper"},
    group_name="market_indicators",
    description="Scrapes Bitcoin Fear & Greed Index data from Alternative.me API"
)
def bitcoin_fear_greed_index(context: AssetExecutionContext) -> MaterializeResult:
    """Scrape and store Bitcoin Fear & Greed Index data."""
    scraper: UnifiedScraperResource = context.resources.unified_scraper
    db: Session = context.resources.db
    
    base_url = "https://alternative.me/crypto/fear-and-greed-index/"
    url_pattern = "/api/crypto/fear-and-greed-index/history"

    with db.get_session() as session:
        try:
            context.log.info("Scraping Bitcoin Fear & Greed Index data...")
            
            response_data = scraper.client.scrape_api_data(
                url=base_url,
                url_pattern=url_pattern,
                timeout=30
            )
            
            if not response_data or 'body' not in response_data:
                raise ValueError("Failed to capture API response")
            
            body = response_data.get('body', '')
            data = json.loads(body) if isinstance(body, str) else body
            
            if data.get("success") != 1:
                raise ValueError("API response did not indicate success")
            
            extracted_data = data.get('data', {})
            if not extracted_data:
                raise ValueError("No data found in API response")
            
            parsed_rows = _parse_fear_and_greed_history(extracted_data)
            if not parsed_rows:
                raise ValueError("No historical data could be parsed")
            
            records_processed = 0
            
            for row in parsed_rows:
                timestamp_str = row['timestamp']
                timestamp = datetime.fromisoformat(timestamp_str.replace(' ', 'T'))
                value = int(row['value'])
                
                # Determine classification based on value
                if value <= 25:
                    classification = "Extreme Fear"
                elif value <= 45:
                    classification = "Fear"
                elif value <= 55:
                    classification = "Neutral"
                elif value <= 75:
                    classification = "Greed"
                else:
                    classification = "Extreme Greed"
                
                # FIXED: Check for existing record by timestamp only (unique constraint)
                existing = session.query(FearGreedIndex).filter_by(
                    timestamp=timestamp
                ).first()
                
                if not existing:
                    # FIXED: Remove created_at since model has default_factory
                    record = FearGreedIndex(
                        timestamp=timestamp,
                        value=value,
                        classification=classification,
                        api_url=response_data.get('url', '')
                        # created_at will be set by default_factory
                    )
                    session.add(record)
                    records_processed += 1
                else:
                    # Update if value changed
                    if existing.value != value or existing.classification != classification:
                        existing.value = value
                        existing.classification = classification
                        # updated_at will be set by onupdate
                        records_processed += 1
            
            session.commit()
            
            latest_row = parsed_rows[0] if parsed_rows else {}
            metadata = {
                "records_processed": records_processed,
                "latest_value": latest_row.get('value'),
                "latest_timestamp": latest_row.get('timestamp'),
                "total_historical_records": len(parsed_rows),
                "api_url": response_data.get('url', ''),
                "capture_time": datetime.utcnow().isoformat()
            }
            
            context.log.info(f"Successfully processed {records_processed} Fear & Greed Index records")
            
            return MaterializeResult(metadata=metadata)
            
        except Exception as e:
            context.log.error(f"Error scraping Fear & Greed Index: {str(e)}")
            session.rollback()
            raise