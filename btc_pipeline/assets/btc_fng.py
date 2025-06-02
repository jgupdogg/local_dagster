"""
Bitcoin Fear & Greed Index Asset - Revised for new schema and bulk operations
"""
from dagster import asset, AssetExecutionContext, MaterializeResult
from sqlalchemy.orm import Session
import pandas as pd
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

from btc_pipeline.models.btc_models import FearGreedIndex
from common.resources.unified_scraper import UnifiedScraperResource
from common.resources.database import DatabaseResource

logger = logging.getLogger(__name__)


def _parse_fear_and_greed_history(fear_and_greed_data: dict) -> List[Dict[str, Any]]:
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
        
        row = {
            "timestamp": dt_with_tz,
            "value": val
        }
        parsed_rows.append(row)

    return parsed_rows


def _validate_fear_greed_data(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Validate Fear & Greed Index data.
    
    Validates:
    - Value is between 0 and 100
    - Classification matches value range
    """
    validated_records = []
    
    for record in records:
        value = record['value']
        
        # Validate value range
        if not (0 <= value <= 100):
            logger.warning(f"Invalid Fear & Greed value {value} - skipping record")
            continue
        
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
        
        # Add classification to record
        record['classification'] = classification
        validated_records.append(record)
    
    return validated_records


@asset(
    required_resource_keys={"db", "unified_scraper"},
    group_name="market_indicators",
    description="Scrapes Bitcoin Fear & Greed Index data from Alternative.me API"
)
def bitcoin_fear_greed_index(context: AssetExecutionContext) -> MaterializeResult:
    """Scrape and store Bitcoin Fear & Greed Index data using bulk operations."""
    scraper: UnifiedScraperResource = context.resources.unified_scraper
    db: DatabaseResource = context.resources.db
    
    base_url = "https://alternative.me/crypto/fear-and-greed-index/"
    url_pattern = "/api/crypto/fear-and-greed-index/history"

    try:
        context.log.info("Scraping Bitcoin Fear & Greed Index data...")
        
        # Scrape API data
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
        
        # Parse historical data
        parsed_rows = _parse_fear_and_greed_history(extracted_data)
        if not parsed_rows:
            raise ValueError("No historical data could be parsed")
        
        context.log.info(f"Parsed {len(parsed_rows)} historical records")
        
        # Validate and prepare records
        validated_records = _validate_fear_greed_data(parsed_rows)
        
        if not validated_records:
            raise ValueError("No valid records after validation")
        
        context.log.info(f"Validated {len(validated_records)} records")
        
        # Add API URL to all records
        api_url = response_data.get('url', '')
        for record in validated_records:
            record['api_url'] = api_url
        
        # Optionally limit historical data on first run (last 90 days)
        # Uncomment if you want to limit initial load
        # from datetime import timedelta
        # cutoff_date = datetime.now() - timedelta(days=90)
        # validated_records = [r for r in validated_records if r['timestamp'] > cutoff_date]
        
        # Bulk insert with conflict handling
        records_inserted = db.bulk_insert_ignore_conflicts(
            model_class=FearGreedIndex,
            records=validated_records,
            conflict_columns=['timestamp']  # Unique constraint on timestamp
        )
        
        # Calculate metadata
        latest_record = max(validated_records, key=lambda x: x['timestamp']) if validated_records else {}
        earliest_record = min(validated_records, key=lambda x: x['timestamp']) if validated_records else {}
        
        # Value distribution for monitoring
        value_distribution = {}
        for record in validated_records:
            classification = record['classification']
            value_distribution[classification] = value_distribution.get(classification, 0) + 1
        
        metadata = {
            "records_processed": len(validated_records),
            "records_inserted": records_inserted,
            "records_skipped": len(validated_records) - records_inserted,
            "latest_value": latest_record.get('value'),
            "latest_timestamp": latest_record.get('timestamp').isoformat() if latest_record else None,
            "earliest_timestamp": earliest_record.get('timestamp').isoformat() if earliest_record else None,
            "value_distribution": value_distribution,
            "api_url": api_url,
            "capture_time": datetime.utcnow().isoformat()
        }
        
        context.log.info(
            f"Successfully processed Fear & Greed Index data: "
            f"{records_inserted} new records inserted, "
            f"{len(validated_records) - records_inserted} duplicates skipped"
        )
        
        return MaterializeResult(metadata=metadata)
        
    except Exception as e:
        context.log.error(f"Error scraping Fear & Greed Index: {str(e)}")
        raise