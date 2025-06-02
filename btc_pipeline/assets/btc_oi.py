"""
Bitcoin Derivatives Aggregated Asset - Revised for new schema and bulk operations
"""
from dagster import asset, AssetExecutionContext, MaterializeResult
from sqlalchemy.orm import Session
import pandas as pd
import json
from datetime import datetime, timezone
from functools import reduce
import logging
from typing import Dict, List, Any, Optional
from decimal import Decimal

from btc_pipeline.models.btc_models import BitcoinDerivativesMetrics
from common.resources.unified_scraper import UnifiedScraperResource
from common.resources.database import DatabaseResource

logger = logging.getLogger(__name__)

# Coinalyze endpoints and categories
COINALYZE_TARGETS = [
    {"url": "https://coinalyze.net/bitcoin/open-interest/",   "category": "open_interest"},
    {"url": "https://coinalyze.net/bitcoin/funding-rate/",    "category": "funding_rate"},
    {"url": "https://coinalyze.net/bitcoin/liquidations/",     "category": "liquidations"},
    {"url": "https://coinalyze.net/bitcoin/long-short-ratio/",  "category": "long_short_ratio"},
]


def _fetch_coinalyze_data(
    scraper: UnifiedScraperResource,
    url: str,
    category: str,
    context: AssetExecutionContext
) -> Dict[str, Any]:
    """Fetch and select the correct 'getTheBars' response for a given category."""
    max_capture = 3 if category == 'funding_rate' else 2
    
    context.log.info(f"Fetching {category} data from {url}")
    
    responses = scraper.client.scrape_api_data_multiple(
        url=url,
        url_pattern="getTheBars",
        timeout=45,
        capture_count=max_capture,
    )

    if not responses:
        raise ValueError(f"No captures for {category} from {url}")

    selected: Optional[Dict[str, Any]] = None
    
    for resp in responses:
        status = resp.get('status')
        body = resp.get('body')
        
        if status != 200 or not body:
            continue
            
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            continue
            
        bar_wrapper = data.get('barData')
        if not isinstance(bar_wrapper, dict):
            continue

        # Category-specific validation
        try:
            if category == 'funding_rate':
                symbols = bar_wrapper.get('symbolsBarData')
                if isinstance(symbols, dict) and symbols:
                    ts0 = next(iter(symbols))
                    label = symbols[ts0][0][0]
                    if '_FR.' in label and '_PFR.' not in label:
                        selected = data
                        break
            else:
                ts0 = next(iter(bar_wrapper))
                first = bar_wrapper[ts0][0]
                if category == 'open_interest' and len(first) >= 5:
                    selected = data
                    break
                elif category == 'liquidations' and len(first) == 2:
                    selected = data
                    break
                elif category == 'long_short_ratio' and len(first) == 3:
                    selected = data
                    break
        except Exception as e:
            logger.debug(f"Validation error for {category}: {e}")
            continue

    if not selected:
        raise ValueError(f"Valid barData for {category} not found in captures.")

    return selected


def _aggregate_bar_data(bar_data: Dict[str, List[Any]]) -> pd.Series:
    """Aggregate open-interest by averaging each instrument's OHLC then summing per timestamp."""
    agg: Dict[datetime, Decimal] = {}
    
    for ts_str, instruments in bar_data.items():
        try:
            ts = int(ts_str)
        except (ValueError, TypeError):
            continue
            
        total_avg = Decimal('0')
        valid_instruments = 0
        
        for inst in instruments:
            if not isinstance(inst, list) or len(inst) < 5:
                continue
                
            vals = inst[1:5]  # OHLC values
            if all(isinstance(v, (int, float)) for v in vals):
                # Convert to Decimal for precision
                decimal_vals = [Decimal(str(v)) for v in vals]
                total_avg += sum(decimal_vals) / Decimal('4')
                valid_instruments += 1
                
        if valid_instruments > 0:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            agg[dt] = total_avg
            
    return pd.Series(agg, name='open_interest_usd')


def _process_response(category: str, data: Dict[str, Any]) -> pd.DataFrame:
    """Convert Coinalyze JSON into a DataFrame indexed by timestamp."""
    bar_wrapper = data.get('barData', {})
    
    if category == 'funding_rate':
        bar_data = bar_wrapper.get('symbolsBarData', {})
    else:
        bar_data = bar_wrapper

    if not isinstance(bar_data, dict) or not bar_data:
        logger.warning(f"Empty or invalid barData for {category}")
        return pd.DataFrame()

    if category == 'open_interest':
        series = _aggregate_bar_data(bar_data)
        df = series.to_frame()
    else:
        records = []
        for ts_str, bars in bar_data.items():
            try:
                ts = int(ts_str)
            except (ValueError, TypeError):
                continue
                
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            
            for bar in bars:
                if not isinstance(bar, list):
                    continue
                    
                if category == 'liquidations' and len(bar) == 2:
                    _, v = bar
                    records.append({
                        'timestamp': dt, 
                        'liquidations_usd': Decimal(str(v))
                    })
                elif category == 'long_short_ratio' and len(bar) >= 3:
                    _, long_pct, _ = bar[:3]
                    records.append({
                        'timestamp': dt, 
                        'long_short_ratio': Decimal(str(long_pct))
                    })
                elif category == 'funding_rate' and len(bar) >= 5:
                    vals = bar[1:5]
                    if all(isinstance(v, (int, float)) for v in vals):
                        decimal_vals = [Decimal(str(v)) for v in vals]
                        avg_fr = sum(decimal_vals) / Decimal('4')
                        records.append({
                            'timestamp': dt, 
                            'funding_rate': avg_fr
                        })
                        
        if not records:
            return pd.DataFrame()
            
        df = pd.DataFrame(records).set_index('timestamp')

    if not pd.api.types.is_datetime64_any_dtype(df.index):
        df.index = pd.to_datetime(df.index)
        
    return df


def _validate_derivatives_data(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Validate derivatives data with financial constraints.
    
    Validates:
    - Open interest >= 0
    - Volume >= 0
    - Funding rate between -10% and 10%
    - Liquidations >= 0
    - Long/short ratio between 0 and 100
    """
    validated_records = []
    
    for record in records:
        # Validate each metric if present AND not None
        if 'open_interest_usd' in record and record['open_interest_usd'] is not None:
            if record['open_interest_usd'] < 0:
                logger.warning(f"Invalid open interest {record['open_interest_usd']} at {record['timestamp']}")
                continue
                
        if 'volume_24h' in record and record['volume_24h'] is not None:
            if record['volume_24h'] < 0:
                logger.warning(f"Invalid volume {record['volume_24h']} at {record['timestamp']}")
                continue
                
        if 'funding_rate' in record and record['funding_rate'] is not None:
            # Funding rate typically between -10% and 10%
            if not (-0.1 <= record['funding_rate'] <= 0.1):
                logger.warning(f"Unusual funding rate {record['funding_rate']} at {record['timestamp']}")
                # Don't skip, just log warning
                
        if 'liquidations_usd' in record and record['liquidations_usd'] is not None:
            if record['liquidations_usd'] < 0:
                logger.warning(f"Invalid liquidations {record['liquidations_usd']} at {record['timestamp']}")
                continue
                
        if 'long_short_ratio' in record and record['long_short_ratio'] is not None:
            # Ratio should be between 0 and 100 (percentage)
            if not (0 <= record['long_short_ratio'] <= 100):
                logger.warning(f"Invalid long/short ratio {record['long_short_ratio']} at {record['timestamp']}")
                continue
                
        validated_records.append(record)
        
    return validated_records


@asset(
    required_resource_keys={"db", "unified_scraper"},
    group_name="market_indicators",
    description="Scrapes and aggregates Bitcoin derivatives metrics from Coinalyze into a unified table"
)
def bitcoin_derivatives_aggregated(
    context: AssetExecutionContext
) -> MaterializeResult:
    """Scrape and store aggregated Bitcoin derivatives metrics using bulk operations."""
    scraper: UnifiedScraperResource = context.resources.unified_scraper
    db: DatabaseResource = context.resources.db

    dfs: List[pd.DataFrame] = []
    fetched: Dict[str, Any] = {}
    fetch_errors: Dict[str, str] = {}

    # 1) Fetch data from all categories
    for target in COINALYZE_TARGETS:
        cat = target['category']
        try:
            context.log.info(f"Fetching {cat} data")
            fetched[cat] = _fetch_coinalyze_data(scraper, target['url'], cat, context)
        except Exception as e:
            error_msg = f"{cat} fetch failed: {e}"
            context.log.error(error_msg)
            fetch_errors[cat] = str(e)

    if not fetched:
        raise ValueError("Failed to fetch any Coinalyze data. Errors: " + str(fetch_errors))

    # 2) Process fetched data
    process_errors: Dict[str, str] = {}
    
    for cat, raw in fetched.items():
        try:
            context.log.info(f"Processing {cat} data")
            df = _process_response(cat, raw)
            
            if df.empty:
                context.log.warning(f"No records for {cat}")
                continue
                
            context.log.info(f"Processed {len(df)} records for {cat}")
            dfs.append(df)
            
        except Exception as e:
            error_msg = f"{cat} process error: {e}"
            context.log.error(error_msg)
            process_errors[cat] = str(e)

    if not dfs:
        raise ValueError(f"No Coinalyze data processed. Fetch errors: {fetch_errors}, Process errors: {process_errors}")

    # 3) Combine all dataframes
    context.log.info(f"Combining {len(dfs)} dataframes")
    combined = reduce(lambda a, b: a.join(b, how='outer'), dfs)
    combined = combined.sort_index()
    combined.index.name = 'timestamp'
    combined = combined.reset_index()
    
    context.log.info(f"Combined data has {len(combined)} unique timestamps")

    # 4) Prepare records for bulk upsert
    records = []
    api_url = COINALYZE_TARGETS[0]['url']  # Use first URL as reference

    # Define all possible metric columns
    metric_columns = ['open_interest_usd', 'volume_24h', 'funding_rate', 'liquidations_usd', 'long_short_ratio']

    for _, row in combined.iterrows():
        # Start with required fields
        record = {
            'timestamp': row['timestamp'],
            'exchange': 'COINALYZE_AGGREGATED',
            'api_url': api_url,
        }
        
        # Initialize ALL metric columns - this is critical for bulk operations
        for col in metric_columns:
            record[col] = None
        
        # Now override with actual values where present
        if 'open_interest_usd' in row and pd.notna(row['open_interest_usd']):
            record['open_interest_usd'] = row['open_interest_usd']
            record['volume_24h'] = row['open_interest_usd']  # Use as proxy
            
        if 'funding_rate' in row and pd.notna(row['funding_rate']):
            record['funding_rate'] = row['funding_rate']
            
        if 'liquidations_usd' in row and pd.notna(row['liquidations_usd']):
            record['liquidations_usd'] = row['liquidations_usd']
            
        if 'long_short_ratio' in row and pd.notna(row['long_short_ratio']):
            record['long_short_ratio'] = row['long_short_ratio']
                    
        records.append(record)
    
    
    # 5) Validate records
    context.log.info(f"Validating {len(records)} records")
    validated_records = _validate_derivatives_data(records)
    
    if not validated_records:
        raise ValueError("No valid records after validation")
    
    context.log.info(f"Validated {len(validated_records)} records")
    
    # 6) Only include columns in update_columns that might actually be present
    # This prevents issues with columns that are always None
    update_columns = ['api_url']  # Always update api_url
    
    # Check which columns actually have data
    sample_record = validated_records[0] if validated_records else {}
    potential_columns = ['open_interest_usd', 'volume_24h', 'funding_rate', 'liquidations_usd', 'long_short_ratio']
    
    for col in potential_columns:
        if any(col in record for record in validated_records):
            update_columns.append(col)
    
    context.log.info(f"Update columns: {update_columns}")
    
    # 7) Bulk upsert to database
    records_affected = db.bulk_insert_ignore_conflicts(
        model_class=BitcoinDerivativesMetrics,
        records=validated_records,
        conflict_columns=['timestamp', 'exchange']
    )

    # 8) Calculate metadata
    timestamps = [r['timestamp'] for r in validated_records]
    earliest = min(timestamps) if timestamps else None
    latest = max(timestamps) if timestamps else None
    
    # Metric coverage
    metric_coverage = {}
    for metric in ['open_interest_usd', 'funding_rate', 'liquidations_usd', 'long_short_ratio']:
        count = sum(1 for r in validated_records if metric in r)
        metric_coverage[metric] = {
            'count': count,
            'coverage_pct': round(count / len(validated_records) * 100, 2) if validated_records else 0
        }

    metadata = {
        'records_processed': len(combined),
        'records_validated': len(validated_records),
        'records_affected': records_affected,
        'categories_fetched': list(fetched.keys()),
        'categories_failed': list(fetch_errors.keys()),
        'earliest_timestamp': earliest.isoformat() if earliest else None,
        'latest_timestamp': latest.isoformat() if latest else None,
        'metric_coverage': metric_coverage,
        'update_columns': update_columns,
        'capture_time': datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
    }

    context.log.info(
        f"Successfully upserted {records_affected} records to BitcoinDerivativesMetrics. "
        f"Categories: {list(fetched.keys())}"
    )
    
    return MaterializeResult(metadata=metadata)