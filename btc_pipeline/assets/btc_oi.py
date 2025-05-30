from dagster import asset, AssetExecutionContext, MaterializeResult
from sqlalchemy.orm import Session
import pandas as pd
import json
from datetime import datetime, timezone
from functools import reduce
import logging
from typing import Dict, List, Any, Optional

from btc_pipeline.models.btc_models import BitcoinDerivativesMetrics
from common.resources.unified_scraper import UnifiedScraperResource

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
    category: str
) -> Dict[str, Any]:
    """Fetch and select the correct 'getTheBars' response for a given category."""
    max_capture = 3 if category == 'funding_rate' else 2
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
                    selected = data; break
                if category == 'liquidations'   and len(first) == 2:
                    selected = data; break
                if category == 'long_short_ratio' and len(first) == 3:
                    selected = data; break
        except Exception:
            continue

    if not selected:
        raise ValueError(f"Valid barData for {category} not found in captures.")

    return selected


def _aggregate_bar_data(bar_data: Dict[str, List[Any]]) -> pd.Series:
    """Aggregate open-interest by averaging each instrument's OHLC then summing per timestamp."""
    agg: Dict[datetime, float] = {}
    for ts_str, instruments in bar_data.items():
        try:
            ts = int(ts_str)
        except (ValueError, TypeError):
            continue
        total_avg = 0.0
        for inst in instruments:
            if not isinstance(inst, list) or len(inst) < 5:
                continue
            vals = inst[1:5]
            if all(isinstance(v, (int, float)) for v in vals):
                total_avg += sum(vals) / 4.0
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
                    records.append({'timestamp': dt, 'liquidations_usd': float(v)})
                elif category == 'long_short_ratio' and len(bar) >= 3:
                    _, long_pct, _ = bar[:3]
                    records.append({'timestamp': dt, 'long_short_ratio': float(long_pct)})
                elif category == 'funding_rate' and len(bar) >= 5:
                    vals = bar[1:5]
                    if all(isinstance(v, (int, float)) for v in vals):
                        avg_fr = sum(vals) / 4.0
                        records.append({'timestamp': dt, 'funding_rate': float(avg_fr)})
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records).set_index('timestamp')

    if not pd.api.types.is_datetime64_any_dtype(df.index):
        df.index = pd.to_datetime(df.index)
    return df


@asset(
    required_resource_keys={"db", "unified_scraper"},
    group_name="market_indicators",
    description="Scrapes and aggregates Bitcoin derivatives metrics from Coinalyze into a unified table"
)
def bitcoin_derivatives_aggregated(
    context: AssetExecutionContext
) -> MaterializeResult:
    scraper: UnifiedScraperResource = context.resources.unified_scraper
    db: Session = context.resources.db

    dfs: List[pd.DataFrame] = []
    fetched: Dict[str, Any] = {}

    # 1) Fetch
    for target in COINALYZE_TARGETS:
        cat = target['category']
        try:
            context.log.info(f"Fetching {cat}")
            fetched[cat] = _fetch_coinalyze_data(scraper, target['url'], cat)
        except Exception as e:
            context.log.error(f"{cat} fetch failed: {e}")

    # 2) Process
    for cat, raw in fetched.items():
        try:
            context.log.info(f"Processing {cat}")
            df = _process_response(cat, raw)
            if df.empty:
                context.log.warning(f"No records for {cat}")
                continue
            dfs.append(df)
        except Exception as e:
            context.log.error(f"{cat} process error: {e}")

    if not dfs:
        raise ValueError("No Coinalyze data processed.")

    # 3) Combine
    combined = reduce(lambda a, b: a.join(b, how='outer'), dfs)
    combined = combined.sort_index()
    combined.index.name = 'timestamp'
    combined = combined.reset_index()

    # 4) Upsert unified model
    count = 0
    with db.get_session() as session:
        for _, row in combined.iterrows():
            ts = row['timestamp']
            vals = {
                'open_interest_usd':  row.get('open_interest_usd'),
                'volume_24h':         row.get('open_interest_usd'),
                'funding_rate':       row.get('funding_rate'),
                'liquidations_usd':   row.get('liquidations_usd'),
                'long_short_ratio':   row.get('long_short_ratio'),
                'api_url':            COINALYZE_TARGETS[0]['url'],
                'exchange':           'COINALYZE_AGGREGATED',
            }
            existing = session.query(BitcoinDerivativesMetrics).filter_by(
                timestamp=ts, exchange='COINALYZE_AGGREGATED'
            ).first()
            if existing:
                for k, v in vals.items():
                    if hasattr(existing, k) and v is not None and getattr(existing, k) != v:
                        setattr(existing, k, v)
            else:
                session.add(BitcoinDerivativesMetrics(timestamp=ts, **vals))
            count += 1
        session.commit()

    meta = {
        'records_processed': count,
        'categories': list(fetched.keys()),
        'earliest': combined['timestamp'].min().isoformat(),
        'latest':   combined['timestamp'].max().isoformat(),
        'capture_time': datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
    }
    context.log.info(f"Wrote {count} merged records to BitcoinDerivativesMetrics")
    return MaterializeResult(metadata=meta)
