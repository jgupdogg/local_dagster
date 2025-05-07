from dagster import op, Out, job, graph
import logging
from datetime import datetime

@op(
    required_resource_keys={"birdeye_api", "db"},
    out={"timestamp": Out(str)},
)
def extract_trending_tokens(context):
    """
    Extract trending tokens from BirdEye API and store in bronze layer.
    """
    birdeye_sdk = context.resources.birdeye_api
    db_manager = context.resources.db
    limit = 50  # You can make this configurable
    
    context.log.info(f"Fetching trending tokens from BirdEye API with limit {limit}...")
    # This is just a placeholder - you'll implement the actual logic
    
    # Return the timestamp
    return {"timestamp": datetime.now().isoformat()}

@graph
def process_tokens():
    """Process trending tokens and filter for quality."""
    return extract_trending_tokens()