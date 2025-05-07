from dagster import repository, schedule
from datetime import datetime, timedelta

@repository
def solana_pipeline():
    """Repository for Solana trading pipeline"""
    return [
        # Jobs and assets will be added here
    ]