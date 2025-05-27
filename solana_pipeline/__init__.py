"""Solana Pipeline Dagster package."""
from dotenv import load_dotenv

# Load environment variables at module import time
load_dotenv()

# Export the repository directly for Dagster to find it
from solana_pipeline.repository import solana_repository as repository

