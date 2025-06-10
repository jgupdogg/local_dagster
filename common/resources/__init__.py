"""
Common resources shared across pipelines
"""
from common.resources.database import DatabaseResource, db_resource
from .unified_scraper import UnifiedScraperResource
from .google_sheets import google_sheets_resource

__all__ = [
    "DatabaseResource",
    "db_resource",
    "UnifiedScraperResource",
    "google_sheets_resource",
]
