"""
Common resources shared across pipelines
"""
from common.resources.database import DatabaseResource, db_resource
from .unified_scraper import UnifiedScraperResource

__all__ = [
    "DatabaseResource",
    "db_resource",
    "UnifiedScraperResource",
]
