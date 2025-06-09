#!/usr/bin/env python3
"""
Script to drop and recreate the Gold table with new columns
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from engineering_pipeline.models.engineering_models import EmmaSolicitationsGold
from sqlalchemy import create_engine, text

def update_schema():
    """Drop and recreate the Gold table with new schema"""
    
    # Database connection
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:St0ck!adePG@localhost:5432/engineering")
    engine = create_engine(DATABASE_URL)
    
    try:
        with engine.connect() as conn:
            # Drop existing table
            print("Dropping existing Gold table...")
            conn.execute(text("DROP TABLE IF EXISTS gold.emma_solicitations_gold CASCADE"))
            conn.commit()
            
            # Create new table with updated schema
            print("Creating new Gold table with updated schema...")
            EmmaSolicitationsGold.metadata.create_all(engine)
            
            print("✅ Gold table schema updated successfully!")
            
    except Exception as e:
        print(f"❌ Error updating schema: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        engine.dispose()

if __name__ == "__main__":
    update_schema()