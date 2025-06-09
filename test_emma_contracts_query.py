"""
Test script to query emma_public_contracts table
"""
import os
from sqlalchemy import create_engine, text
from datetime import datetime

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:St0ck!adePG@localhost:5432/engineering")

def test_query_emma_contracts():
    """Query the emma_public_contracts table to verify data exists"""
    engine = create_engine(DATABASE_URL)
    
    try:
        with engine.connect() as conn:
            # First check if table exists
            table_exists = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'bronze' 
                    AND table_name = 'emma_public_contracts'
                );
            """)).scalar()
            
            print(f"Table engineering.bronze.emma_public_contracts exists: {table_exists}")
            
            if table_exists:
                # Count records
                count = conn.execute(text("""
                    SELECT COUNT(*) FROM bronze.emma_public_contracts;
                """)).scalar()
                
                print(f"Total records in emma_public_contracts: {count}")
                
                # Get latest record
                result = conn.execute(text("""
                    SELECT id, timestamp, created_at, 
                           value->>'url' as url,
                           value->>'body_length' as body_length
                    FROM bronze.emma_public_contracts
                    ORDER BY created_at DESC
                    LIMIT 1;
                """)).fetchone()
                
                if result:
                    print("\nLatest record:")
                    print(f"  ID: {result.id}")
                    print(f"  Timestamp: {result.timestamp}")
                    print(f"  Created at: {result.created_at}")
                    print(f"  URL: {result.url}")
                    print(f"  Body length: {result.body_length}")
                else:
                    print("\nNo records found in the table yet.")
                    
                # Check raw HTML content (first 200 chars)
                if count > 0:
                    html_preview = conn.execute(text("""
                        SELECT LEFT(value->>'raw_html', 200) as html_preview
                        FROM bronze.emma_public_contracts
                        ORDER BY created_at DESC
                        LIMIT 1;
                    """)).scalar()
                    
                    print(f"\nHTML preview (first 200 chars):")
                    print(html_preview)
                    
            else:
                print("\nTable doesn't exist yet. Run the Dagster asset first.")
                
    except Exception as e:
        print(f"Error querying database: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    test_query_emma_contracts()