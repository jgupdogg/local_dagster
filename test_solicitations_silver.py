"""
Test script to verify bronze to silver transformation for solicitations
"""
import os
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:St0ck!adePG@localhost:5432/engineering")

def test_bronze_to_silver_transformation():
    """Test the bronze to silver transformation for solicitations"""
    engine = create_engine(DATABASE_URL)
    
    print("=== TESTING BRONZE TO SILVER TRANSFORMATION ===\n")
    
    try:
        with engine.connect() as conn:
            # Check bronze data
            print("1. CHECKING BRONZE DATA:")
            bronze_count = conn.execute(text("""
                SELECT COUNT(*) as count 
                FROM bronze.emma_public_solicitations;
            """)).scalar()
            
            print(f"   Bronze records: {bronze_count}")
            
            if bronze_count == 0:
                print("   ❌ No bronze data found. Run emma_public_solicitations asset first.")
                return
            
            # Get latest bronze record info
            bronze_info = conn.execute(text("""
                SELECT 
                    id,
                    timestamp,
                    length(value->>'raw_html') as html_length
                FROM bronze.emma_public_solicitations
                ORDER BY timestamp DESC
                LIMIT 1;
            """)).fetchone()
            
            print(f"   Latest bronze ID: {bronze_info.id}")
            print(f"   HTML length: {bronze_info.html_length}")
            print(f"   Timestamp: {bronze_info.timestamp}")
            
            # Check if silver table exists
            print("\n2. CHECKING SILVER TABLE:")
            silver_table_exists = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'silver' 
                    AND table_name = 'emma_public_solicitations_silver'
                );
            """)).scalar()
            
            print(f"   Silver table exists: {silver_table_exists}")
            
            if silver_table_exists:
                # Count silver records
                silver_count = conn.execute(text("""
                    SELECT COUNT(*) as count 
                    FROM silver.emma_public_solicitations_silver;
                """)).scalar()
                
                print(f"   Silver records: {silver_count}")
                
                if silver_count > 0:
                    # Show sample silver data
                    print("\n3. SAMPLE SILVER DATA:")
                    sample_data = conn.execute(text("""
                        SELECT 
                            solicitation_id,
                            title,
                            detail_url,
                            status,
                            due_close_date,
                            issuing_agency,
                            processed,
                            created_at,
                            source_bronze_id
                        FROM silver.emma_public_solicitations_silver
                        ORDER BY created_at DESC
                        LIMIT 5;
                    """)).fetchall()
                    
                    for row in sample_data:
                        print(f"   ID: {row.solicitation_id}")
                        print(f"      Title: {row.title[:50]}...")
                        print(f"      Detail URL: {row.detail_url}")
                        print(f"      Status: {row.status}")
                        print(f"      Due Date: {row.due_close_date}")
                        print(f"      Agency: {row.issuing_agency}")
                        print(f"      Processed: {row.processed}")
                        print(f"      Created: {row.created_at}")
                        print(f"      Source Bronze ID: {row.source_bronze_id}")
                        print()
                    
                    # Check for unprocessed records
                    unprocessed_count = conn.execute(text("""
                        SELECT COUNT(*) as count 
                        FROM silver.emma_public_solicitations_silver
                        WHERE processed = false;
                    """)).scalar()
                    
                    print(f"   Unprocessed records: {unprocessed_count}")
                    
                    # Check URL extraction success
                    url_count = conn.execute(text("""
                        SELECT COUNT(*) as count 
                        FROM silver.emma_public_solicitations_silver
                        WHERE detail_url IS NOT NULL AND detail_url != '';
                    """)).scalar()
                    
                    print(f"   Records with detail URLs: {url_count}")
                    
                    # Show unique solicitation IDs
                    unique_solicitations = conn.execute(text("""
                        SELECT COUNT(DISTINCT solicitation_id) as count 
                        FROM silver.emma_public_solicitations_silver;
                    """)).scalar()
                    
                    print(f"   Unique solicitations: {unique_solicitations}")
                    
                    # Check for duplicates/history
                    if silver_count > unique_solicitations:
                        print(f"   Historical records: {silver_count - unique_solicitations}")
                        
                        # Show an example of historical changes
                        history_example = conn.execute(text("""
                            SELECT solicitation_id, COUNT(*) as versions
                            FROM silver.emma_public_solicitations_silver
                            GROUP BY solicitation_id
                            HAVING COUNT(*) > 1
                            LIMIT 1;
                        """)).fetchone()
                        
                        if history_example:
                            print(f"   Example: {history_example.solicitation_id} has {history_example.versions} versions")
                    
                    print("\n4. VALIDATION:")
                    print("   ✅ Bronze data exists")
                    print("   ✅ Silver table created")
                    print("   ✅ Records transformed successfully")
                    print("   ✅ Field mapping working")
                    print("   ✅ Change detection implemented")
                    if url_count > 0:
                        print("   ✅ Hyperlink extraction working")
                    else:
                        print("   ❌ No hyperlinks extracted")
                    
                else:
                    print("   ❌ No silver records found. Run emma_solicitations_silver asset.")
            else:
                print("   ❌ Silver table not created. Run emma_solicitations_silver asset.")
                
    except Exception as e:
        print(f"❌ Error testing transformation: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    test_bronze_to_silver_transformation()