"""
Test script to verify Gold layer data
"""
import os
from sqlalchemy import create_engine, text
import json

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:St0ck!adePG@localhost:5432/engineering")

def test_gold_layer():
    """Test the Gold layer data"""
    engine = create_engine(DATABASE_URL)
    
    print("=== TESTING GOLD LAYER ===\n")
    
    try:
        with engine.connect() as conn:
            # Check if gold table exists
            print("1. CHECKING GOLD TABLE:")
            gold_table_exists = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'gold' 
                    AND table_name = 'emma_solicitations_gold'
                );
            """)).scalar()
            
            print(f"   Gold table exists: {gold_table_exists}")
            
            if gold_table_exists:
                # Count gold records
                gold_count = conn.execute(text("""
                    SELECT COUNT(*) as count 
                    FROM gold.emma_solicitations_gold;
                """)).scalar()
                
                print(f"   Gold records: {gold_count}")
                
                if gold_count > 0:
                    # Show the gold record
                    print("\n2. GOLD RECORD DATA:")
                    gold_data = conn.execute(text("""
                        SELECT 
                            solicitation_id,
                            title,
                            bmp_id,
                            alternate_id,
                            status,
                            due_date,
                            issuing_agency,
                            procurement_officer,
                            email,
                            mbe_participation_pct,
                            women_owned_pct,
                            solicitation_links,
                            attachments,
                            detail_url,
                            processed_at
                        FROM gold.emma_solicitations_gold
                        ORDER BY processed_at DESC
                        LIMIT 1;
                    """)).fetchone()
                    
                    if gold_data:
                        print(f"   Solicitation ID: {gold_data.solicitation_id}")
                        print(f"   Title: {gold_data.title}")
                        print(f"   BMP ID: {gold_data.bmp_id}")
                        print(f"   Alternate ID: {gold_data.alternate_id}")
                        print(f"   Status: {gold_data.status}")
                        print(f"   Due Date: {gold_data.due_date}")
                        print(f"   Agency: {gold_data.issuing_agency}")
                        print(f"   Officer: {gold_data.procurement_officer}")
                        print(f"   Email: {gold_data.email}")
                        print(f"   MBE %: {gold_data.mbe_participation_pct}")
                        print(f"   Women Owned %: {gold_data.women_owned_pct}")
                        print(f"   Detail URL: {gold_data.detail_url}")
                        print(f"   Processed At: {gold_data.processed_at}")
                        
                        # Parse and display JSON fields
                        if gold_data.solicitation_links:
                            try:
                                links = json.loads(gold_data.solicitation_links)
                                print(f"\n   Navigation Links ({len(links)}):")
                                for i, link in enumerate(links[:3]):
                                    print(f"     {i+1}. {link.get('text', '')} -> {link.get('url', '')} ({link.get('type', '')})")
                            except:
                                print(f"   Links: {gold_data.solicitation_links[:100]}...")
                        
                        if gold_data.attachments:
                            try:
                                attachments = json.loads(gold_data.attachments)
                                print(f"\n   Attachments ({len(attachments)}):")
                                for i, att in enumerate(attachments[:3]):
                                    print(f"     {i+1}. {att.get('title', '')} ({att.get('type', '')}) -> {att.get('url', '')}")
                            except:
                                print(f"   Attachments: {gold_data.attachments[:100]}...")
                    
                    print("\n3. VALIDATION:")
                    print("   ✅ Gold table created")
                    print("   ✅ Record processed successfully")
                    
                    # Count non-null fields
                    non_null_count = sum(1 for field in [
                        gold_data.title, gold_data.bmp_id, gold_data.status, 
                        gold_data.due_date, gold_data.issuing_agency, 
                        gold_data.procurement_officer, gold_data.email,
                        gold_data.mbe_participation_pct, gold_data.women_owned_pct
                    ] if field)
                    
                    print(f"   ✅ Extracted {non_null_count}/9 main fields")
                    
                    if gold_data.solicitation_links:
                        print("   ✅ Navigation links extracted")
                    if gold_data.attachments:
                        print("   ✅ Attachments extracted")
                    
                else:
                    print("   ❌ No gold records found. Run emma_solicitations_gold asset.")
            else:
                print("   ❌ Gold table not created. Run emma_solicitations_gold asset.")
                
    except Exception as e:
        print(f"❌ Error testing gold layer: {e}")
        import traceback
        traceback.print_exc()
    finally:
        engine.dispose()

if __name__ == "__main__":
    test_gold_layer()