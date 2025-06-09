#!/usr/bin/env python3
"""
Test script for EMMA Contracts Gold layer
"""
import os
import sys
import logging
from sqlalchemy import create_engine, text
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection
DATABASE_URL = "postgresql://postgres:St0ck!adePG@localhost:5432/engineering"
engine = create_engine(DATABASE_URL)

def test_contracts_gold_layer():
    """Test the Contracts Gold layer functionality"""
    
    print("=== TESTING CONTRACTS GOLD LAYER ===")
    print()
    
    try:
        with engine.connect() as conn:
            # 1. Check if Gold table exists
            table_exists_query = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'gold' 
                    AND table_name = 'emma_contracts_gold'
                );
            """)
            
            table_exists = conn.execute(table_exists_query).fetchone()[0]
            print(f"1. CHECKING GOLD TABLE:")
            print(f"   Gold table exists: {table_exists}")
            
            if not table_exists:
                print("   ❌ Gold table does not exist!")
                return
            
            # 2. Check records in Gold table
            count_query = text("SELECT COUNT(*) FROM gold.emma_contracts_gold;")
            record_count = conn.execute(count_query).fetchone()[0]
            print(f"   Gold records: {record_count}")
            print()
            
            # 3. Show contract data if exists
            if record_count > 0:
                data_query = text("""
                    SELECT 
                        contract_id,
                        contract_title,
                        contract_type,
                        vendor_name,
                        contract_amount,
                        currency,
                        effective_date,
                        expiration_date,
                        procurement_officer,
                        contact_email,
                        agency_org,
                        vsbe_goal_percentage,
                        detail_url,
                        processed_at
                    FROM gold.emma_contracts_gold 
                    ORDER BY processed_at DESC 
                    LIMIT 1;
                """)
                
                result = conn.execute(data_query).fetchone()
                if result:
                    print("2. GOLD RECORD DATA:")
                    print(f"   Contract ID: {result[0]}")
                    print(f"   Title: {result[1]}")
                    print(f"   Type: {result[2]}")
                    print(f"   Vendor: {result[3]}")
                    print(f"   Amount: {result[4]} {result[5]}")
                    print(f"   Effective: {result[6]}")
                    print(f"   Expires: {result[7]}")
                    print(f"   Officer: {result[8]}")
                    print(f"   Email: {result[9]}")
                    print(f"   Agency: {result[10]}")
                    print(f"   VSBE Goal: {result[11]}%")
                    print(f"   Detail URL: {result[12]}")
                    print(f"   Processed At: {result[13]}")
                    print()
                
                # 4. Field extraction validation
                validation_query = text("""
                    SELECT 
                        CASE WHEN contract_title IS NOT NULL THEN 1 ELSE 0 END +
                        CASE WHEN vendor_name IS NOT NULL THEN 1 ELSE 0 END +
                        CASE WHEN contract_amount IS NOT NULL THEN 1 ELSE 0 END +
                        CASE WHEN effective_date IS NOT NULL THEN 1 ELSE 0 END +
                        CASE WHEN expiration_date IS NOT NULL THEN 1 ELSE 0 END +
                        CASE WHEN procurement_officer IS NOT NULL THEN 1 ELSE 0 END +
                        CASE WHEN contact_email IS NOT NULL THEN 1 ELSE 0 END +
                        CASE WHEN agency_org IS NOT NULL THEN 1 ELSE 0 END +
                        CASE WHEN contract_type IS NOT NULL THEN 1 ELSE 0 END as extracted_fields
                    FROM gold.emma_contracts_gold 
                    ORDER BY processed_at DESC 
                    LIMIT 1;
                """)
                
                extracted_count = conn.execute(validation_query).fetchone()[0]
                
                print("3. VALIDATION:")
                print(f"   ✅ Gold table created")
                if record_count > 0:
                    print(f"   ✅ Record processed successfully")
                    print(f"   ✅ Extracted {extracted_count}/9 main fields")
                else:
                    print(f"   ❌ No records processed")
                    
            else:
                print("2. GOLD RECORD DATA:")
                print("   No records found in Gold table")
                print()
                print("3. VALIDATION:")
                print(f"   ✅ Gold table created")
                print(f"   ❌ No records processed")
                
    except Exception as e:
        print(f"❌ Error testing Gold layer: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_contracts_gold_layer()