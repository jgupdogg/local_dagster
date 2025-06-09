#!/usr/bin/env python3
"""
Analyze EMMA data from the database
This script simulates the notebook analysis for contracts data
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get DATABASE_URL
DB_URL = os.getenv('DATABASE_URL')
if not DB_URL:
    print('ERROR: No DATABASE_URL environment variable found')
    exit(1)

# Create SQLAlchemy engine
engine = create_engine(DB_URL)

print("=== EMMA Data Analysis ===\n")

# First, check what tables exist
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE '%emma%'
        ORDER BY table_schema, table_name;
    """))
    tables = result.fetchall()
    
    print("Available EMMA tables:")
    for schema, table in tables:
        print(f"  - {schema}.{table}")
    print()

# Check if contracts table exists
contracts_table_exists = any(table == 'emma_public_contracts' for _, table in tables)

if not contracts_table_exists:
    print("NOTE: emma_public_contracts table does not exist yet.")
    print("This would need to be created by adding a new asset to the engineering pipeline.\n")
    print("For demonstration, let's analyze the solicitations data instead:\n")
    
    # Get solicitations data
    query_html = """
    SELECT 
        id,
        timestamp,
        value->>'raw_html' as raw_html
    FROM bronze.emma_public_solicitations
    ORDER BY timestamp DESC
    LIMIT 1;
    """
    
    df_html = pd.read_sql(query_html, engine)
    
    if len(df_html) > 0:
        raw_html = df_html.iloc[0]['raw_html']
        print(f"Retrieved HTML content of length: {len(raw_html)}")
        print(f"Timestamp: {df_html.iloc[0]['timestamp']}")
        
        # Analyze HTML structure
        soup = BeautifulSoup(raw_html, 'html.parser')
        tables = soup.find_all('table')
        
        print(f"\nTotal tables found: {len(tables)}")
        
        # Find the main data table (table 11)
        if len(tables) >= 11:
            target_table = tables[10]  # Table 11 (0-indexed)
            rows = target_table.find_all('tr')
            
            print(f"\nTable 11 (main data table):")
            print(f"  - Total rows: {len(rows)}")
            
            if len(rows) > 0:
                headers = [cell.get_text(strip=True) for cell in rows[0].find_all(['th', 'td'])]
                print(f"  - Headers ({len(headers)}): {headers[:5]}...")
                
                # Count data rows
                data_rows = 0
                for row in rows[1:]:
                    cells = [cell.get_text(strip=True) for cell in row.find_all(['td', 'th'])]
                    if any(cell.strip() for cell in cells):
                        data_rows += 1
                
                print(f"  - Data rows: {data_rows}")
                
                # Show sample data
                if len(rows) > 1:
                    first_data = [cell.get_text(strip=True) for cell in rows[1].find_all(['td', 'th'])]
                    print(f"  - First record: {first_data[:3]}...")
    else:
        print("No solicitations data found in database")
        
else:
    print("emma_public_contracts table exists! Analyzing contracts data...\n")
    
    # This code would run if the contracts table existed
    query_contracts = """
    SELECT 
        id,
        timestamp,
        value->>'raw_html' as raw_html
    FROM bronze.emma_public_contracts
    ORDER BY timestamp DESC
    LIMIT 1;
    """
    
    df_contracts = pd.read_sql(query_contracts, engine)
    
    if len(df_contracts) > 0:
        contracts_html = df_contracts.iloc[0]['raw_html']
        print(f"Retrieved contracts HTML content of length: {len(contracts_html)}")
        
        # Analyze structure
        soup = BeautifulSoup(contracts_html, 'html.parser')
        tables = soup.find_all('table')
        
        print(f"\nTotal tables in contracts page: {len(tables)}")
        
        # Look for the main contracts table
        for i, table in enumerate(tables):
            rows = table.find_all('tr')
            if rows and len(rows) > 10:
                headers = [cell.get_text(strip=True) for cell in rows[0].find_all(['th', 'td'])]
                if any('contract' in h.lower() or 'award' in h.lower() for h in headers):
                    print(f"\nFound contracts table at position {i+1}:")
                    print(f"  - Headers: {headers}")
                    print(f"  - Total rows: {len(rows)}")
                    break

print("\n=== Summary ===")
print("To analyze contracts data, you would need to:")
print("1. Create a new asset 'emma_public_contracts' in the engineering pipeline")
print("2. Update the asset to scrape the contracts page URL")
print("3. Run the pipeline to populate the database")
print("4. Then the notebook cells 6-8 would work properly")