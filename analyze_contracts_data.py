"""
Analyze contracts data structure
"""
import pandas as pd
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup

# Database connection
DB_URL = 'postgresql://postgres:St0ck!adePG@localhost:5432/engineering'
engine = create_engine(DB_URL)

# Get contracts data
query = '''
SELECT 
    id,
    timestamp,
    value->>'raw_html' as raw_html
FROM bronze.emma_public_contracts
ORDER BY timestamp DESC
LIMIT 1;
'''

df = pd.read_sql(query, engine)
if len(df) > 0:
    print(f'Found {len(df)} contract records')
    html = df.iloc[0]['raw_html']
    print(f'HTML length: {len(html)}')
    
    # Parse HTML
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')
    print(f'Total tables found: {len(tables)}')
    
    # Look for the main data table
    for i, table in enumerate(tables):
        rows = table.find_all('tr')
        if len(rows) > 10:  # Tables with substantial data
            headers = [cell.get_text(strip=True) for cell in rows[0].find_all(['th', 'td'])]
            print(f'\n=== TABLE {i+1} (has {len(rows)} rows) ===')
            print(f'Headers: {headers[:8]}...' if len(headers) > 8 else f'Headers: {headers}')
            
            # Check if this looks like contracts data
            if any(keyword in ' '.join(headers).lower() for keyword in ['contract', 'award', 'vendor', 'purchase']):
                print('  -> This appears to be the contracts table!')
                
                # Show first data row
                if len(rows) > 1:
                    first_row = [cell.get_text(strip=True) for cell in rows[1].find_all(['td', 'th'])]
                    print(f'  -> First data row: {first_row[:5]}...')
                    
                # Extract all data
                contracts_data = []
                for row_idx, row in enumerate(rows[1:], 1):
                    cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
                    if cells and any(cell.strip() for cell in cells):
                        record = {headers[j]: cells[j] if j < len(cells) else '' for j in range(len(headers))}
                        contracts_data.append(record)
                
                # Create DataFrame
                df_contracts = pd.DataFrame(contracts_data)
                print(f'\nExtracted {len(df_contracts)} contract records')
                print(f'Columns: {list(df_contracts.columns)[:10]}...')
                print('\nFirst 3 contracts:')
                print(df_contracts.head(3))
                break
    
    # Also check table 11 specifically (like solicitations)
    if len(tables) > 10:
        print(f'\n=== Checking Table 11 specifically ===')
        table_11 = tables[10]
        rows_11 = table_11.find_all('tr')
        if rows_11:
            headers_11 = [cell.get_text(strip=True) for cell in rows_11[0].find_all(['th', 'td'])]
            print(f'Table 11 has {len(rows_11)} rows')
            print(f'Headers: {headers_11[:8]}...' if len(headers_11) > 8 else f'Headers: {headers_11}')
            
else:
    print('No contracts data found in database')