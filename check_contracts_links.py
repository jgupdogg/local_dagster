"""
Check if hyperlinks exist in contracts bronze HTML data
"""
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup

# Use the same credentials as the engineering pipeline
DATABASE_URL = "postgresql://postgres:St0ck!adePG@localhost:5432/engineering"

def check_contracts_links():
    """Check if hyperlinks are present in the contracts bronze HTML data"""
    engine = create_engine(DATABASE_URL)
    
    try:
        # Get the latest raw HTML from contracts bronze
        query = '''
        SELECT value->>'raw_html' as raw_html
        FROM bronze.emma_public_contracts
        ORDER BY timestamp DESC
        LIMIT 1;
        '''
        
        df = pd.read_sql(query, engine)
        html = df.iloc[0]['raw_html']
        
        print(f"Contracts HTML length: {len(html)}")
        
        # Parse and check for links in table 11 (the contracts table)
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table')
        
        print(f"Found {len(tables)} tables")
        
        if len(tables) >= 11:
            table_11 = tables[10]  # Table 11 (0-indexed)
            rows = table_11.find_all('tr')
            
            print(f'\n=== CHECKING CONTRACTS TABLE (Table 11) FOR LINKS ===')
            print(f'Table has {len(rows)} rows')
            
            # Get headers
            if rows:
                headers = [cell.get_text(strip=True) for cell in rows[0].find_all(['th', 'td'])]
                print(f'Headers: {headers}')
            
            # Check first few data rows for links
            links_found = 0
            for i, row in enumerate(rows[1:6], 1):  # Check first 5 data rows
                cells = row.find_all('td')
                print(f'\n--- Row {i} ---')
                
                for j, cell in enumerate(cells):
                    if j >= len(headers):
                        break
                    
                    links = cell.find_all('a')
                    if links:
                        for link in links:
                            href = link.get('href', '')
                            text = link.get_text(strip=True)
                            print(f'  {headers[j]}: LINK FOUND!')
                            print(f'    Text: "{text}"')
                            print(f'    URL: {href}')
                            links_found += 1
                            
                            # Check if this matches expected contract URL pattern
                            if '/ctr/contract_manage_public/' in href:
                                print(f'    ✅ Matches expected contract URL pattern!')
                    else:
                        text = cell.get_text(strip=True)
                        print(f'  {headers[j]}: "{text[:40]}..." (no links)')
            
            print(f'\n=== SUMMARY ===')
            print(f'Total links found: {links_found}')
            
            # Search specifically for contract_manage_public links
            contract_links = table_11.find_all('a', href=lambda x: x and 'contract_manage_public' in x)
            print(f'Contract management links found: {len(contract_links)}')
            
            if contract_links:
                print('\nFirst few contract links:')
                for i, link in enumerate(contract_links[:3]):
                    href = link.get('href', '')
                    text = link.get_text(strip=True)
                    print(f'  {i+1}: "{text}" -> {href}')
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    check_contracts_links()