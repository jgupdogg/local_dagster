"""
Check if hyperlinks exist in bronze HTML data
"""
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup

# Use the same credentials as the engineering pipeline
DATABASE_URL = "postgresql://postgres:St0ck!adePG@localhost:5432/engineering"

def check_links_in_bronze():
    """Check if hyperlinks are present in the bronze HTML data"""
    engine = create_engine(DATABASE_URL)
    
    try:
        # Get the latest raw HTML from bronze
        query = '''
        SELECT value->>'raw_html' as raw_html
        FROM bronze.emma_public_solicitations
        ORDER BY timestamp DESC
        LIMIT 1;
        '''
        
        df = pd.read_sql(query, engine)
        html = df.iloc[0]['raw_html']
        
        print(f"HTML length: {len(html)}")
        
        # Parse and check for links in table 11
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table')
        
        print(f"Found {len(tables)} tables")
        
        if len(tables) >= 11:
            table_11 = tables[10]
            rows = table_11.find_all('tr')
            
            print(f'\n=== CHECKING TABLE 11 FOR LINKS ===')
            print(f'Table has {len(rows)} rows')
            
            # Get headers
            if rows:
                headers = [cell.get_text(strip=True) for cell in rows[0].find_all(['th', 'td'])]
                print(f'Headers: {headers[:5]}...')
            
            # Check first few data rows for links
            links_found = 0
            for i, row in enumerate(rows[1:6], 1):  # Check first 5 data rows
                cells = row.find_all('td')
                print(f'\n--- Row {i} ---')
                
                for j, cell in enumerate(cells[:5]):  # Check first 5 cells
                    links = cell.find_all('a')
                    if links:
                        for link in links:
                            href = link.get('href', '')
                            text = link.get_text(strip=True)
                            print(f'  Cell {j} ({headers[j] if j < len(headers) else "Unknown"}): LINK FOUND!')
                            print(f'    Text: "{text}"')
                            print(f'    URL: {href}')
                            links_found += 1
                    else:
                        text = cell.get_text(strip=True)
                        cell_name = headers[j] if j < len(headers) else f"Cell {j}"
                        print(f'  {cell_name}: "{text[:40]}..." (no links)')
                        
                        # Let's also check the raw HTML for this cell
                        cell_html = str(cell)
                        if '<a' in cell_html:
                            print(f'    ⚠️  Found <a> tag in HTML: {cell_html[:100]}...')
            
            print(f'\n=== SUMMARY ===')
            print(f'Total links found: {links_found}')
            
            # Let's also search the entire table for any <a> tags
            table_html = str(table_11)
            a_tags = table_11.find_all('a')
            print(f'Total <a> tags in table 11: {len(a_tags)}')
            
            if a_tags:
                print('\nFirst few <a> tags found:')
                for i, a_tag in enumerate(a_tags[:3]):
                    print(f'  {i+1}: {str(a_tag)[:100]}...')
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    check_links_in_bronze()