"""
Test script to analyze the structure of the solicitation detail page
"""
import asyncio
from common.resources.unified_scraper import UnifiedScraperResource
from bs4 import BeautifulSoup
import json
import re

async def test_solicitation_scrape():
    """Test scraping the specific solicitation detail URL"""
    
    # Test URL
    test_url = "https://emma.maryland.gov/page.aspx/en/bpm/process_manage_extranet/81788"
    
    # Initialize scraper
    scraper = UnifiedScraperResource(
        headless=False,
        visible=True,
        no_sandbox=True,
        timeout=60
    )
    
    try:
        print(f"Testing URL: {test_url}")
        
        # Setup scraper
        await scraper.setup_resource(None)
        
        # Get page content
        async def scrape_page():
            client = scraper.client
            
            # Get a new tab
            tab = await asyncio.wait_for(client.browser.get('about:blank'), timeout=10)
            
            try:
                # Navigate to the URL
                print(f"Navigating to {test_url}")
                await asyncio.wait_for(tab.get(test_url), timeout=30)
                
                # Wait for page to load
                print("Waiting for page to load...")
                await asyncio.sleep(8)
                
                # Get the page content
                page_content = await tab.get_content()
                return page_content
                
            finally:
                if tab and tab != client.main_tab:
                    try:
                        await asyncio.wait_for(tab.close(), timeout=5)
                    except Exception as e:
                        print(f"Error closing tab: {e}")
        
        # Run the scraping
        html_content = await scrape_page()
        
        if not html_content:
            print("❌ Failed to get page content")
            return
        
        print(f"✅ Successfully retrieved page content ({len(html_content)} characters)")
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Analyze page structure
        print("\n=== PAGE STRUCTURE ANALYSIS ===")
        
        # Look for title
        title_candidates = [
            soup.find('title'),
            soup.find('h1'),
            soup.find('h2'),
            soup.find('h3')
        ]
        
        for i, candidate in enumerate(title_candidates):
            if candidate:
                print(f"Title candidate {i}: {candidate.get_text(strip=True)[:100]}")
        
        # Look for the specific data context IDs mentioned
        print("\n=== SEARCHING FOR SPECIFIC CONTEXT IDs ===")
        
        # Links section
        links_section = soup.find(attrs={"data-context-id": "body_x_tabc_rfp_ext_prxrfp_ext_x_placeholder_rfp_160831232210"})
        if links_section:
            print("✅ Found links section!")
            links = links_section.find_all('a')
            print(f"   Found {len(links)} links")
            for i, link in enumerate(links[:3]):
                href = link.get('href', '')
                text = link.get_text(strip=True)
                print(f"   Link {i+1}: '{text}' -> {href}")
        else:
            print("❌ Links section not found")
        
        # Attachments section
        attachments_section = soup.find(attrs={"data-context-id": "body_x_tabc_rfp_ext_prxrfp_ext_x_phcDoc"})
        if attachments_section:
            print("✅ Found attachments section!")
            attachments = attachments_section.find_all('a')
            print(f"   Found {len(attachments)} attachment links")
            for i, attachment in enumerate(attachments[:3]):
                href = attachment.get('href', '')
                text = attachment.get_text(strip=True)
                print(f"   Attachment {i+1}: '{text}' -> {href}")
        else:
            print("❌ Attachments section not found")
        
        # Look for common field patterns
        print("\n=== SEARCHING FOR DATA FIELDS ===")
        
        # Try to find fields by common patterns
        patterns_to_search = [
            ('BMP', r'BMP[#\s]*(\w+)'),
            ('Due Date', r'Due\s*Date[:\s]*([^\n]+)'),
            ('Status', r'Status[:\s]*([^\n]+)'),
            ('Agency', r'Agency[:\s]*([^\n]+)'),
            ('Officer', r'Officer[:\s]*([^\n]+)'),
            ('Email', r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'),
            ('MBE', r'MBE[:\s]*(\d+%?)'),
            ('Pre.*bid', r'Pre.*bid[:\s]*([^\n]+)')
        ]
        
        page_text = soup.get_text()
        for pattern_name, pattern in patterns_to_search:
            matches = re.findall(pattern, page_text, re.IGNORECASE | re.MULTILINE)
            if matches:
                print(f"✅ Found {pattern_name}: {matches[:3]}")
        
        # Look for tables
        tables = soup.find_all('table')
        if tables:
            print(f"\n=== FOUND {len(tables)} TABLES ===")
            for i, table in enumerate(tables[:5]):
                rows = table.find_all('tr')
                print(f"Table {i+1}: {len(rows)} rows")
                if rows:
                    first_row_text = rows[0].get_text(strip=True)[:50]
                    print(f"   First row: {first_row_text}...")
        
        # Save a sample of the HTML for manual inspection
        sample_html = html_content[:5000] + "\n...\n" + html_content[-2000:]
        with open('solicitation_page_sample.html', 'w', encoding='utf-8') as f:
            f.write(sample_html)
        print(f"\n💾 Saved HTML sample to 'solicitation_page_sample.html'")
        
        print("\n✅ Page analysis complete!")
        
    except Exception as e:
        print(f"❌ Error during scraping: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up
        try:
            await scraper.teardown_resource(None)
        except:
            pass

if __name__ == "__main__":
    asyncio.run(test_solicitation_scrape())