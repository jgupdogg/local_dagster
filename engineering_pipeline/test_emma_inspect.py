#!/usr/bin/env python3
"""
Test to inspect the EMMA page for data loading patterns
"""
import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
import sys
sys.path.append('/home/jgupdogg/dev/dagster')

import nodriver as uc


async def inspect_emma_page():
    """Inspect the EMMA page for how data is loaded"""
    
    browser = await uc.start(
        headless=False,
        browser_args=['--no-sandbox', '--disable-setuid-sandbox']
    )
    
    try:
        tab = await browser.get('about:blank')
        
        print(f"Navigating to EMMA page at {datetime.now()}")
        await tab.get("https://emma.maryland.gov/page.aspx/en/rfp/request_browse_public")
        
        # Wait for initial load
        print("Waiting for initial page load...")
        await asyncio.sleep(5)
        
        # Check page title
        title = await tab.evaluate('document.title')
        print(f"Page title: {title}")
        
        # Look for data on the page
        print("\n=== Checking page content ===")
        
        # Check if there are tables with solicitation data
        tables = await tab.evaluate('document.querySelectorAll("table").length')
        print(f"Number of tables: {tables}")
        
        # Check for specific elements that might contain solicitation data
        elements_to_check = [
            'table tbody tr',
            '.solicitation',
            '.rfp',
            '[data-solicitation]',
            '.data-table',
            '.results'
        ]
        
        for selector in elements_to_check:
            try:
                count = await tab.evaluate(f'document.querySelectorAll("{selector}").length')
                if count > 0:
                    print(f"Found {count} elements matching '{selector}'")
                    
                    # Get some sample content
                    sample_text = await tab.evaluate(f'''
                        Array.from(document.querySelectorAll("{selector}"))
                            .slice(0, 3)
                            .map(el => el.textContent.trim().substring(0, 100))
                    ''')
                    print(f"Sample content: {sample_text}")
            except Exception as e:
                print(f"Error checking {selector}: {e}")
        
        # Check for JavaScript variables that might contain data
        print("\n=== Checking for JavaScript data ===")
        js_checks = [
            'window.solicitations',
            'window.rfpData', 
            'window.publicData',
            'window.gridData',
            'typeof DataTables',
            'typeof jQuery'
        ]
        
        for check in js_checks:
            try:
                result = await tab.evaluate(check)
                print(f"{check}: {result}")
            except Exception as e:
                print(f"Error checking {check}: {e}")
        
        # Look for any AJAX/fetch calls in the page source
        print("\n=== Checking page source for API patterns ===")
        page_source = await tab.get_content()
        
        api_patterns = ['fetch(', 'XMLHttpRequest', '$.ajax', '$.get', '$.post', 'api/', '/service/']
        for pattern in api_patterns:
            if pattern in page_source:
                print(f"Found pattern '{pattern}' in page source")
        
        # Try to find forms that might submit data
        forms = await tab.evaluate('document.querySelectorAll("form").length')
        print(f"\nNumber of forms: {forms}")
        
        if forms > 0:
            form_actions = await tab.evaluate('''
                Array.from(document.querySelectorAll("form"))
                    .map(f => ({action: f.action, method: f.method}))
            ''')
            print(f"Form actions: {form_actions}")
        
        # Try to find any buttons that might trigger data loading
        buttons = await tab.evaluate('''
            Array.from(document.querySelectorAll("button, input[type=submit], .btn"))
                .map(b => ({text: b.textContent.trim(), type: b.type, id: b.id, class: b.className}))
                .filter(b => b.text)
        ''')
        print(f"\nButtons found: {buttons}")
        
        # Save page source for manual inspection
        output_dir = Path("/home/jgupdogg/dev/dagster/engineering_pipeline/data")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        source_file = output_dir / f"emma_page_source_{timestamp}.html"
        
        with open(source_file, 'w', encoding='utf-8') as f:
            f.write(page_source)
        
        print(f"\nSaved page source to: {source_file}")
        
        # Keep browser open for manual inspection
        print("\n=== Browser will stay open for 30 seconds for manual inspection ===")
        await asyncio.sleep(30)
        
    finally:
        try:
            if browser:
                await browser.stop()
        except Exception as e:
            print(f"Error stopping browser: {e}")


if __name__ == "__main__":
    asyncio.run(inspect_emma_page())