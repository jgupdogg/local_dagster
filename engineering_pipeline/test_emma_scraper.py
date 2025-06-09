"""
Test script for EMMA scraper - run independently to debug
"""
import json
import time
from datetime import datetime
from pathlib import Path
import asyncio
import sys
sys.path.append('/home/jgupdogg/dev/dagster')

from common.resources.unified_scraper import UnifiedScraperResource


async def test_emma_scraper():
    """Test the EMMA scraper with different approaches"""
    
    # Initialize scraper with visible display
    scraper = UnifiedScraperResource(
        headless=False,
        visible=True,
        no_sandbox=True,
        timeout=60,
    )
    
    # Setup the scraper client
    await scraper.client.setup()
    
    base_url = "https://emma.maryland.gov/page.aspx/en/rfp/request_browse_public"
    
    print(f"Testing EMMA scraper at {datetime.now()}")
    print(f"URL: {base_url}")
    
    # Try different URL patterns
    patterns_to_try = [
        "request_browse_public",
        ".*request_browse_public.*",
        "RFPService",
        "api",
        "GetPublicRFPs",
        "browse_public"
    ]
    
    for pattern in patterns_to_try:
        print(f"\n--- Trying pattern: {pattern} ---")
        
        try:
            # First try single capture
            print("Attempting single capture...")
            response = scraper.client.scrape_api_data(
                url=base_url,
                url_pattern=pattern,
                timeout=30
            )
            
            if response:
                print(f"SUCCESS! Got response with pattern: {pattern}")
                print(f"URL: {response.get('url', 'unknown')}")
                print(f"Status: {response.get('status', 'unknown')}")
                
                # Save the successful response
                output_dir = Path("/home/jgupdogg/dev/dagster/engineering_pipeline/data")
                output_dir.mkdir(parents=True, exist_ok=True)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = output_dir / f"emma_test_{pattern.replace('*', '').replace('.', '')}_{timestamp}.json"
                
                with open(output_file, 'w') as f:
                    json.dump(response, f, indent=2, default=str)
                
                print(f"Saved to: {output_file}")
                
                # Try to parse the body
                body = response.get('body', '')
                if body:
                    try:
                        data = json.loads(body) if isinstance(body, str) else body
                        print(f"Data type: {type(data)}")
                        if isinstance(data, dict):
                            print(f"Keys: {list(data.keys())[:10]}")
                        elif isinstance(data, list):
                            print(f"List length: {len(data)}")
                    except:
                        print("Could not parse body as JSON")
                
                break  # Found a working pattern
                
            else:
                print("No response captured with single capture")
                
                # Try multiple captures
                print("Attempting multiple captures...")
                responses = scraper.client.scrape_api_data_multiple(
                    url=base_url,
                    url_pattern=pattern,
                    timeout=30,
                    capture_count=5
                )
                
                if responses:
                    print(f"Got {len(responses)} responses")
                    for i, resp in enumerate(responses):
                        print(f"  Response {i+1}: {resp.get('url', 'unknown')} - Status: {resp.get('status', 'unknown')}")
                else:
                    print("No responses captured")
                    
        except Exception as e:
            print(f"Error with pattern {pattern}: {e}")
    
    # Cleanup
    await scraper.client.teardown()
    print("\n--- Test complete ---")


if __name__ == "__main__":
    # Run the async test
    asyncio.run(test_emma_scraper())