#!/usr/bin/env python3
"""
Simple test to find EMMA network requests
"""
import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
import sys
sys.path.append('/home/jgupdogg/dev/dagster')

import nodriver as uc
from nodriver import cdp
network = cdp.network


async def capture_all_network_traffic():
    """Capture ALL network traffic to find the right pattern"""
    
    # Track all requests and responses
    all_requests = []
    all_responses = []
    captured_bodies = {}
    
    def on_request(event):
        """Log every request"""
        request = event.request
        all_requests.append({
            'requestId': event.request_id,
            'url': request.url,
            'method': request.method,
            'timestamp': datetime.now().isoformat()
        })
        print(f"REQUEST: {request.method} {request.url[:100]}...")
    
    def on_response(event):
        """Log every response"""
        all_responses.append({
            'requestId': event.request_id,
            'url': event.response.url,
            'status': event.response.status,
            'mimeType': event.response.mime_type,
            'timestamp': datetime.now().isoformat()
        })
        print(f"RESPONSE: {event.response.status} {event.response.url[:100]}... [{event.response.mime_type}]")
    
    # Setup browser
    browser = await uc.start(
        headless=False,
        browser_args=['--no-sandbox', '--disable-setuid-sandbox']
    )
    
    try:
        # Get a tab
        tab = await browser.get('about:blank')
        
        # Enable network monitoring
        await tab.send(network.enable())
        tab.add_handler(network.RequestWillBeSent, on_request)
        tab.add_handler(network.ResponseReceived, on_response)
        
        print("\n=== Starting network capture ===")
        print(f"Time: {datetime.now()}")
        print("Navigating to EMMA...")
        
        # Navigate to the page
        await tab.get("https://emma.maryland.gov/page.aspx/en/rfp/request_browse_public")
        
        # Wait for page to load and make API calls
        print("\nWaiting for page to load and API calls...")
        await asyncio.sleep(15)
        
        # Try clicking or interacting to trigger any lazy-loaded content
        try:
            # Try to find and interact with search/load buttons
            print("Looking for interactive elements...")
            # This is a basic approach - might need to be more specific
            await tab.evaluate('document.readyState')
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Error with page interaction: {e}")
        
        print(f"\n=== Capture complete ===")
        print(f"Total requests: {len(all_requests)}")
        print(f"Total responses: {len(all_responses)}")
        
        # Try to get response bodies for interesting requests
        print("\n=== Checking for API responses ===")
        api_responses = []
        
        for response in all_responses:
            url = response['url']
            # Look for potential API endpoints
            if any(pattern in url.lower() for pattern in ['api', 'service', 'json', 'data', 'rfp', 'request', 'public']):
                if response['mimeType'] in ['application/json', 'text/json', 'application/javascript']:
                    try:
                        # Try to get the response body
                        body_data = await tab.send(
                            network.get_response_body(request_id=response['requestId'])
                        )
                        
                        if body_data and isinstance(body_data, dict):
                            print(f"\nGot body for: {url}")
                            api_responses.append({
                                'url': url,
                                'status': response['status'],
                                'mimeType': response['mimeType'],
                                'body': body_data.get('body', ''),
                                'base64Encoded': body_data.get('base64Encoded', False)
                            })
                    except Exception as e:
                        print(f"Could not get body for {url}: {e}")
        
        # Save all captured data
        output_dir = Path("/home/jgupdogg/dev/dagster/engineering_pipeline/data")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save all requests/responses
        all_traffic_file = output_dir / f"emma_all_traffic_{timestamp}.json"
        with open(all_traffic_file, 'w') as f:
            json.dump({
                'requests': all_requests,
                'responses': all_responses,
                'api_responses': api_responses
            }, f, indent=2)
        
        print(f"\nSaved all traffic to: {all_traffic_file}")
        
        # Save just API responses
        if api_responses:
            api_file = output_dir / f"emma_api_responses_{timestamp}.json"
            with open(api_file, 'w') as f:
                json.dump(api_responses, f, indent=2)
            print(f"Saved API responses to: {api_file}")
        
        # Print summary of interesting URLs
        print("\n=== Potential API endpoints found ===")
        for resp in api_responses:
            print(f"- {resp['url']}")
            if resp.get('body'):
                try:
                    data = json.loads(resp['body'])
                    print(f"  Data type: {type(data).__name__}")
                    if isinstance(data, dict):
                        print(f"  Keys: {list(data.keys())[:5]}")
                    elif isinstance(data, list):
                        print(f"  List length: {len(data)}")
                except:
                    print("  (Could not parse as JSON)")
        
    finally:
        try:
            if browser:
                await browser.stop()
        except Exception as e:
            print(f"Error stopping browser: {e}")


if __name__ == "__main__":
    asyncio.run(capture_all_network_traffic())