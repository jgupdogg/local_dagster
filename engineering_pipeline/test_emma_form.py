#!/usr/bin/env python3
"""
Test to capture EMMA form POST requests for solicitation data
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


async def capture_form_requests():
    """Capture the form POST requests that load solicitation data"""
    
    # Track all requests and responses
    all_requests = []
    all_responses = []
    captured_bodies = {}
    
    def on_request(event):
        """Log every request, especially POST requests"""
        request = event.request
        all_requests.append({
            'requestId': event.request_id,
            'url': request.url,
            'method': request.method,
            'timestamp': datetime.now().isoformat()
        })
        
        if request.method == 'POST':
            print(f"🔥 POST REQUEST: {request.url}")
            if hasattr(request, 'postData'):
                print(f"   Post data: {request.postData[:200]}...")
    
    def on_response(event):
        """Log every response, especially for POST requests"""
        request_match = next((req for req in all_requests if req['requestId'] == event.request_id), None)
        all_responses.append({
            'requestId': event.request_id,
            'url': event.response.url,
            'status': event.response.status,
            'mimeType': event.response.mime_type,
            'timestamp': datetime.now().isoformat(),
            'method': request_match['method'] if request_match else 'unknown'
        })
        
        if request_match and request_match['method'] == 'POST':
            print(f"🔥 POST RESPONSE: {event.response.status} {event.response.url} [{event.response.mime_type}]")
    
    browser = await uc.start(
        headless=False,
        browser_args=['--no-sandbox', '--disable-setuid-sandbox']
    )
    
    try:
        tab = await browser.get('about:blank')
        
        # Enable network monitoring
        await tab.send(network.enable())
        tab.add_handler(network.RequestWillBeSent, on_request)
        tab.add_handler(network.ResponseReceived, on_response)
        
        print(f"🚀 Starting EMMA form capture at {datetime.now()}")
        
        # Navigate to the page
        await tab.get("https://emma.maryland.gov/page.aspx/en/rfp/request_browse_public")
        print("📄 Page loaded, waiting for initial content...")
        await asyncio.sleep(5)
        
        # Try to trigger a search to capture the form POST
        print("🔍 Triggering search to capture form POST...")
        
        try:
            # Look for the search button and click it
            search_button = await tab.find('button[id*="cmdSearchBtn"], input[id*="cmdSearchBtn"]', timeout=5)
            if search_button:
                print("✅ Found search button, clicking...")
                await search_button.click()
                await asyncio.sleep(3)
            else:
                print("⚠️ Search button not found, trying alternative approach...")
                
                # Try submitting the form directly
                form_submit = await tab.evaluate('''
                    // Find and submit the main form
                    const forms = document.querySelectorAll('form');
                    if (forms.length > 0) {
                        const mainForm = forms[0];
                        // Try to find search button and click it
                        const searchBtn = document.querySelector('[id*="cmdSearchBtn"]');
                        if (searchBtn) {
                            searchBtn.click();
                            return "Clicked search button";
                        } else {
                            return "No search button found";
                        }
                    }
                    return "No forms found";
                ''')
                print(f"Form submit result: {form_submit}")
                await asyncio.sleep(3)
        except Exception as e:
            print(f"Error triggering search: {e}")
        
        # Try pagination to trigger more POST requests
        print("📖 Trying pagination...")
        try:
            next_page = await tab.find('input[id*="PagerBtnNextPage"], button[id*="PagerBtn"]', timeout=3)
            if next_page:
                print("📄 Found pagination button, clicking...")
                await next_page.click()
                await asyncio.sleep(3)
        except Exception as e:
            print(f"Pagination error: {e}")
        
        print(f"\n📊 Capture complete!")
        print(f"Total requests: {len(all_requests)}")
        print(f"Total responses: {len(all_responses)}")
        
        # Check for POST requests
        post_requests = [req for req in all_requests if req['method'] == 'POST']
        print(f"POST requests: {len(post_requests)}")
        
        for post_req in post_requests:
            print(f"  - {post_req['url']}")
        
        # Try to get response bodies for POST responses
        print("\n📋 Getting response bodies for POST requests...")
        post_response_data = []
        
        for response in all_responses:
            if response['method'] == 'POST' and response['status'] == 200:
                try:
                    body_data = await tab.send(
                        network.get_response_body(request_id=response['requestId'])
                    )
                    
                    if body_data and isinstance(body_data, dict):
                        body_content = body_data.get('body', '')
                        if body_content:
                            post_response_data.append({
                                'url': response['url'],
                                'status': response['status'],
                                'body': body_content,
                                'timestamp': response['timestamp']
                            })
                            print(f"✅ Got POST response body for: {response['url']}")
                            print(f"   Body length: {len(body_content)} characters")
                except Exception as e:
                    print(f"❌ Could not get body for {response['url']}: {e}")
        
        # Save all captured data
        output_dir = Path("/home/jgupdogg/dev/dagster/engineering_pipeline/data")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save comprehensive traffic data
        all_traffic_file = output_dir / f"emma_form_traffic_{timestamp}.json"
        with open(all_traffic_file, 'w') as f:
            json.dump({
                'requests': all_requests,
                'responses': all_responses,
                'post_responses': post_response_data
            }, f, indent=2)
        
        print(f"\n💾 Saved all traffic to: {all_traffic_file}")
        
        # Save just POST response bodies if we got any
        if post_response_data:
            post_file = output_dir / f"emma_post_data_{timestamp}.json"
            with open(post_file, 'w') as f:
                json.dump(post_response_data, f, indent=2)
            print(f"💾 Saved POST data to: {post_file}")
            
            # Analyze the POST response content
            for post_data in post_response_data:
                body = post_data['body']
                print(f"\n🔍 Analyzing POST response from {post_data['url']}:")
                
                # Check if it's HTML (likely contains the table data)
                if '<table' in body.lower() or '<tr' in body.lower():
                    print("   ✅ Contains HTML table data")
                    # Count table rows
                    row_count = body.lower().count('<tr')
                    print(f"   📊 Estimated table rows: {row_count}")
                
                # Check if it contains form data or JavaScript
                if 'viewstate' in body.lower() or '__dopostback' in body.lower():
                    print("   ⚙️ Contains ASP.NET ViewState/PostBack data")
        
        print("\n🎯 Next steps:")
        print("   1. Check the saved files for POST response data")
        print("   2. The solicitations data is likely in the HTML response")
        print("   3. Parse the HTML table to extract solicitation records")
        
    finally:
        try:
            if browser:
                await browser.stop()
        except Exception as e:
            print(f"Error stopping browser: {e}")


if __name__ == "__main__":
    asyncio.run(capture_form_requests())