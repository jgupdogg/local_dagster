#!/usr/bin/env python3
"""
Test script to run the EMMA asset directly
"""
import sys
from dagster import materialize

from engineering_pipeline.definitions import engineering_pipeline_defs

if __name__ == "__main__":
    print("🚀 Starting EMMA asset test...")
    
    try:
        # Get the assets and resources from the definitions file directly
        from engineering_pipeline.definitions import engineering_assets, resources
        assets = engineering_assets
        
        print(f"Found {len(assets)} assets")
        print(f"Available resources: {list(resources.keys())}")
        
        # Find the EMMA asset
        emma_asset = None
        for asset in assets:
            if 'emma_public_solicitations' in str(asset.key):
                emma_asset = asset
                break
        
        if not emma_asset:
            print("❌ EMMA asset not found!")
            sys.exit(1)
        
        print(f"✅ Found EMMA asset: {emma_asset.key}")
        
        # Materialize the asset
        print("🔄 Materializing asset...")
        result = materialize([emma_asset], resources=resources)
        
        print("✅ Asset materialization completed!")
        print(f"Result success: {result.success}")
        
        if result.success:
            print("🎉 EMMA data should be saved to file!")
        else:
            print("❌ Materialization failed")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()