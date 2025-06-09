"""
Script to run the emma_contracts_silver asset
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dagster import materialize
from engineering_pipeline.definitions import engineering_pipeline_defs

def run_contracts_silver():
    """Materialize the emma_contracts_silver asset"""
    print("Starting emma_contracts_silver asset materialization...")
    
    try:
        # Find the silver asset
        silver_asset = None
        for asset in engineering_pipeline_defs.assets:
            if asset.key.path[-1] == "emma_contracts_silver":
                silver_asset = asset
                break
        
        if not silver_asset:
            print("❌ emma_contracts_silver asset not found")
            return
        
        # Materialize just the silver asset
        result = materialize(
            [silver_asset],
            resources=engineering_pipeline_defs.resources,
        )
        
        print(f"✅ Asset materialization completed successfully!")
        print(f"Run ID: {result.run_id}")
        
        # Run the test script to verify
        print("\n" + "="*50)
        print("Running verification test...")
        print("="*50)
        import subprocess
        subprocess.run([sys.executable, "test_contracts_silver.py"])
        
    except Exception as e:
        print(f"❌ Error running asset: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_contracts_silver()