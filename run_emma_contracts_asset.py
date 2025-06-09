"""
Script to run the emma_public_contracts asset directly
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dagster import materialize
from engineering_pipeline.definitions import engineering_pipeline_defs

def run_emma_contracts():
    """Materialize the emma_public_contracts asset"""
    print("Starting emma_public_contracts asset materialization...")
    
    try:
        result = materialize(
            [asset for asset in engineering_pipeline_defs.assets if asset.key.path[-1] == "emma_public_contracts"],
            resources=engineering_pipeline_defs.resources,
        )
        
        print(f"Asset materialization completed successfully!")
        print(f"Run ID: {result.run_id}")
        
        # Now run the test query
        print("\nRunning test query...")
        import subprocess
        subprocess.run([sys.executable, "test_emma_contracts_query.py"])
        
    except Exception as e:
        print(f"Error running asset: {e}")
        raise

if __name__ == "__main__":
    run_emma_contracts()