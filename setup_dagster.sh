#!/bin/bash
# setup_dagster.sh - Script to set up Dagster project

# Set project directory
PROJECT_DIR="/home/jgupdogg/dev/dagster"
cd $PROJECT_DIR

# Create the basic directory structure
mkdir -p dagster_project/dagster_project/{assets,jobs,ops,resources,utils}
touch dagster_project/dagster_project/__init__.py
touch dagster_project/dagster_project/{assets,jobs,ops,resources,utils}/__init__.py
touch dagster_project/workspace.yaml
touch dagster_project/setup.py

# Create workspace.yaml
cat > dagster_project/workspace.yaml << 'EOL'
load_from:
  - python_module:
      module_name: dagster_project
      working_directory: .
EOL

# Create setup.py
cat > dagster_project/setup.py << 'EOL'
from setuptools import find_packages, setup

setup(
    name="dagster_project",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-webserver",
        "dagster-postgres",
        "pandas",
    ],
)
EOL

# Create the main __init__.py with sample assets
cat > dagster_project/dagster_project/__init__.py << 'EOL'
from dagster import Definitions, asset, define_asset_job, AssetSelection, load_assets_from_modules
import pandas as pd
from .resources.database import db_resource
import dagster_project.assets.data_models as data_models

@asset
def sample_data():
    """Create a sample dataframe."""
    data = {
        'id': [1, 2, 3, 4, 5],
        'value': [10, 20, 30, 40, 50],
        'category': ['A', 'B', 'A', 'C', 'B']
    }
    return pd.DataFrame(data)

@asset
def processed_data(sample_data):
    """Process the sample data."""
    df = sample_data.copy()
    df['value_squared'] = df['value'] ** 2
    return df

# Load assets from the data_models module
data_model_assets = load_assets_from_modules([data_models])

# Define a job that processes all assets
all_assets_job = define_asset_job(
    name="process_all_data",
    selection=AssetSelection.all()
)

# Define the Dagster definitions
defs = Definitions(
    assets=[sample_data, processed_data, *data_model_assets],
    jobs=[all_assets_job],
    resources={
        "db": db_resource
    }
)
EOL

# Create a sample database resource
cat > dagster_project/dagster_project/resources/database.py << 'EOL'
from dagster import resource, ConfigurableResource
from typing import Optional

# Example of wrapping your existing DB manager as a Dagster resource
class DatabaseManager(ConfigurableResource):
    connection_string: str
    pool_size: Optional[int] = 5
    
    def get_connection(self):
        # Replace with your actual DB manager code
        print(f"Connecting to database with {self.connection_string} and pool size {self.pool_size}")
        return {"connection": "db_connection_object"}

@resource
def db_resource(init_context):
    return DatabaseManager(
        connection_string="postgresql://user:password@localhost:5432/db",
        pool_size=10
    )
EOL

# Create a sample data model
cat > dagster_project/dagster_project/assets/data_models.py << 'EOL'
from dagster import asset
from ..resources.database import DatabaseManager

@asset(
    required_resource_keys={"db"},
)
def user_data(context, db):
    # Use your data model to fetch data
    conn = db.get_connection()
    
    # Example of using your existing data model (replace with actual code)
    context.log.info("Fetching user data")
    result = {"users": ["user1", "user2", "user3"]}
    
    return result
EOL

# Create a sample utility function
cat > dagster_project/dagster_project/utils/helpers.py << 'EOL'
def process_data(data):
    """Sample utility function that could process data."""
    if isinstance(data, dict):
        return {k: v.upper() if isinstance(v, str) else v for k, v in data.items()}
    return data
EOL

# Create a startup script
cat > $PROJECT_DIR/start_dagster.sh << 'EOL'
#!/bin/bash

# Activate conda environment
source ~/miniconda3/etc/profile.d/conda.sh
conda activate dagster-env

# Install the project in development mode
cd /home/jgupdogg/dev/dagster/dagster_project
pip install -e .

# Start Dagster
cd /home/jgupdogg/dev/dagster
dagster dev
EOL

# Make the startup script executable
chmod +x $PROJECT_DIR/start_dagster.sh

echo "Dagster project setup complete!"
echo "To start Dagster, run: ./start_dagster.sh"