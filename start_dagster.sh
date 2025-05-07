#!/bin/bash

# Activate conda environment
source ~/miniconda3/etc/profile.d/conda.sh
conda activate dagster-env

# Install the project in development mode
cd /home/jgupdogg/dev/dagster/dagster_project
pip install -e .

# Create pyproject.toml in the main directory if it doesn't exist
cat > /home/jgupdogg/dev/dagster/pyproject.toml << 'EOL'
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[tool.dagster]
module_name = "dagster_project"
EOL

# Start Dagster with explicit workspace file
cd /home/jgupdogg/dev/dagster
dagster dev -w dagster_project/workspace.yaml