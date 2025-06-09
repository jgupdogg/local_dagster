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
