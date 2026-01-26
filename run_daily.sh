#!/bin/bash

# Exit immediately if any command fails
set -e

# Activate virtual environment
source /absolute/path/to/your/project/venv/bin/activate

# Go to project directory
cd /absolute/path/to/your/project

echo "[$(date)] Running incremental load..."
python increment_run.py

echo "[$(date)] Running transformation notebook..."
papermill transform_data.ipynb transform_data_output.ipynb

echo "[$(date)] Daily job completed successfully."
