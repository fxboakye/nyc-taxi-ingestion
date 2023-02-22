#!/bin/bash
#upgrades pip
pip install --upgrade pip
#installs required modules
echo "Installing required modules..."
pip install -r requirements.txt

echo "Running ingestion script..."
python pg_ingestion.py