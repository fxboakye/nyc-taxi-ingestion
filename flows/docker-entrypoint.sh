#!/bin/bash

# install dependencies
echo "Installing required modules..."
pip install -r requirements.txt

echo "Running ingestion script..."
python ingestion.py