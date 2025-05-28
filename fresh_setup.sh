#!/bin/bash

echo "Healthcare Analytics - Fresh Setup"
echo "=================================="

# Check Python version
python3 --version

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate || venv\Scripts\activate

# Upgrade pip
echo "Upgrading pip..."
python -m pip install --upgrade pip

# Install requirements
echo "Installing requirements..."
pip install -r requirements.txt

# Run setup
echo "Running database setup..."
python setup.py

# Generate sample data
echo "Generating sample data..."
python scripts/generate_sample_data.py

# Run ETL to load data
echo "Loading data into database..."
python scripts/run_etl.py

# Verify installation
echo "Verifying installation..."
python scripts/verify_system.py

echo "Setup complete!"
echo "To start the dashboard, run: python dashboard/app.py"