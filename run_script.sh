#!/bin/bash

# Create a virtual environment if it doesn't exist
if [ ! -d "env" ]; then
    echo "Creating virtual environment..."
    python3 -m venv env
    echo "Virtual environment created."
fi

# Activate the virtual environment
echo "Activating the virtual environment..."
source env/bin/activate

# Install dependencies from requirements.txt
echo "Installing dependencies..."
pip3 install -r requirements.txt --quiet

# Run the Python script
echo "Running the Python script..."
cd extract
python3 main.py
cd ..

# Run the dbt script
echo "Running the dbt script..."
cd dbt
export DBT_PROFILES_DIR=$(pwd)

file_path="./profiles.yml"
# Check if the file exists
if [ -f "$file_path" ]; then
  echo "The file profiles.yml is exists."
else
  dbt init
fi


dbt deps
dbt build

# Deactivate the virtual environment
echo "Deactivating the virtual environment..."
deactivate

echo "Script execution completed."