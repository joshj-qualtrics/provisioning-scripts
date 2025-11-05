#!/bin/bash

PROJECT_DIR="/Users/joshj/Documents/GitHub/provisioning-scripts/coda" 
VENV_ACTIVATE="$PROJECT_DIR/venv/bin/activate" 
PYTHON_SCRIPT="coda_data_pipeline.py"0


echo "--- Starting Coda Pipeline @ $(date) ---"

cd "$PROJECT_DIR" || { echo "Error: Cannot change directory to $PROJECT_DIR"; exit 1; }

if [ -f "$VENV_ACTIVATE" ]; then
    source "$VENV_ACTIVATE"
else
    echo "Error: Virtual environment activation script not found at $VENV_ACTIVATE"
    exit 1
fi

python3 "$PYTHON_SCRIPT"

deactivate

echo "--- Coda Pipeline Finished @ $(date) ---"

