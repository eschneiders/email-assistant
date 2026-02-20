#!/bin/bash
cd "$(dirname "$0")"

# Kill any existing instance
pkill -f "python app.py" 2>/dev/null
sleep 1

# Activate virtual environment and run
source venv/bin/activate
python app.py
