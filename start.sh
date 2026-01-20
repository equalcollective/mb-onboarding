#!/bin/bash

# Install Python dependencies
pip install -r requirements.txt

# Build frontend
cd frontend
npm install
npm run build
cd ..

# Start the FastAPI server (serves both API and static frontend)
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
