#!/usr/bin/env python3
"""Test pivot endpoints."""

import subprocess
import sys

# Run tests in a fresh Python process to avoid module caching
result = subprocess.run([
    sys.executable, '-c', '''
import sys
sys.path.insert(0, ".")

from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

print("=== Test 1: Pivot at Child Level ===")
try:
    response = client.post(
        "/api/seller/AttakPik/pivot",
        json={"aggregation_level": "child", "granularity": "weekly", "metric_preset": "sales_overview"}
    )
    if response.status_code == 200:
        data = response.json()
        print(f"SUCCESS: {data.get('count')} rows")
    else:
        print(f"FAILED: {response.status_code}")
        print(response.text[:300])
except Exception as e:
    print(f"ERROR: {e}")

print()
print("=== Test 2: Pivot at Parent Level (Advertising) ===")
try:
    response = client.post(
        "/api/seller/AttakPik/pivot",
        json={"aggregation_level": "parent", "granularity": "weekly", "metric_preset": "advertising"}
    )
    if response.status_code == 200:
        data = response.json()
        print(f"SUCCESS: {data.get('count')} rows, metrics: {data.get('metrics')}")
    else:
        print(f"FAILED: {response.status_code}")
except Exception as e:
    print(f"ERROR: {e}")

print()
print("=== Test 3: Monthly Granularity ===")
try:
    response = client.post(
        "/api/seller/AttakPik/pivot",
        json={"aggregation_level": "parent", "granularity": "monthly", "metric_preset": "sales_overview"}
    )
    if response.status_code == 200:
        data = response.json()
        print(f"SUCCESS: {data.get('count')} rows, periods: {data.get('periods', [])[:3]}")
    else:
        print(f"FAILED: {response.status_code}")
except Exception as e:
    print(f"ERROR: {e}")

print()
print("=== Test 4: Account Level (All Metrics) ===")
try:
    response = client.post(
        "/api/seller/AttakPik/pivot",
        json={"aggregation_level": "account", "granularity": "weekly", "metric_preset": "all"}
    )
    if response.status_code == 200:
        data = response.json()
        print(f"SUCCESS: {data.get('count')} rows, {len(data.get('metrics', []))} metrics")
    else:
        print(f"FAILED: {response.status_code}")
except Exception as e:
    print(f"ERROR: {e}")
'''
], capture_output=True, text=True)

print(result.stdout)
if result.stderr:
    print("STDERR:", result.stderr)
sys.exit(0)

client = TestClient(app)

print("=== Test 1: Pivot at Child Level ===")
response = client.post(
    "/api/seller/AttakPik/pivot",
    json={"aggregation_level": "child", "granularity": "weekly", "metric_preset": "sales_overview"}
)
if response.status_code == 200:
    data = response.json()
    print(f"SUCCESS: {data.get('count')} rows")
else:
    print(f"FAILED: {response.status_code}")
    print(response.text[:300] if hasattr(response, 'text') else str(response))

print("\n=== Test 2: Pivot at Parent Level (Advertising) ===")
response = client.post(
    "/api/seller/AttakPik/pivot",
    json={"aggregation_level": "parent", "granularity": "weekly", "metric_preset": "advertising"}
)
if response.status_code == 200:
    data = response.json()
    print(f"SUCCESS: {data.get('count')} rows, metrics: {data.get('metrics')}")
else:
    print(f"FAILED: {response.status_code}")

print("\n=== Test 3: Monthly Granularity ===")
response = client.post(
    "/api/seller/AttakPik/pivot",
    json={"aggregation_level": "parent", "granularity": "monthly", "metric_preset": "sales_overview"}
)
if response.status_code == 200:
    data = response.json()
    print(f"SUCCESS: {data.get('count')} rows, periods: {data.get('periods', [])[:3]}")
else:
    print(f"FAILED: {response.status_code}")

print("\n=== Test 4: Account Level (All Metrics) ===")
response = client.post(
    "/api/seller/AttakPik/pivot",
    json={"aggregation_level": "account", "granularity": "weekly", "metric_preset": "all"}
)
if response.status_code == 200:
    data = response.json()
    print(f"SUCCESS: {data.get('count')} rows, {len(data.get('metrics', []))} metrics")
else:
    print(f"FAILED: {response.status_code}")
