#!/usr/bin/env python3
"""
Basic example — inspect sales state through the REST API.

Usage:
    python client_basic.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from pulsivo_salesman_client import PulsivoSalesman

client = PulsivoSalesman("http://localhost:4200")

# Check server health
health = client.health()
print("Server:", health)

# Inspect the current B2C profile
profile = client.sales.get_profile("b2c")
print("B2C profile:", profile)

# Fetch the latest recent runs
runs = client.sales.list_runs(segment="b2c", limit=5)
print("Recent runs:", runs)
