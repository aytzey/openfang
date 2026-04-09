#!/usr/bin/env python3
"""
Active job polling example — monitor the currently running B2C job.

Usage:
    python client_streaming.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from pulsivo_salesman_client import PulsivoSalesman

client = PulsivoSalesman("http://localhost:4200")

active = client.sales.get_active_job("b2c")
job = active.get("job")
if not job:
    print("No active B2C job.")
    raise SystemExit(0)

job_id = job.get("job_id") or job.get("id")
print(f"Polling job: {job_id}")

for _ in range(10):
    progress = client.sales.get_job(job_id)
    print(progress)
    if progress.get("status") and progress.get("status") != "running":
        break
