#!/usr/bin/env python3
"""
ServiceM8 Import Script — Imports data from Housecall Pro NDJSON exports to ServiceM8.

Usage:
    python sm8_import.py --ndjson-dir ./hcp_export/20251006T000000Z [--dry-run]

NOTE:
    - Assumes NDJSON files like customers.ndjson, jobs.ndjson in the dir.
    - Maps basic fields; extend as needed for more (e.g., addresses, attachments).
    - Uses Bearer token auth from .env (SM8_API_KEY).
    - Imports customers first, stores ID mapping, then jobs linking via company_uuid.
"""
import argparse
import json
import pathlib
import requests
import sys
import os
from dotenv import load_dotenv

load_dotenv()  # Load .env file

BASE_URL = "https://api.servicem8.com/api_1.0"

def authenticate():
    token = os.getenv("SM8_API_KEY")
    if not token:
        sys.exit("Missing SM8_API_KEY in .env file.")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def import_customers(ndjson_path, headers, dry_run=False, mapping={}):
    print(f"[INFO] Importing customers from {ndjson_path}")
    with open(ndjson_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            hcp_customer = json.loads(line)
            # Map HCP to SM8 Company
            sm8_company = {
                "name": hcp_customer.get("company") or f"{hcp_customer.get('first_name', '')} {hcp_customer.get('last_name', '')}".strip(),
                "phone": hcp_customer.get("mobile_number") or hcp_customer.get("home_number") or hcp_customer.get("work_number"),
                "email": hcp_customer.get("email"),
                # Address mapping (assuming first address is billing/service)
                "billing_address": hcp_customer.get("address", {}).get("street", "") if "address" in hcp_customer else "",
                # Add more fields as needed: notes, etc.
            }
            if dry_run:
                print(f"[DRY] Would create company: {sm8_company}")
                mapping[hcp_customer["id"]] = "fake_uuid"
                continue
            response = requests.post(f"{BASE_URL}/company.json", headers=headers, json=sm8_company)
            if response.status_code == 200:
                sm8_uuid = response.json().get("uuid")
                mapping[hcp_customer["id"]] = sm8_uuid
                print(f"[SUCCESS] Imported customer {hcp_customer['id']} as {sm8_uuid}")
            else:
                print(f"[ERROR] Failed to import customer {hcp_customer['id']}: {response.status_code} - {response.text}")
    return mapping

def import_jobs(ndjson_path, headers, mapping, dry_run=False):
    print(f"[INFO] Importing jobs from {ndjson_path}")
    with open(ndjson_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            hcp_job = json.loads(line)
            customer_id = hcp_job.get("customer", {}).get("id")
            if customer_id not in mapping:
                print(f"[WARN] Skipping job {hcp_job['id']} - no matching customer {customer_id}")
                continue
            sm8_job = {
                "company_uuid": mapping[customer_id],
                "status": hcp_job.get("work_status", "Quote"),  # Map statuses as needed
                "geo_address": hcp_job.get("address", {}).get("street", ""),  # Or use lat/long if available
                "description": hcp_job.get("description"),
                # Scheduled time if available
                "date": hcp_job.get("schedule", {}).get("scheduled_start") if "schedule" in hcp_job else None,
                # Add more: total_amount, etc.
            }
            if dry_run:
                print(f"[DRY] Would create job: {sm8_job}")
                continue
            response = requests.post(f"{BASE_URL}/job.json", headers=headers, json=sm8_job)
            if response.status_code == 200:
                print(f"[SUCCESS] Imported job {hcp_job['id']}")
            else:
                print(f"[ERROR] Failed to import job {hcp_job['id']}: {response.status_code} - {response.text}")

def main():
    parser = argparse.ArgumentParser(description="Import Housecall Pro NDJSON to ServiceM8")
    parser.add_argument("--ndjson-dir", required=True, help="Directory with NDJSON export files")
    parser.add_argument("--dry-run", action="store_true", help="Simulate imports without posting")
    args = parser.parse_args()

    dir_path = pathlib.Path(args.ndjson_dir)
    if not dir_path.exists():
        sys.exit(f"Directory {args.ndjson_dir} not found")

    headers = authenticate()

    # Step 1: Import customers and build ID mapping
    customer_mapping = {}
    customers_ndjson = dir_path / "customers.ndjson"
    if customers_ndjson.exists():
        customer_mapping = import_customers(customers_ndjson, headers, args.dry_run, customer_mapping)
    else:
        print("[WARN] customers.ndjson not found; skipping customer import")

    # Step 2: Import jobs using mapping
    jobs_ndjson = dir_path / "jobs.ndjson"
    if jobs_ndjson.exists():
        import_jobs(jobs_ndjson, headers, customer_mapping, args.dry_run)
    else:
        print("[WARN] jobs.ndjson not found; skipping job import")

    # Add more entities as needed (e.g., invoices, estimates)

    print("Import complete.")

if __name__ == "__main__":
    main()