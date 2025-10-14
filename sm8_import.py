#!/usr/bin/env python3
"""
ServiceM8 Import Script — Imports data from Housecall Pro NDJSON exports to ServiceM8.

Usage:
    python sm8_import.py --ndjson-dir ./hcp_export/20251006T000000Z [--dry-run] [--only customers]

NOTE:
    - Assumes NDJSON files like customers.ndjson, employees.ndjson in the dir.
    - Maps basic fields; extend as needed for more (e.g., addresses, attachments).
    - Uses Bearer token auth from .env (SM8_API_KEY).
    - Imports customers first, stores ID mapping, then employees.
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
            sm8_company = {
                "name": hcp_customer.get("company") or f"{hcp_customer.get('first_name', '')} {hcp_customer.get('last_name', '')}".strip(),
                "phone": hcp_customer.get("mobile_number") or hcp_customer.get("home_number") or hcp_customer.get("work_number"),
                "email": hcp_customer.get("email"),
                "billing_address": hcp_customer.get("address", {}).get("street", "") if "address" in hcp_customer else "",
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

def import_employees(ndjson_path, headers, dry_run=False, mapping={}):
    print(f"[INFO] Importing employees from {ndjson_path}")
    with open(ndjson_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            hcp_employee = json.loads(line)
            sm8_staff = {
                "first": hcp_employee.get("first_name", ""),
                "last": hcp_employee.get("last_name", ""),
                "email": hcp_employee.get("email", ""),
                "mobile": hcp_employee.get("mobile_number", ""),
                "job_title": hcp_employee.get("role", ""),
                "color": hcp_employee.get("color_hex", ""),
                # Map permissions to security_role_uuid (you'll need to predefine or map role UUIDs in SM8)
                "security_role_uuid": "admin_uuid_here" if hcp_employee.get("permissions", {}).get("is_admin") else "tech_uuid_here",  # Replace with actual SM8 role UUIDs
                # Add more fields as needed: status_message, hide_from_schedule (e.g., based on permissions)
                "hide_from_schedule": 1 if not hcp_employee.get("permissions", {}).get("can_see_full_schedule") else 0,
            }
            if dry_run:
                print(f"[DRY] Would create staff: {sm8_staff}")
                mapping[hcp_employee["id"]] = "fake_uuid"
                continue
            response = requests.post(f"{BASE_URL}/staff.json", headers=headers, json=sm8_staff)
            if response.status_code == 200:
                sm8_uuid = response.json().get("uuid")
                mapping[hcp_employee["id"]] = sm8_uuid
                print(f"[SUCCESS] Imported employee {hcp_employee['id']} as {sm8_uuid}")
            else:
                print(f"[ERROR] Failed to import employee {hcp_employee['id']}: {response.status_code} - {response.text}")
    return mapping

def main():
    parser = argparse.ArgumentParser(description="Import Housecall Pro NDJSON to ServiceM8")
    parser.add_argument("--ndjson-dir", required=True, help="Directory with NDJSON export files")
    parser.add_argument("--dry-run", action="store_true", help="Simulate imports without posting")
    parser.add_argument("--only", help="Only import a single entity by name (e.g. customers, employees)")
    args = parser.parse_args()

    dir_path = pathlib.Path(args.ndjson_dir)
    if not dir_path.exists():
        sys.exit(f"Directory {args.ndjson_dir} not found")

    headers = authenticate()

    entities = ["customers", "employees"]
    if args.only:
        entities = [args.only]

    # Step 1: Import customers and build ID mapping
    customer_mapping = {}
    if "customers" in entities:
        customers_ndjson = dir_path / "customers.ndjson"
        if customers_ndjson.exists():
            customer_mapping = import_customers(customers_ndjson, headers, args.dry_run, customer_mapping)
        else:
            print("[WARN] customers.ndjson not found; skipping customer import")

    # Step 2: Import employees and build ID mapping
    employee_mapping = {}
    if "employees" in entities:
        employees_ndjson = dir_path / "employees.ndjson"
        if employees_ndjson.exists():
            employee_mapping = import_employees(employees_ndjson, headers, args.dry_run, employee_mapping)
        else:
            print("[WARN] employees.ndjson not found; skipping employee import")

    print("Import complete.")

if __name__ == "__main__":
    main()