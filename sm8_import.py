#!/usr/bin/env python3
"""
ServiceM8 Import Script — Imports data from Housecall Pro NDJSON exports to ServiceM8.

Usage:
    python sm8_import.py --ndjson-dir ./hcp_export/20251006T000000Z --only employees
    python sm8_import.py --ndjson-dir ./hcp_export/latest --only employees --dry-run
    python sm8_import.py --ndjson-dir ./hcp_export/latest --only employees --limit 50 --skip 200

Environment:
    SM8_API_KEY                (required) Bearer token for ServiceM8 API
    SM8_BASE_URL               (optional) defaults to https://api.servicem8.com/api_1.0
    SM8_ROLE_UUID_ADMIN        (optional) UUID to assign when HCP employee.permissions.is_admin = True
    SM8_ROLE_UUID_DEFAULT      (optional) UUID to assign otherwise (if unset, the field is omitted)

Notes:
    - Expects an NDJSON file named employees.ndjson in the provided directory.
    - ServiceM8 POST /staff.json requires first, last, email (length ≤ 30 for first/last).
    - We keep a mapping file to avoid duplicating staff on reruns:
        ./sm8_mappings/staff_map.json  ->  { "<hcp_employee_id>": "<sm8_uuid>" }
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import sys
import time
from typing import Any, Dict, Iterable, Optional, Tuple

import requests
from requests import Session
from requests.exceptions import (
    ReadTimeout,
    ConnectTimeout,
    ConnectionError as ReqConnectionError,
    ChunkedEncodingError,
)
from urllib3.exceptions import ProtocolError

# ---------------- Env & Constants ----------------

DEFAULT_BASE_URL = "https://api.servicem8.com/api_1.0"
STAFF_ENDPOINT = "/staff.json"

TRANSIENT_HTTP_STATUSES = {408, 425, 429, 500, 502, 503, 504}
TRANSIENT_EXC = (ReadTimeout, ConnectTimeout, ReqConnectionError, ChunkedEncodingError, ProtocolError)

# ---------------- Utility helpers ----------------

def getenv_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

def ensure_dir(p: pathlib.Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def load_json_if_exists(p: pathlib.Path, default: Any) -> Any:
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def save_json(p: pathlib.Path, obj: Any) -> None:
    ensure_dir(p.parent)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def iter_ndjson_lines(path: pathlib.Path, *, skip: int = 0, limit: Optional[int] = None) -> Iterable[Tuple[int, Dict[str, Any]]]:
    """Yield (line_index, parsed_obj) from an NDJSON file, supporting skip & limit."""
    count = 0
    with path.open("r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            if idx < skip:
                continue
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                print(f"[WARN] Bad JSON at line {idx+1}, skipping")
                continue
            yield idx, obj
            count += 1
            if limit is not None and count >= limit:
                break

def only_digits_phone(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    return digits or None

def truncate(s: str, n: int) -> str:
    return s if len(s) <= n else s[:n]

# ---------------- ServiceM8 client ----------------

class SM8Client:
    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None):
        self.base_url = (base_url or getenv_str("SM8_BASE_URL", DEFAULT_BASE_URL)).rstrip("/")
        self.api_key = api_key or getenv_str("SM8_API_KEY")
        if not self.api_key:
            sys.exit("Missing SM8_API_KEY in environment.")

        self.session: Session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        # knobs (tweak if needed)
        self.connect_timeout = float(getenv_str("SM8_CONNECT_TIMEOUT", "15"))
        self.read_timeout = float(getenv_str("SM8_READ_TIMEOUT", "60"))
        self.max_retries = int(getenv_str("SM8_MAX_RETRIES", "8"))
        self.backoff_cap = float(getenv_str("SM8_BACKOFF_MAX", "60"))

    def _sleep_backoff(self, attempt: int, retry_after: Optional[str]) -> None:
        # prefer explicit Retry-After seconds if present
        if retry_after and retry_after.isdigit():
            sleep_for = min(int(retry_after), self.backoff_cap)
        else:
            # simple exponential with jitter
            base = min(2 ** (attempt - 1), self.backoff_cap)
            sleep_for = min(base + (base * 0.25), self.backoff_cap)
        print(f"[INFO] Backing off {sleep_for:.1f}s before retry...")
        time.sleep(sleep_for)

    def request(self, method: str, path: str, *, json_body: Optional[dict] = None, params: Optional[dict] = None) -> requests.Response:
        url = f"{self.base_url}{path}"
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = self.session.request(
                    method=method.upper(),
                    url=url,
                    json=json_body,
                    params=params or {},
                    timeout=(self.connect_timeout, self.read_timeout),
                )
            except TRANSIENT_EXC as e:
                if attempt >= self.max_retries:
                    print(f"[ERROR] Network error (final): {type(e).__name__}: {e}")
                    raise
                print(f"[WARN] Network error {type(e).__name__} on {url} (attempt {attempt}/{self.max_retries})")
                self._sleep_backoff(attempt, None)
                continue

            if resp.status_code in TRANSIENT_HTTP_STATUSES:
                if attempt >= self.max_retries:
                    print(f"[ERROR] HTTP {resp.status_code} (final) for {url}: {resp.text[:300]}")
                    resp.raise_for_status()
                print(f"[WARN] HTTP {resp.status_code} on {url} (attempt {attempt}/{self.max_retries})")
                self._sleep_backoff(attempt, resp.headers.get("Retry-After"))
                continue

            if resp.status_code >= 400:
                # Non-retryable
                print(f"[ERROR] HTTP {resp.status_code} for {url}: {resp.text[:500]}")
                resp.raise_for_status()

            return resp

    # ---- Generic creator ----
    def create_record(self, endpoint: str, record: dict) -> Tuple[str, dict]:
        """
        POST a record to an API endpoint (e.g. '/staff.json').
        Returns (uuid, parsed_response_json_dict).
        uuid is derived from 'x-record-uuid' header if present, else from a 'uuid' key in JSON.
        """
        resp = self.request("POST", endpoint, json_body=record)
        uuid = resp.headers.get("x-record-uuid")
        parsed = {}
        try:
            parsed = resp.json()
        except Exception:
            parsed = {}
        if not uuid:
            uuid = (parsed or {}).get("uuid") or ""
        return uuid, parsed

# ---------------- Mappers (HCP -> SM8) ----------------

def map_employee_hcp_to_sm8(hcp: dict) -> Optional[dict]:
    """
    Convert an HCP employee row to ServiceM8 staff payload.
    Enforces SM8's required fields: first, last, email.
    """
    # Names: ensure required
    first = (hcp.get("first_name") or "").strip()
    last = (hcp.get("last_name") or "").strip()

    # If both missing, try HCP 'name'
    if not first and not last:
        full = (hcp.get("name") or "").strip()
        if full:
            parts = full.split()
            first = parts[0]
            last = " ".join(parts[1:]) if len(parts) > 1 else "Unknown"
    if not first:
        first = "Unknown"
    if not last:
        last = "Employee"

    # Enforce 30 char limit per SM8 docs
    first = truncate(first, 30)
    last = truncate(last, 30)

    email = (hcp.get("email") or "").strip()
    if not email:
        # ServiceM8 requires email (also used as login). Skip if absent.
        return None

    mobile = (hcp.get("mobile_number") or hcp.get("phone") or "").strip()
    mobile = only_digits_phone(mobile) or ""

    job_title = (hcp.get("role") or hcp.get("title") or "").strip()
    color = (hcp.get("color_hex") or "").strip()

    # Permissions mapping -> security role UUIDs from env (optional)
    perms = hcp.get("permissions") or {}
    is_admin = bool(perms.get("is_admin"))
    role_admin = getenv_str("SM8_ROLE_UUID_ADMIN")
    role_default = getenv_str("SM8_ROLE_UUID_DEFAULT")

    # Hide from schedule (optional behaviour): if employee cannot see full schedule, hide them by default.
    hide_from_schedule = 1 if not bool(perms.get("can_see_full_schedule")) else 0

    payload = {
        "first": first,
        "last": last,
        "email": email,
        "mobile": mobile,
        "job_title": job_title,
        "color": color,
        "hide_from_schedule": hide_from_schedule,
    }

    # Only include security_role_uuid when provided via env.
    if is_admin and role_admin:
        payload["security_role_uuid"] = role_admin
    elif (not is_admin) and role_default:
        payload["security_role_uuid"] = role_default

    # Remove empty strings (SM8 often prefers omitted vs empty)
    payload = {k: v for k, v in payload.items() if v not in ("", None)}

    return payload

# ---------------- Importers ----------------

def import_employees(ndjson_path: pathlib.Path, client: SM8Client, *, dry_run: bool, limit: Optional[int], skip: int, mapping_path: pathlib.Path) -> None:
    """
    Import employees.ndjson into ServiceM8 /staff.json.
    Maintains a mapping file {hcp_employee_id: sm8_uuid} to avoid duplicates on reruns.
    """
    print(f"[INFO] Importing employees from {ndjson_path}")
    if not ndjson_path.exists():
        print("[WARN] employees.ndjson not found; nothing to import.")
        return

    # Load existing HCP->SM8 id map to make the process idempotent
    id_map: Dict[str, str] = load_json_if_exists(mapping_path, default={})
    seen_emails_this_run: set[str] = set()

    created = 0
    skipped = 0
    errors = 0

    for line_idx, hcp_employee in iter_ndjson_lines(ndjson_path, skip=skip, limit=limit):
        hcp_id = str(hcp_employee.get("id", ""))
        if hcp_id and hcp_id in id_map:
            print(f"[SKIP] HCP employee {hcp_id} already mapped to SM8 {id_map[hcp_id]}")
            skipped += 1
            continue

        payload = map_employee_hcp_to_sm8(hcp_employee)
        if payload is None:
            print(f"[WARN] Missing required fields (email) at file line {line_idx+1}; skipping")
            skipped += 1
            continue

        # naive dedupe by email in the same run
        email = payload.get("email")
        if email in seen_emails_this_run:
            print(f"[SKIP] Duplicate email '{email}' in this run; skipping")
            skipped += 1
            continue

        if dry_run:
            print(f"[DRY] Would POST {STAFF_ENDPOINT}: {payload}")
            # fake uuid to keep mapping consistent during dry-run
            fake_uuid = f"dryrun-{hcp_id or line_idx}"
            if hcp_id:
                id_map[hcp_id] = fake_uuid
            created += 1
            seen_emails_this_run.add(email)
            continue

        try:
            sm8_uuid, _resp_json = client.create_record(STAFF_ENDPOINT, payload)
            if not sm8_uuid:
                # Some responses might not include UUID; treat as success but warn.
                print(f"[WARN] Created staff but no UUID returned for HCP {hcp_id} (check ServiceM8).")
            else:
                print(f"[OK] Created staff {sm8_uuid} for HCP {hcp_id or '(unknown id)'}")

            if hcp_id:
                id_map[hcp_id] = sm8_uuid or "unknown"
            created += 1
            seen_emails_this_run.add(email)

            # Persist mapping every few records to survive interruptions
            if created % 25 == 0:
                save_json(mapping_path, id_map)

        except requests.HTTPError as e:
            errors += 1
            code = getattr(e.response, "status_code", None)
            body = getattr(e.response, "text", "") or ""
            print(f"[ERROR] Failed to create staff for HCP {hcp_id or '(unknown)'} :: HTTP {code} :: {body[:300]}")

    # Final save of id_map
    save_json(mapping_path, id_map)

    print(f"[DONE] Employees import complete. created={created}, skipped={skipped}, errors={errors}")
    print(f"[INFO] Mapping saved to: {mapping_path}")

# ---------------- Main ----------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Import Housecall Pro NDJSON → ServiceM8")
    ap.add_argument("--ndjson-dir", required=True, help="Directory containing NDJSON files from HCP export")
    ap.add_argument("--only", default="employees", choices=["employees"], help="Which entity to import (default: employees)")
    ap.add_argument("--dry-run", action="store_true", help="Simulate without creating records")
    ap.add_argument("--limit", type=int, help="Limit number of rows to import (testing)")
    ap.add_argument("--skip", type=int, default=0, help="Skip the first N NDJSON rows (resume-ish)")
    ap.add_argument("--mapping-out", help="Where to write/read the HCP→SM8 id map (default: ./sm8_mappings/staff_map.json)")
    args = ap.parse_args()

    ndjson_dir = pathlib.Path(args.ndjson_dir).resolve()
    if not ndjson_dir.exists():
        sys.exit(f"NDJSON dir not found: {ndjson_dir}")

    # Build client
    client = SM8Client()

    # Paths
    employees_file = ndjson_dir / "employees.ndjson"
    mapping_path = pathlib.Path(args.mapping_out) if args.mapping_out else pathlib.Path("./sm8_mappings/staff_map.json")

    if args.only == "employees":
        import_employees(
            employees_file,
            client,
            dry_run=args.dry_run,
            limit=args.limit,
            skip=args.skip,
            mapping_path=mapping_path,
        )
    else:
        print("[INFO] Nothing to do (unknown entity).")

if __name__ == "__main__":
    main()
