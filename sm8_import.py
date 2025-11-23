#!/usr/bin/env python3
"""
ServiceM8 Import & Dump Tool

Features:
  - Logging controls:
      --quiet (hide INFO), --silent (hide almost everything),
      --log-file PATH (write logs to file), --no-console (no terminal logging).
  - Built-in rate limiter to avoid throttling (default --rpm 120).
  - Strict /company.json payload (documented fields only).
  - Sites are companies with parent_company_uuid; dedupe via parent+address.
  - Dump mode to export all Clients + Company Contacts to one JSON file.
  - Per-run, timestamped audit and dump file names.
  - For individual clients, also creates a BILLING + primary Company Contact.
  - Reactivation tools:
      --activate-inactive {off,clients,contacts,both}
      --reactivate-from {live,hcp}
    Respects --limit for how many records to activate.
  - Address merge on existing *individual* clients:
      --merge-address {off,missing,always} (default: missing)
    missing: only fills blank addr fields from HCP
    always:  overwrites addr fields (when HCP has a value)
    off:     no address changes on existing records

Usage examples:
  # Dump all without printing to terminal, write logs to file, paced at 120 rpm
  python sm8_import.py --dump-all --dump-file ./sm8_export/ \
    --audit-file ./audit/dump.ndjson --no-console --log-file ./logs/run.log --rpm 120

  # Customers only, dry-run, detailed audit on newest export
  python sm8_import.py --latest --only customers --limit 5 \
    --dry-run --audit-file ./audit/customers.ndjson --audit-detail --rpm 120

  # Reactivate up to 3 inactive records found by matching your HCP export (customers.ndjson)
  python sm8_import.py --activate-inactive both --reactivate-from hcp --latest \
    --limit 3 --audit-file ./audit/reactivate.ndjson --audit-detail --rpm 120
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib
import random
import re
import sys
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional, Tuple
from collections import deque

import requests
from requests import Session
from requests.exceptions import (
    ReadTimeout,
    ConnectTimeout,
    ConnectionError as ReqConnectionError,
    ChunkedEncodingError,
)
from urllib3.exceptions import ProtocolError
from dotenv import load_dotenv, find_dotenv

# ---------------- Config & Globals ----------------

SM8_BASE_URL = "https://api.servicem8.com/api_1.0"
TIMESTAMP_DIR_RE = re.compile(r"^\d{8}T\d{6}Z$")

TRANSIENT_STATUSES = {408, 425, 429, 500, 502, 503, 504}
TRANSIENT_EXC = (ReadTimeout, ConnectTimeout, ReqConnectionError,
                 ChunkedEncodingError, ProtocolError)

DEFAULT_CONNECT_TIMEOUT = float(os.getenv("SM8_CONNECT_TIMEOUT", "15"))
DEFAULT_READ_TIMEOUT = float(os.getenv("SM8_READ_TIMEOUT", "90"))
DEFAULT_MAX_RETRIES = int(os.getenv("SM8_MAX_RETRIES", "8"))
DEFAULT_BACKOFF_CAP = float(os.getenv("SM8_BACKOFF_MAX", "60"))
DEFAULT_RPM = int(os.getenv("SM8_RPM", "120"))  # stay below 180/min

# Rate limiting globals
_GLOBAL_RPM: int = DEFAULT_RPM
_REQUEST_TIMES = deque()  # monotonic timestamps of requests (rolling 60s window)

# Logger
log = logging.getLogger("sm8-import")

# ---------------- Logging setup ----------------


def setup_logging(*, quiet: bool, silent: bool, log_file: Optional[str], no_console: bool) -> None:
    level = logging.INFO
    if silent:
        level = logging.CRITICAL
    elif quiet:
        level = logging.WARNING

    root = logging.getLogger()
    root.setLevel(level)

    # Remove existing handlers
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    # File handler
    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(fmt)
        root.addHandler(fh)

    # Console handler (unless disabled)
    if not no_console:
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(level)
        sh.setFormatter(fmt)
        root.addHandler(sh)

# ---------------- Env / Auth ----------------


def load_env(dotenv_path: Optional[str]) -> Optional[str]:
    used_path = None
    if dotenv_path:
        env_file = str(pathlib.Path(dotenv_path).resolve())
        if load_dotenv(env_file, override=False):
            used_path = env_file
    else:
        found = find_dotenv(usecwd=True)
        if found:
            load_dotenv(found, override=False)
            used_path = found
    if used_path:
        log.info("Loaded env from: %s", used_path)
    return used_path


def build_auth(headers: Dict[str, str], auth_mode: Optional[str]) -> Tuple[Dict[str, str], str]:
    forced = (auth_mode or "").strip().lower()
    oauth_token = os.getenv("SM8_OAUTH_TOKEN")
    api_key = os.getenv("SM8_API_KEY")

    if forced == "oauth":
        if not oauth_token:
            sys.exit("Auth mode forced to 'oauth' but SM8_OAUTH_TOKEN is missing.")
        headers["Authorization"] = f"Bearer {oauth_token}"
        log.info("Auth mode: oauth (Bearer token)")
        return headers, "oauth"

    if forced == "apikey":
        if not api_key:
            sys.exit("Auth mode forced to 'apikey' but SM8_API_KEY is missing.")
        headers["X-Api-Key"] = api_key
        log.info("Auth mode: apikey (X-Api-Key)")
        return headers, "apikey"

    if oauth_token:
        headers["Authorization"] = f"Bearer {oauth_token}"
        log.info("Auth mode: oauth (Bearer token) [auto]")
        return headers, "oauth"
    if api_key:
        headers["X-Api-Key"] = api_key
        log.info("Auth mode: apikey (X-Api-Key) [auto]")
        return headers, "apikey"

    sys.exit("Missing ServiceM8 credentials. Provide SM8_OAUTH_TOKEN or SM8_API_KEY.")

# ---------------- Path & NDJSON helpers ----------------


def list_timestamp_dirs(root: pathlib.Path) -> List[pathlib.Path]:
    if not root.exists():
        return []
    out: List[pathlib.Path] = []
    for child in root.iterdir():
        if child.is_dir() and TIMESTAMP_DIR_RE.match(child.name):
            out.append(child)
    return sorted(out, key=lambda p: p.name, reverse=True)


def resolve_ndjson_dir(base_or_run: Optional[str], latest: bool) -> pathlib.Path:
    """
    If latest is True, pick newest timestamped subfolder under base_or_run or ./hcp_export.
    Else, base_or_run must be a specific run directory.
    """
    if latest:
        base = pathlib.Path(base_or_run or "./hcp_export").resolve()
        runs = list_timestamp_dirs(base)
        if not runs:
            sys.exit(f"No timestamped runs found under: {base}")
        log.info("Using newest run: %s", runs[0])
        return runs[0]
    if not base_or_run:
        sys.exit("You must pass --ndjson-dir or --latest")
    return pathlib.Path(base_or_run).resolve()


def iter_ndjson(path: pathlib.Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                yield obj

# ---------------- Canonicalization & OData ----------------


def norm_email(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def norm_phone(s: Optional[str]) -> str:
    s = (s or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits


def _row_uuid(row: Dict[str, Any]) -> Optional[str]:
    return row.get("uuid") or row.get("id")


def _quote_odata_value(val: Any) -> str:
    if isinstance(val, str):
        return "'" + val.replace("'", "''") + "'"
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, (int, float)):
        return str(val)
    sval = str(val)
    return "'" + sval.replace("'", "''") + "'"


def odata_filter(conditions: List[Tuple[str, str, Any]]) -> str:
    """
    Build an OData $filter like: field1 eq 'X' and field2 gt 10
    Only 'and' is supported (per SM8 docs).
    """
    parts: List[str] = []
    for field, op, value in conditions:
        if value is None or value == "":
            continue
        parts.append(f"{field} {op} {_quote_odata_value(value)}")
    return " and ".join(parts)

# ---------------- Auditing & Timestamped files ----------------


def _roll_timestamped_file(base_path: Optional[pathlib.Path], *, label: str,
                           default_suffix: str = ".ndjson", default_name: str = "file") -> Optional[pathlib.Path]:
    """
    If base_path is a directory or path without an extension, append default_name+default_suffix inside it.
    Otherwise, treat base_path as a file path and roll <stem>_YYYYMMDDTHHMMSSZ<suffix>.
    """
    if not base_path:
        return None

    p = base_path.resolve()

    # If user gave a directory (or path lacking a suffix), write inside that dir with a default filename.
    if p.is_dir() or p.suffix == "" or str(base_path).endswith(("/", "\\")):
        p = p / f"{default_name}{default_suffix}"

    p.parent.mkdir(parents=True, exist_ok=True)

    stem = p.stem
    suffix = p.suffix or default_suffix
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    rolled = p.with_name(f"{stem}_{ts}{suffix}")
    rolled.touch()
    log.info("%s: %s", label, rolled)
    return rolled


def _append_audit(
    audit: Optional[pathlib.Path],
    row: Dict[str, Any],
    *,
    detail: bool = False,
    data: Any = None,
    response: Optional[requests.Response] = None,
) -> None:
    if not audit:
        return
    if detail:
        if isinstance(data, list):
            preview = None
            if data and isinstance(data[0], dict):
                preview = {k: data[0].get(k) for k in (
                    "uuid", "id", "name", "email", "first", "last") if k in data[0]}
            row.setdefault("response_detail", {})["first_row"] = preview
            row["response_detail"]["rows"] = len(data)
        elif isinstance(data, dict):
            row.setdefault("response_detail", {})[
                "uuid"] = data.get("uuid") or data.get("id")
            row["response_detail"]["snapshot"] = {k: data.get(k) for k in (
                "uuid", "id", "name", "email", "first", "last") if k in data}
        if response is not None:
            row.setdefault("response_detail", {})[
                "x_record_uuid"] = response.headers.get("x-record-uuid")
            row["response_detail"]["x_next_cursor"] = response.headers.get(
                "x-next-cursor")

    with audit.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

# ---------------- HTTP Client, Rate Limiting & Retries ----------------


_session: Optional[Session] = None


def get_session() -> Session:
    global _session
    if _session is None:
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20, pool_maxsize=40, max_retries=0)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _session = s
    return _session


def _enforce_rpm():
    """Ensure we do not exceed _GLOBAL_RPM requests in any rolling 60-second window."""
    rpm = _GLOBAL_RPM
    if not rpm or rpm <= 0:
        return
    now = time.monotonic()
    window = 60.0
    dq = _REQUEST_TIMES
    # prune old
    while dq and (now - dq[0]) > window:
        dq.popleft()
    if len(dq) >= rpm:
        sleep_for = window - (now - dq[0]) + random.uniform(0, 0.25)  # tiny jitter
        if sleep_for > 0:
            time.sleep(sleep_for)
        # prune again
        now = time.monotonic()
        while dq and (now - dq[0]) > window:
            dq.popleft()
    dq.append(time.monotonic())


def sm8_request(
    method: str,
    resource: str,
    headers: Dict[str, str],
    *,
    json_body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    retries: int = DEFAULT_MAX_RETRIES,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    read_timeout: float = DEFAULT_READ_TIMEOUT,
    backoff_cap: float = DEFAULT_BACKOFF_CAP,
    audit: Optional[pathlib.Path] = None,
    audit_detail: bool = False,
    entity: Optional[str] = None,
    action: Optional[str] = None,
    key: Optional[str] = None,
) -> Tuple[Optional[Any], requests.Response]:
    """
    Generic request wrapper for ServiceM8 API with retries on 429/5xx + network hiccups.
    Returns (parsed_json_or_None, Response). Raises on non-transient 4xx/5xx.
    """
    assert resource.startswith("/"), "resource should start with '/' (e.g. '/staff.json')"
    url = f"{SM8_BASE_URL}{resource}"
    session = get_session()
    backoff = 1.0
    last_exc: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            # pace requests BEFORE each attempt (retries included)
            _enforce_rpm()

            r = session.request(
                method=method.upper(),
                url=url,
                json=json_body,
                params=params,
                headers=headers,
                timeout=(connect_timeout, read_timeout),
            )

            # transient HTTP? (includes 429 throttling)
            if r.status_code in TRANSIENT_STATUSES:
                if attempt == retries:
                    snippet = (r.text or "")[:500]
                    _append_audit(audit, {
                        "ts": time.time(), "entity": entity, "action": action or "error", "key": key,
                        "method": method.upper(), "resource": resource, "status": r.status_code,
                        "request": {"params": params or {}, "json": json_body},
                        "response": {"text_snippet": snippet},
                    }, detail=audit_detail, response=r)
                    r.raise_for_status()
                retry_after = r.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    sleep_for = min(int(retry_after), backoff_cap)
                else:
                    sleep_for = min(backoff + random.uniform(0, backoff * 0.25), backoff_cap)
                log.warning("Transient HTTP %s on %s %s. Attempt %d/%d. Sleeping %.1fs.",
                            r.status_code, method.upper(), resource, attempt, retries, sleep_for)
                time.sleep(sleep_for)
                backoff = min(backoff * 2, backoff_cap)
                continue

            # Non-transient error
            if r.status_code >= 400:
                snippet = (r.text or "")[:500]
                log.error("%s %s -> HTTP %s :: %s", method.upper(), resource, r.status_code, snippet)
                _append_audit(audit, {
                    "ts": time.time(), "entity": entity, "action": action or "error", "key": key,
                    "method": method.upper(), "resource": resource, "status": r.status_code,
                    "request": {"params": params or {}, "json": json_body},
                    "response": {"text_snippet": snippet},
                }, detail=audit_detail, response=r)
                r.raise_for_status()

            try:
                data = r.json()
            except ValueError:
                data = None

            _append_audit(audit, {
                "ts": time.time(), "entity": entity, "action": action or method.lower(), "key": key,
                "method": method.upper(), "resource": resource, "status": r.status_code,
                "request": {"params": params or {}, "json": json_body},
                "response": {
                    "x_record_uuid": r.headers.get("x-record-uuid"),
                    "x_next_cursor": r.headers.get("x-next-cursor"),
                    "uuid": (data or {}).get("uuid") if isinstance(data, dict) else None,
                    "count": len(data) if isinstance(data, list) else (1 if data else 0),
                },
            }, detail=audit_detail, data=data, response=r)
            return data, r

        except TRANSIENT_EXC as e:
            last_exc = e
            if attempt == retries:
                break
            sleep_for = min(backoff + random.uniform(0, backoff * 0.25), backoff_cap)
            log.warning("Network error (%s) on %s %s. Attempt %d/%d. Sleeping %.1fs.",
                        type(e).__name__, method.upper(), resource, attempt, retries, sleep_for)
            time.sleep(sleep_for)
            backoff = min(backoff * 2, backoff_cap)

    log.error("Exhausted %d retries for %s %s. Last error: %s",
              retries, method.upper(), resource, last_exc)
    _append_audit(audit, {
        "ts": time.time(), "entity": entity, "action": "error_retries_exhausted", "key": key,
        "method": method.upper(), "resource": resource, "error": str(last_exc),
    }, detail=audit_detail)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"sm8_request: exhausted {retries} attempts for {method.upper()} {resource}")

# ---------------- Lookups & Creation ----------------


def normalize_hcp_address(a: Dict[str, Any]) -> Dict[str, str]:
    if not a:
        return {}
    s1 = a.get("street") or a.get("street_line_1") or ""
    s2 = a.get("street_line_2") or ""
    street = f"{s1}\n{s2}".strip() if s2 else s1
    return {
        "street": street,
        "city": a.get("city") or "",
        "state": a.get("state") or "",
        "zip": a.get("zip") or a.get("postcode") or "",
        "country": a.get("country") or "",
    }


def extract_addresses(hcp_customer: Dict[str, Any]) -> Tuple[Dict[str, str], Dict[str, str]]:
    addrs = hcp_customer.get("addresses") or []
    service = next((a for a in addrs if (a.get("type") or "").lower() == "service"), None)
    billing = next((a for a in addrs if (a.get("type") or "").lower() == "billing"), None)
    
    norm_service = normalize_hcp_address(service)
    norm_billing = normalize_hcp_address(billing)
    
    # Fallback for main address (ServiceM8 address fields)
    main_addr = norm_service if any(norm_service.values()) else norm_billing
    
    return main_addr, norm_billing


def format_address_string(addr: Dict[str, str]) -> str:
    parts = []
    if addr.get("street"): parts.append(addr["street"])
    
    line2 = []
    if addr.get("city"): line2.append(addr["city"])
    if addr.get("state"): line2.append(addr["state"])
    if addr.get("zip"): line2.append(addr["zip"])
    
    if line2: parts.append(" ".join(line2))
    if addr.get("country"): parts.append(addr["country"])
    
    return "\n".join(parts)


def map_company_payload(
    *,
    name: str,
    address: Optional[Dict[str, str]] = None,
    billing_address: Optional[str] = None,
    is_individual: bool,
    parent_company_uuid: Optional[str] = None,
) -> Dict[str, Any]:
    """Build strict SM8 /company.json payload (Client or Site)."""
    address = address or {}
    obj: Dict[str, Any] = {
        "name": name,
        "address_street": address.get("street") or None,
        "address_city": address.get("city") or None,
        "address_state": address.get("state") or None,
        "address_postcode": address.get("zip") or None,
        "address_country": address.get("country") or None,
        "billing_address": billing_address or None,
        "is_individual": "1" if is_individual else "0",
    }
    if parent_company_uuid:
        obj["parent_company_uuid"] = parent_company_uuid
    return {k: v for k, v in obj.items() if v not in (None, "", [])}


def map_contact_payload(
    company_uuid: str,
    hcp_customer: Dict[str, Any],
    *,
    type_value: Optional[str] = None,
    primary: bool = False,
) -> Dict[str, Any]:
    first = (hcp_customer.get("first_name") or "").strip()
    last = (hcp_customer.get("last_name") or "").strip()
    email = (hcp_customer.get("email") or "").strip()
    
    raw_phone = hcp_customer.get("home_number") or hcp_customer.get("work_number")
    phone = norm_phone(raw_phone)
    
    raw_mobile = hcp_customer.get("mobile_number")
    mobile = norm_phone(raw_mobile)

    obj = {
        "company_uuid": company_uuid,
        "first": first or None,
        "last": last or None,
        "email": email or None,
        "phone": phone or None,
        "mobile": mobile or None,
    }
    if type_value:
        obj["type"] = type_value            # e.g. "BILLING"
    if primary:
        obj["is_primary_contact"] = "1"       # mark as primary
    return {k: v for k, v in obj.items() if v not in (None, "", [])}


def find_company_by_name(
    headers: Dict[str, str],
    *,
    name: str,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> Optional[str]:
    if not name:
        return None
    flt = odata_filter([("name", "eq", name)])
    data, _ = sm8_request("GET", "/company.json", headers,
                          params={"$filter": flt},
                          audit=audit, audit_detail=audit_detail,
                          entity="clients", action="lookup_row", key=flt)
    rows = data if isinstance(data, list) else []
    if rows:
        return _row_uuid(rows[0])
    return None


def find_site_by_address(
    headers: Dict[str, str],
    *,
    parent_uuid: str,
    street: str,
    city: str,
    state: str,
    postcode: str,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> Optional[str]:
    conds: List[Tuple[str, str, Any]] = [("parent_company_uuid", "eq", parent_uuid)]
    if street:
        conds.append(("address_street", "eq", street))
    if city:
        conds.append(("address_city", "eq", city))
    if state:
        conds.append(("address_state", "eq", state))
    if postcode:
        conds.append(("address_postcode", "eq", postcode))
    flt = odata_filter(conds)
    if not flt:
        return None
    data, _ = sm8_request("GET", "/company.json", headers,
                          params={"$filter": flt},
                          audit=audit, audit_detail=audit_detail,
                          entity="clients", action="lookup_site", key=f"parent={parent_uuid}")
    rows = data if isinstance(data, list) else []
    if rows:
        return _row_uuid(rows[0])
    return None


def find_contact(
    headers: Dict[str, str],
    *,
    first: str,
    last: str,
    email: str,
    phone: str,
    mobile: str,
    # scope search to a company when available
    company_uuid: Optional[str] = None,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> Optional[str]:
    base_conds: List[Tuple[str, str, Any]] = []
    if company_uuid:
        base_conds.append(("company_uuid", "eq", company_uuid))

    # Email
    if email:
        flt = odata_filter(base_conds + [("email", "eq", email)])
        data, _ = sm8_request("GET", "/companycontact.json", headers,
                              params={"$filter": flt},
                              audit=audit, audit_detail=audit_detail,
                              entity="company_contacts", action="lookup_row", key=flt)
        rows = data if isinstance(data, list) else []
        if rows:
            return _row_uuid(rows[0])

    # Phones
    for field, val in (("mobile", mobile), ("phone", phone)):
        if not val:
            continue
        flt = odata_filter(base_conds + [(field, "eq", val)])
        data, _ = sm8_request("GET", "/companycontact.json", headers,
                              params={"$filter": flt},
                              audit=audit, audit_detail=audit_detail,
                              entity="company_contacts", action="lookup_row", key=flt)
        rows = data if isinstance(data, list) else []
        if rows:
            return _row_uuid(rows[0])

    # Name
    if first or last:
        flt = odata_filter(base_conds + [("first", "eq", first), ("last", "eq", last)])
        data, _ = sm8_request("GET", "/companycontact.json", headers,
                              params={"$filter": flt},
                              audit=audit, audit_detail=audit_detail,
                              entity="company_contacts", action="lookup_row", key=flt)
        rows = data if isinstance(data, list) else []
        if rows:
            return _row_uuid(rows[0])
    return None


def create_company(
    headers: Dict[str, str],
    *,
    payload: Dict[str, Any],
    dry_run: bool,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> str:
    if dry_run:
        _append_audit(audit, {
            "ts": time.time(), "entity": "clients", "action": "dry_post", "key": payload.get("name"),
            "method": "POST", "resource": "/company.json", "status": 0,
            "request": {"json": payload},
        }, detail=audit_detail)
        return "dry-run-company-uuid"

    data, resp = sm8_request("POST", "/company.json", headers,
                             json_body=payload, audit=audit, audit_detail=audit_detail,
                             entity="clients", action="create", key=payload.get("name"))
    uuid = resp.headers.get("x-record-uuid") or (data or {}).get("uuid")
    if not uuid:
        raise RuntimeError("Company created but no uuid returned")
    return uuid


def create_contact(
    headers: Dict[str, str],
    *,
    payload: Dict[str, Any],
    dry_run: bool,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> Optional[str]:
    if dry_run:
        _append_audit(audit, {
            "ts": time.time(), "entity": "company_contacts", "action": "dry_post",
            "key": (payload.get("email") or payload.get("mobile") or payload.get("phone") or ""),
            "method": "POST", "resource": "/companycontact.json", "status": 0,
            "request": {"json": payload},
        }, detail=audit_detail)
        return "dry-run-contact-uuid"

    data, resp = sm8_request("POST", "/companycontact.json", headers,
                             json_body=payload, audit=audit, audit_detail=audit_detail,
                             entity="company_contacts", action="create",
                             key=(payload.get("email") or payload.get("mobile") or payload.get("phone") or ""))
    uuid = resp.headers.get("x-record-uuid") or (data or {}).get("uuid")
    return uuid

# ---------------- Address merge helper ----------------


def _fetch_company_by_uuid(headers: Dict[str, str], uuid: str,
                           audit: Optional[pathlib.Path], audit_detail: bool) -> Optional[Dict[str, Any]]:
    flt = odata_filter([("uuid", "eq", uuid)])
    data, _ = sm8_request("GET", "/company.json", headers,
                          params={"$filter": flt},
                          audit=audit, audit_detail=audit_detail,
                          entity="clients", action="get", key=uuid)
    rows = data if isinstance(data, list) else []
    return rows[0] if rows else None


def maybe_merge_address_into_individual(
    headers: Dict[str, str],
    *,
    company_uuid: str,
    hcp_address: Dict[str, str],
    mode: str,  # "off" | "missing" | "always"
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> bool:
    """
    For existing INDIVIDUAL client only: merge or overwrite address from HCP according to mode.
    Returns True if an update POST was made.
    """
    if mode == "off" or not any(hcp_address.values()):
        return False

    row = _fetch_company_by_uuid(headers, company_uuid, audit, audit_detail)
    if not row:
        return False
    if not (row.get("is_individual") in (1, True)):  # only touch individuals
        return False

    # Build patch
    mapping = [
        ("address_street", "street"),
        ("address_city", "city"),
        ("address_state", "state"),
        ("address_postcode", "zip"),
        ("address_country", "country"),
    ]
    patch: Dict[str, Any] = {"uuid": company_uuid}

    for field, akey in mapping:
        new_val = (hcp_address.get(akey) or "").strip()
        if not new_val:
            continue
        if mode == "always":
            patch[field] = new_val
        elif mode == "missing":
            cur = (row.get(field) or "").strip()
            if not cur:
                patch[field] = new_val

    if len(patch) > 1:
        sm8_request("POST", "/company.json", headers,
                    json_body=patch,
                    audit=audit, audit_detail=audit_detail,
                    entity="clients", action="merge_address", key=company_uuid)
        return True
    return False

# ---------------- Dump helpers ----------------


def _collect_all(
    headers: Dict[str, str],
    resource: str,
    *,
    entity: str,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> List[Dict[str, Any]]:
    """
    Collect all rows from an endpoint using cursor-based pagination.
    """
    cursor = "-1"
    out: List[Dict[str, Any]] = []
    while True:
        params = {"cursor": cursor}
        data, resp = sm8_request("GET", resource, headers,
                                 params=params,
                                 audit=audit, audit_detail=audit_detail,
                                 entity=entity, action="dump", key=f"cursor={cursor}")
        rows = data if isinstance(data, list) else []
        out.extend(rows)
        nxt = resp.headers.get("x-next-cursor")
        if not nxt:
            break
        cursor = nxt
    return out


def dump_clients_and_contacts(
    headers: Dict[str, str],
    *,
    out_path: pathlib.Path,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> None:
    """
    Dump all Clients and Company Contacts to a single JSON file:
      { "clients": [...], "company_contacts": [...] }
    """
    log.info("Dumping Clients and Company Contacts to %s ...", out_path)
    clients = _collect_all(headers, "/company.json",
                           entity="clients", audit=audit, audit_detail=audit_detail)
    contacts = _collect_all(headers, "/companycontact.json",
                            entity="company_contacts", audit=audit, audit_detail=audit_detail)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump({"clients": clients, "company_contacts": contacts},
                  f, ensure_ascii=False, indent=2)

    log.info("[DUMP SUMMARY] clients=%d company_contacts=%d -> %s",
             len(clients), len(contacts), out_path)

# ---------------- Reactivation helpers ----------------


def _activate_company_uuid(headers: Dict[str, str], uuid: str,
                           audit: Optional[pathlib.Path], audit_detail: bool) -> None:
    sm8_request("POST", "/company.json", headers,
                json_body={"uuid": uuid, "active": 1},
                audit=audit, audit_detail=audit_detail,
                entity="clients", action="activate", key=uuid)


def _activate_contact_uuid(headers: Dict[str, str], uuid: str,
                           audit: Optional[pathlib.Path], audit_detail: bool) -> None:
    sm8_request("POST", "/companycontact.json", headers,
                json_body={"uuid": uuid, "active": 1},
                audit=audit, audit_detail=audit_detail,
                entity="company_contacts", action="activate", key=uuid)


def reactivate_from_live(
    headers: Dict[str, str],
    *,
    scope: str,  # "clients" | "contacts" | "both"
    limit: Optional[int],
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> None:
    target = limit if (limit is not None and limit >= 0) else None
    done = 0

    def scan_and_activate(resource: str, entity: str, activator):
        nonlocal done, target
        cursor = "-1"
        while True:
            if target is not None and done >= target:
                return
            params = {"cursor": cursor, "$filter": "active eq 0"}
            data, resp = sm8_request("GET", resource, headers,
                                     params=params,
                                     audit=audit, audit_detail=audit_detail,
                                     entity=entity, action="scan", key=f"cursor={cursor}")
            rows = data if isinstance(data, list) else []
            for row in rows:
                if target is not None and done >= target:
                    break
                uuid = _row_uuid(row)
                if not uuid:
                    continue
                activator(headers, uuid, audit, audit_detail)
                done += 1
            if target is not None and done >= target:
                return
            nxt = resp.headers.get("x-next-cursor")
            if not nxt:
                break
            cursor = nxt

    if scope in ("clients", "both"):
        scan_and_activate("/company.json", "clients", _activate_company_uuid)
    if scope in ("contacts", "both"):
        scan_and_activate("/companycontact.json", "company_contacts", _activate_contact_uuid)


def reactivate_from_hcp(
    headers: Dict[str, str],
    *,
    run_dir: pathlib.Path,
    scope: str,  # "clients" | "contacts" | "both"
    limit: Optional[int],
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> None:
    src = run_dir / "customers.ndjson"
    if not src.exists():
        log.warning("customers.ndjson not found in %s; nothing to reactivate.", run_dir)
        return

    target = limit if (limit is not None and limit >= 0) else None
    activations = 0
    processed = 0

    for hcp in iter_ndjson(src):
        if target is not None and activations >= target:
            break

        processed += 1
        comp = (hcp.get("company") or "").strip()
        first = (hcp.get("first_name") or "").strip()
        last = (hcp.get("last_name") or "").strip()
        full_name = get_full_name(hcp)
        email = (hcp.get("email") or "").strip()

        # Try to reactivate the individual client (no company) by name
        tried = False
        if not comp and full_name:
            uuid = find_company_by_name(headers, name=full_name, audit=audit, audit_detail=audit_detail)
            tried = True
            if uuid:
                if scope in ("clients", "both"):
                    _activate_company_uuid(headers, uuid, audit, audit_detail)
                    activations += 1
                if scope in ("contacts", "both") and email:
                    # reactivate matching contact under this company if found by email
                    flt = odata_filter([("company_uuid", "eq", uuid), ("email", "eq", email)])
                    data, _ = sm8_request("GET", "/companycontact.json", headers,
                                          params={"$filter": flt},
                                          audit=audit, audit_detail=audit_detail,
                                          entity="company_contacts", action="lookup_row", key=flt)
                    rows = data if isinstance(data, list) else []
                    if rows:
                        c_uuid = _row_uuid(rows[0])
                        if c_uuid:
                            _activate_contact_uuid(headers, c_uuid, audit, audit_detail)

        # Optional: if company exists and scope includes clients, try by company name as well
        if comp and scope in ("clients", "both") and (target is None or activations < target):
            uuid = find_company_by_name(headers, name=comp, audit=audit, audit_detail=audit_detail)
            if uuid:
                _activate_company_uuid(headers, uuid, audit, audit_detail)
                activations += 1

        if target is not None and activations >= target:
            break

    log.info("[REACTIVATE] source=hcp scope=%s activations=%s processed_hcp_rows=%s",
             scope, activations, processed)

# ---------------- Importers ----------------


def map_job_status(hcp_status: str) -> str:
    s = (hcp_status or "").lower()
    if s in ("finished", "completed", "paid"):
        return "Completed"
    if s in ("canceled", "cancelled"):
        return "Unsuccessful"
    return "Work Order"


def deterministic_uuid(source_str: str) -> str:
    """
    Generate a deterministic UUID from a source string using UUID5.
    """
    # Arbitrary namespace UUID
    namespace = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
    return str(uuid.uuid5(namespace, source_str))


def create_job_contact(
    headers: Dict[str, str],
    *,
    payload: Dict[str, Any],
    dry_run: bool,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> str:
    if dry_run:
        _append_audit(audit, {
            "ts": time.time(), "entity": "job_contacts", "action": "dry_post", "key": payload.get("job_uuid"),
            "method": "POST", "resource": "/jobcontact.json", "status": 0,
            "request": {"json": payload},
        }, detail=audit_detail)
        return "dry-run-job-contact-uuid"

    data, resp = sm8_request("POST", "/jobcontact.json", headers,
                             json_body=payload, audit=audit, audit_detail=audit_detail,
                             entity="job_contacts", action="create", key=payload.get("job_uuid"))
    uuid = resp.headers.get("x-record-uuid") or (data or {}).get("uuid")
    return uuid


def create_job(
    headers: Dict[str, str],
    *,
    payload: Dict[str, Any],
    dry_run: bool,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> str:
    if dry_run:
        _append_audit(audit, {
            "ts": time.time(), "entity": "jobs", "action": "dry_post", "key": payload.get("job_description"),
            "method": "POST", "resource": "/job.json", "status": 0,
            "request": {"json": payload},
        }, detail=audit_detail)
        return "dry-run-job-uuid"

    data, resp = sm8_request("POST", "/job.json", headers,
                             json_body=payload, audit=audit, audit_detail=audit_detail,
                             entity="jobs", action="create", key=payload.get("job_description"))
    uuid = resp.headers.get("x-record-uuid") or (data or {}).get("uuid")
    if not uuid:
        raise RuntimeError("Job created but no uuid returned")
    return uuid


def create_job_payment(
    headers: Dict[str, str],
    *,
    payload: Dict[str, Any],
    dry_run: bool,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> str:
    if dry_run:
        _append_audit(audit, {
            "ts": time.time(), "entity": "job_payments", "action": "dry_post", "key": payload.get("job_uuid"),
            "method": "POST", "resource": "/jobpayment.json", "status": 0,
            "request": {"json": payload},
        }, detail=audit_detail)
        return "dry-run-payment-uuid"

    data, resp = sm8_request("POST", "/jobpayment.json", headers,
                             json_body=payload, audit=audit, audit_detail=audit_detail,
                             entity="job_payments", action="create", key=payload.get("job_uuid"))
    uuid = resp.headers.get("x-record-uuid") or (data or {}).get("uuid")
    return uuid


def create_note(
    headers: Dict[str, str],
    *,
    payload: Dict[str, Any],
    dry_run: bool,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> str:
    if dry_run:
        _append_audit(audit, {
            "ts": time.time(), "entity": "notes", "action": "dry_post", "key": payload.get("related_object_uuid"),
            "method": "POST", "resource": "/note.json", "status": 0,
            "request": {"json": payload},
        }, detail=audit_detail)
        return "dry-run-note-uuid"

    data, resp = sm8_request("POST", "/note.json", headers,
                             json_body=payload, audit=audit, audit_detail=audit_detail,
                             entity="notes", action="create", key=payload.get("related_object_uuid"))
    uuid = resp.headers.get("x-record-uuid") or (data or {}).get("uuid")
    return uuid


def create_attachment(
    headers: Dict[str, str],
    *,
    payload: Dict[str, Any],
    dry_run: bool,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> str:
    if dry_run:
        _append_audit(audit, {
            "ts": time.time(), "entity": "attachments", "action": "dry_post", "key": payload.get("attachment_name"),
            "method": "POST", "resource": "/attachment.json", "status": 0,
            "request": {"json": payload},
        }, detail=audit_detail)
        return "dry-run-attachment-uuid"

    data, resp = sm8_request("POST", "/attachment.json", headers,
                             json_body=payload, audit=audit, audit_detail=audit_detail,
                             entity="attachments", action="create", key=payload.get("attachment_name"))
    uuid = resp.headers.get("x-record-uuid") or (data or {}).get("uuid")
    return uuid


def upload_attachment_content(
    headers: Dict[str, str],
    *,
    attachment_uuid: str,
    file_content: bytes,
    dry_run: bool,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> None:
    resource = f"/attachment/{attachment_uuid}.file"
    if dry_run:
        _append_audit(audit, {
            "ts": time.time(), "entity": "attachments", "action": "dry_upload", "key": attachment_uuid,
            "method": "POST", "resource": resource, "status": 0,
            "request": {"size": len(file_content)},
        }, detail=audit_detail)
        return

    # Upload binary content
    # Note: ServiceM8 expects the file content as the body
    # We use a custom call here because sm8_request assumes JSON usually
    url = f"{SM8_BASE_URL}{resource}"
    session = get_session()
    
    # We need to respect rate limits even for uploads
    _enforce_rpm()
    
    try:
        r = session.post(
            url,
            data=file_content,
            headers=headers, # headers already have Auth
            timeout=(DEFAULT_CONNECT_TIMEOUT, 300.0) # longer read timeout for uploads
        )
        
        if r.status_code >= 400:
            log.error("Upload failed %s -> HTTP %s", resource, r.status_code)
            _append_audit(audit, {
                "ts": time.time(), "entity": "attachments", "action": "upload_error", "key": attachment_uuid,
                "method": "POST", "resource": resource, "status": r.status_code,
                "response": {"text": r.text[:200]},
            }, detail=audit_detail)
        else:
            _append_audit(audit, {
                "ts": time.time(), "entity": "attachments", "action": "upload", "key": attachment_uuid,
                "method": "POST", "resource": resource, "status": r.status_code,
            }, detail=audit_detail)

    except Exception as e:
        log.error("Upload exception %s: %s", resource, e)
        _append_audit(audit, {
            "ts": time.time(), "entity": "attachments", "action": "upload_exception", "key": attachment_uuid,
            "error": str(e),
        }, detail=audit_detail)


def get_full_name(hcp: Dict[str, Any]) -> str:
    first = (hcp.get("first_name") or "").strip()
    last = (hcp.get("last_name") or "").strip()
    if first or last:
        return f"{first} {last}".strip()
    email = (hcp.get("email") or "").strip()
    if email:
        return email.split("@")[0]
    return ""


def hcp_id_to_uuid(hcp_id: Optional[str]) -> Optional[str]:
    """
    Convert HCP ID (e.g. 'job_20c73b1fb00b4262bab76848102352ef') 
    to UUID (e.g. '20c73b1f-b00b-4262-bab7-6848102352ef').
    Returns None if format doesn't match 32-char hex suffix.
    """
    if not hcp_id:
        return None
    parts = hcp_id.split("_")
    hex_part = parts[-1] if len(parts) > 1 else hcp_id
    
    if len(hex_part) != 32:
        return None
        
    return f"{hex_part[:8]}-{hex_part[8:12]}-{hex_part[12:16]}-{hex_part[16:20]}-{hex_part[20:]}"


def import_employees(
    run_dir: pathlib.Path,
    headers: Dict[str, str],
    *,
    dry_run: bool,
    limit: Optional[int],
    skip: int,
    default_role_uuid: Optional[str],
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> None:
    src = run_dir / "employees.ndjson"
    if not src.exists():
        log.warning("employees.ndjson not found in %s; nothing to import.", run_dir)
        return

    log.info("Importing employees from %s", src)
    created = 0
    processed = 0

    existing_emails = set()
    try:
        cursor = "-1"
        while True:
            params = {"cursor": cursor}
            data, resp = sm8_request("GET", "/staff.json", headers,
                                     params=params, audit=audit, audit_detail=audit_detail,
                                     entity="staff", action="list", key=f"cursor={cursor}")
            rows = data if isinstance(data, list) else []
            for it in rows:
                em = norm_email(it.get("email"))
                if em:
                    existing_emails.add(em)
            nxt = resp.headers.get("x-next-cursor")
            if not nxt:
                break
            cursor = nxt
    except requests.HTTPError:
        pass

    for hcp in iter_ndjson(src):
        processed += 1
        if processed <= max(skip, 0):
            continue
        if limit is not None and (processed - max(skip, 0)) > max(limit, 0):
            break

        first = (hcp.get("first_name") or "").strip()[:30] or "Unknown"
        last = (hcp.get("last_name") or "").strip()[:30] or "Staff"
        email = norm_email(hcp.get("email"))
        mobile = hcp.get("mobile_number") or hcp.get("phone") or ""
        job_title = (hcp.get("role") or hcp.get("title") or "").strip()
        color = (hcp.get("color_hex") or "").strip()
        if color and not color.startswith("#"):
            color = f"#{color}"

        staff = {
            "first": first,
            "last": last,
            "email": email or f"noemail+{processed}@example.invalid",
        }
        if mobile:
            staff["mobile"] = mobile
        if job_title:
            staff["job_title"] = job_title
        if color:
            staff["color"] = color
        role_uuid = os.getenv("SM8_DEFAULT_ROLE_UUID") or default_role_uuid
        if role_uuid:
            staff["security_role_uuid"] = role_uuid

        if dry_run:
            _append_audit(audit, {
                "ts": time.time(), "entity": "staff", "action": "dry_post", "key": staff.get("email"),
                "method": "POST", "resource": "/staff.json", "status": 0,
                "request": {"json": staff},
            }, detail=audit_detail)
            created += 1
            continue

        if email and email in existing_emails:
            continue

        try:
            _, resp = sm8_request("POST", "/staff.json", headers,
                                  json_body=staff, audit=audit, audit_detail=audit_detail,
                                  entity="staff", action="create", key=staff.get("email"))
            if email:
                existing_emails.add(email)
            created += 1
        except requests.HTTPError:
            pass

    log.info("[SUMMARY] employees processed=%d created=%d skipped=%d",
             processed, created, processed - created)


def import_customers(
    run_dir: pathlib.Path,
    headers: Dict[str, str],
    *,
    dry_run: bool,
    limit: Optional[int],
    skip: int,
    merge_address_mode: str,  # "off" | "missing" | "always"
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> None:
    """
    Rules:
      - If no company: create/find a single Client (individual), THEN create a BILLING + primary Company Contact.
        If client existed, optionally merge address per --merge-address.
      - If company present: upsert Company Client (by name), upsert Site by address (parent_company_uuid + address_*),
        upsert Company Contact (dedupe by email OR phone/mobile OR first+last).
      - No heavy preloads: on-demand OData lookups per record.
    """
    src = run_dir / "customers.ndjson"
    if not src.exists():
        log.warning("customers.ndjson not found in %s; nothing to import.", run_dir)
        return

    log.info("Importing customers from %s", src)

    processed = 0
    created_clients = 0
    created_sites = 0
    created_contacts = 0
    merged_addresses = 0

    # Cache for company UUIDs to avoid repeated lookups
    # Key: company name (or individual name) -> Value: UUID
    company_cache: Dict[str, str] = {}

    for hcp in iter_ndjson(src):
        processed += 1
        if processed <= max(skip, 0):
            continue
        if limit is not None and (processed - max(skip, 0)) > max(limit, 0):
            break

        comp = (hcp.get("company") or "").strip()
        first = (hcp.get("first_name") or "").strip()
        last = (hcp.get("last_name") or "").strip()
        full_name = get_full_name(hcp)
        email = (hcp.get("email") or "").strip()
        
        # Normalize phones for lookup to match storage format
        phone = norm_phone(hcp.get("home_number") or hcp.get("work_number"))
        mobile = norm_phone(hcp.get("mobile_number"))
        
        main_addr, billing_addr_dict = extract_addresses(hcp)
        billing_addr_str = format_address_string(billing_addr_dict)

        # No company: single Client (individual) + create a BILLING primary contact
        if not comp:
            client_name = full_name or "Unknown Customer"

            # find or create the individual client
            if client_name in company_cache:
                client_uuid = company_cache[client_name]
            else:
                client_uuid = find_company_by_name(headers, name=client_name, audit=audit, audit_detail=audit_detail)
                if client_uuid:
                    company_cache[client_name] = client_uuid

            created_now = False
            if not client_uuid:
                client_payload = map_company_payload(
                    name=client_name,
                    address=main_addr,
                    billing_address=billing_addr_str,
                    is_individual=True
                )
                client_uuid = create_company(
                    headers, payload=client_payload, dry_run=dry_run,
                    audit=audit, audit_detail=audit_detail
                )
                created_clients += 1
                created_now = True
                if client_uuid:
                    company_cache[client_name] = client_uuid

            # address merge (only for existing individual clients and when we have an address)
            if not created_now and any(main_addr.values()):
                merged = maybe_merge_address_into_individual(
                    headers,
                    company_uuid=client_uuid,
                    hcp_address=main_addr,
                    mode=merge_address_mode,
                    audit=audit,
                    audit_detail=audit_detail,
                )
                if merged:
                    merged_addresses += 1

            # create/dedupe the BILLING + primary contact for the individual
            contact_uuid = find_contact(
                headers,
                first=first, last=last, email=email, phone=phone, mobile=mobile,
                company_uuid=client_uuid,
                audit=audit, audit_detail=audit_detail
            )
            if not contact_uuid:
                contact_payload = map_contact_payload(
                    client_uuid, hcp, type_value="BILLING", primary=True
                )
                contact_uuid = create_contact(
                    headers, payload=contact_payload, dry_run=dry_run,
                    audit=audit, audit_detail=audit_detail
                )
                if contact_uuid:
                    created_contacts += 1

            continue

        # Company client (head office)
        if comp in company_cache:
            parent_uuid = company_cache[comp]
        else:
            parent_uuid = find_company_by_name(headers, name=comp, audit=audit, audit_detail=audit_detail)
            if parent_uuid:
                company_cache[comp] = parent_uuid

        if not parent_uuid:
            company_payload = map_company_payload(
                name=comp,
                is_individual=False
            )
            parent_uuid = create_company(
                headers, payload=company_payload, dry_run=dry_run, audit=audit, audit_detail=audit_detail)
            created_clients += 1
            if parent_uuid:
                company_cache[comp] = parent_uuid

        # Site (by address) — dedupe on parent + address fields
        site_uuid: Optional[str] = None
        if any(main_addr.values()):
            site_uuid = find_site_by_address(headers, parent_uuid=parent_uuid,
                                             street=main_addr.get("street", ""), city=main_addr.get("city", ""),
                                             state=main_addr.get("state", ""), postcode=main_addr.get("zip", ""),
                                             audit=audit, audit_detail=audit_detail)
            if not site_uuid:
                site_payload = map_company_payload(
                    name=comp,
                    address=main_addr,
                    is_individual=False,
                    parent_company_uuid=parent_uuid
                )
                site_uuid = create_company(
                    headers, payload=site_payload, dry_run=dry_run, audit=audit, audit_detail=audit_detail)
                created_sites += 1

        # Company contact (keeps email/phone/mobile)
        contact_uuid = find_contact(headers, first=first, last=last, email=email,
                                    phone=phone, mobile=mobile, company_uuid=parent_uuid,
                                    audit=audit, audit_detail=audit_detail)
        if not contact_uuid:
            contact_payload = map_contact_payload(parent_uuid, hcp)
            contact_uuid = create_contact(
                headers, payload=contact_payload, dry_run=dry_run, audit=audit, audit_detail=audit_detail)
            if contact_uuid:
                created_contacts += 1

    log.info("[SUMMARY] customers processed=%d created_clients=%d created_sites=%d "
             "created_contacts=%d merged_addresses=%d",
             processed, created_clients, created_sites, created_contacts, merged_addresses)


def preload_payments(run_dir: pathlib.Path) -> Dict[str, List[Dict[str, Any]]]:
    """
    Load all payments from invoices.ndjson into a dict keyed by HCP Job ID.
    """
    src = run_dir / "invoices.ndjson"
    if not src.exists():
        return {}
    
    out: Dict[str, List[Dict[str, Any]]] = {}
    for row in iter_ndjson(src):
        job_id = row.get("job_id")
        payments = row.get("payments")
        if job_id and payments:
            if job_id not in out:
                out[job_id] = []
            # Attach the invoice ID to the payment for uniqueness generation later
            for p in payments:
                p["_invoice_id"] = row.get("id")
            out[job_id].extend(payments)
    return out


def import_jobs(
    run_dir: pathlib.Path,
    headers: Dict[str, str],
    *,
    dry_run: bool,
    limit: Optional[int],
    skip: int,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
    target_job_id: Optional[str] = None,
) -> None:
    src = run_dir / "jobs.ndjson"
    if not src.exists():
        log.warning("jobs.ndjson not found in %s; nothing to import.", run_dir)
        return

    # Preload payments
    job_payments_map = preload_payments(run_dir)
    log.info("Preloaded payments for %d jobs", len(job_payments_map))

    log.info("Importing jobs from %s", src)
    processed = 0
    created_jobs = 0
    created_notes = 0
    created_contacts = 0
    created_payments = 0
    created_attachments = 0
    skipped_no_company = 0

    company_cache: Dict[str, str] = {}

    for hcp in iter_ndjson(src):
        if target_job_id and hcp.get("id") != target_job_id:
            continue

        processed += 1
        if processed <= max(skip, 0):
            continue
        if limit is not None and (processed - max(skip, 0)) > max(limit, 0):
            break

        # 1. Find Company
        cust = hcp.get("customer") or {}
        comp_name = (cust.get("company") or "").strip()
        if not comp_name:
            # Fallback to individual name
            first = (cust.get("first_name") or "").strip()
            last = (cust.get("last_name") or "").strip()
            if first or last:
                comp_name = f"{first} {last}".strip()
            else:
                # Try email?
                email = (cust.get("email") or "").strip()
                if email:
                    comp_name = email.split("@")[0]
        
        if not comp_name:
            log.warning("Job %s has no customer name/company. Skipping.", hcp.get("id"))
            skipped_no_company += 1
            continue

        if comp_name in company_cache:
            company_uuid = company_cache[comp_name]
        else:
            company_uuid = find_company_by_name(headers, name=comp_name, audit=audit, audit_detail=audit_detail)
            if company_uuid:
                company_cache[comp_name] = company_uuid
        
        if not company_uuid:
            # If we are targeting a specific job, try to create the customer on the fly
            if target_job_id:
                log.info("Targeted Job: Customer '%s' not found. Creating on-the-fly...", comp_name)
                
                # Prepare address
                job_addr_raw = hcp.get("address") or {}
                norm_addr = normalize_hcp_address(job_addr_raw)
                billing_addr_str = format_address_string(norm_addr)

                # Prepare customer data
                cust_data = hcp.get("customer") or {}
                is_individual = not bool(cust_data.get("company"))
                
                # Create Client
                client_payload = map_company_payload(
                    name=comp_name,
                    address=norm_addr,
                    billing_address=billing_addr_str,
                    is_individual=is_individual
                )
                company_uuid = create_company(
                    headers, payload=client_payload, dry_run=dry_run,
                    audit=audit, audit_detail=audit_detail
                )
                
                # Create Contact
                # cust_data has first_name, last_name, etc.
                contact_payload = map_contact_payload(
                    company_uuid, cust_data, type_value="BILLING", primary=True
                )
                create_contact(
                    headers, payload=contact_payload, dry_run=dry_run,
                    audit=audit, audit_detail=audit_detail
                )
                
                # Cache it
                if company_uuid:
                    company_cache[comp_name] = company_uuid

            else:
                log.warning("Company '%s' not found for Job %s. Skipping.", comp_name, hcp.get("id"))
                skipped_no_company += 1
                continue

        # 2. Prepare Job Payload
        addr = normalize_hcp_address(hcp.get("address") or {})
        status = map_job_status(hcp.get("work_status"))
        desc = hcp.get("description") or "Imported Job"
        
        # Date handling
        job_created_at = hcp.get("created_at")
        formatted_date = None
        if job_created_at:
            # 2025-10-21T17:03:54Z -> 2025-10-21
            formatted_date = job_created_at.split("T")[0]

        # Deterministic UUID from HCP ID
        hcp_job_id = hcp.get("id")
        deterministic_job_uuid = hcp_id_to_uuid(hcp_job_id)

        job_payload = {
            "uuid": deterministic_job_uuid,
            "company_uuid": company_uuid,
            "job_description": desc,
            "status": status,
            "address_street": addr.get("street"),
            "address_city": addr.get("city"),
            "address_state": addr.get("state"),
            "address_postcode": addr.get("zip"),
            "address_country": addr.get("country"),
            "job_address": format_address_string(addr),
            "date": formatted_date,
        }
        # Clean None values
        job_payload = {k: v for k, v in job_payload.items() if v}

        # 3. Create Job
        job_uuid = create_job(headers, payload=job_payload, dry_run=dry_run, audit=audit, audit_detail=audit_detail)
        created_jobs += 1

        # 4. Create Job Contact
        # Use customer info from the job record
        c_first = (cust.get("first_name") or "").strip()
        c_last = (cust.get("last_name") or "").strip()
        c_email = (cust.get("email") or "").strip()
        c_mobile = norm_phone(cust.get("mobile_number"))
        c_phone = norm_phone(cust.get("home_number") or cust.get("work_number"))

        if c_first or c_last:
            # Generate a deterministic UUID for the contact to avoid duplicates
            # Key: job_uuid + "contact" + email/phone
            contact_key = f"{job_uuid}_contact_{c_email}_{c_mobile}"
            contact_uuid = deterministic_uuid(contact_key)

            contact_payload = {
                "uuid": contact_uuid,
                "job_uuid": job_uuid,
                "first": c_first,
                "last": c_last,
                "email": c_email,
                "mobile": c_mobile,
                "phone": c_phone,
                "type": "JOB"
            }
            contact_payload = {k: v for k, v in contact_payload.items() if v}
            create_job_contact(headers, payload=contact_payload, dry_run=dry_run, audit=audit, audit_detail=audit_detail)
            created_contacts += 1

        # 5. Create Notes
        formatted_ts = None
        if job_created_at:
             formatted_ts = job_created_at.replace("T", " ").replace("Z", "")

        notes = hcp.get("notes") or []
        for i, n in enumerate(notes):
            content = (n.get("content") or "").strip()
            if not content:
                continue
            
            # Deterministic UUID for note
            note_key = f"{job_uuid}_note_{i}_{content[:20]}"
            note_uuid = deterministic_uuid(note_key)

            note_payload = {
                "uuid": note_uuid,
                "related_object": "job",
                "related_object_uuid": job_uuid,
                "note": content,
                "action_required": "0",
                "create_date": formatted_ts,
            }
            # Clean None values
            note_payload = {k: v for k, v in note_payload.items() if v}

            create_note(headers, payload=note_payload, dry_run=dry_run, audit=audit, audit_detail=audit_detail)
            created_notes += 1

        # 6. Create Payments
        if hcp_job_id in job_payments_map:
            payments = job_payments_map[hcp_job_id]
            for i, p in enumerate(payments):
                amount_cents = p.get("amount") or 0
                amount_str = f"{amount_cents / 100:.2f}"
                
                paid_at = p.get("paid_at")
                p_formatted_ts = None
                if paid_at:
                    p_formatted_ts = paid_at.replace("T", " ").replace("Z", "")
                
                # Deterministic UUID for payment
                # Use invoice ID + index to ensure uniqueness
                inv_id = p.get("_invoice_id") or "unknown_invoice"
                payment_key = f"{job_uuid}_payment_{inv_id}_{i}"
                payment_uuid = deterministic_uuid(payment_key)

                payment_payload = {
                    "uuid": payment_uuid,
                    "job_uuid": job_uuid,
                    "amount": amount_str,
                    "method": p.get("payment_method") or "Other",
                    "timestamp": p_formatted_ts,
                    "note": p.get("note"),
                }
                # Clean None values
                payment_payload = {k: v for k, v in payment_payload.items() if v}

                create_job_payment(headers, payload=payment_payload, dry_run=dry_run, audit=audit, audit_detail=audit_detail)
                created_payments += 1

        # 7. Create Attachments
        attachments = hcp.get("attachments") or []
        for i, att in enumerate(attachments):
            url = att.get("url")
            if not url:
                continue
            
            file_name = att.get("file_name") or f"attachment_{i}"
            file_type = att.get("file_type") or "application/octet-stream"
            
            # Deterministic UUID for attachment
            # Key: job_uuid + "attachment" + file_name + index
            att_key = f"{job_uuid}_attachment_{file_name}_{i}"
            att_uuid = deterministic_uuid(att_key)
            
            att_payload = {
                "uuid": att_uuid,
                "related_object": "job",
                "related_object_uuid": job_uuid,
                "attachment_name": file_name,
                "file_type": file_type,
                "attachment_source": "JOB",
                "tags": "HCP Import",
            }
            
            # Create metadata
            create_attachment(headers, payload=att_payload, dry_run=dry_run, audit=audit, audit_detail=audit_detail)
            
            # Download and Upload content
            if not dry_run:
                try:
                    # Download from S3
                    dl_resp = requests.get(url, timeout=60)
                    if dl_resp.status_code == 200:
                        content = dl_resp.content
                        upload_attachment_content(headers, attachment_uuid=att_uuid, file_content=content, 
                                                  dry_run=dry_run, audit=audit, audit_detail=audit_detail)
                        created_attachments += 1
                    else:
                        msg = f"HTTP {dl_resp.status_code}"
                        if dl_resp.status_code == 403:
                            msg += " (Link likely expired - get a fresh export)"
                        log.warning("Failed to download attachment %s: %s", file_name, msg)
                except Exception as e:
                    log.warning("Exception downloading attachment %s: %s", file_name, e)
            else:
                # Dry run: pretend we uploaded
                upload_attachment_content(headers, attachment_uuid=att_uuid, file_content=b"dry_run_content", 
                                          dry_run=True, audit=audit, audit_detail=audit_detail)
                created_attachments += 1

    log.info("[SUMMARY] jobs processed=%d created_jobs=%d created_notes=%d created_contacts=%d created_payments=%d created_attachments=%d skipped_no_company=%d",
             processed, created_jobs, created_notes, created_contacts, created_payments, created_attachments, skipped_no_company)



def import_payments(
    run_dir: pathlib.Path,
    headers: Dict[str, str],
    *,
    dry_run: bool,
    limit: Optional[int],
    skip: int,
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> None:
    src = run_dir / "invoices.ndjson"
    if not src.exists():
        log.warning("invoices.ndjson not found in %s; nothing to import.", run_dir)
        return

    log.info("Importing payments from %s", src)
    processed = 0
    created_payments = 0
    skipped_no_job = 0

    for inv in iter_ndjson(src):
        payments = inv.get("payments") or []
        if not payments:
            continue
        
        hcp_job_id = inv.get("job_id")
        if not hcp_job_id:
            continue

        processed += 1
        if processed <= max(skip, 0):
            continue
        if limit is not None and (processed - max(skip, 0)) > max(limit, 0):
            break

        # Deterministic UUID from HCP Job ID
        job_uuid = hcp_id_to_uuid(hcp_job_id)
        
        if not job_uuid:
            log.warning("Could not derive UUID from HCP Job ID %s. Skipping payments.", hcp_job_id)
            skipped_no_job += 1
            continue

        for p in payments:
            amount_cents = p.get("amount") or 0
            amount_str = f"{amount_cents / 100:.2f}"
            
            paid_at = p.get("paid_at")
            formatted_ts = None
            if paid_at:
                formatted_ts = paid_at.replace("T", " ").replace("Z", "")
            
            payment_payload = {
                "job_uuid": job_uuid,
                "amount": amount_str,
                "method": p.get("payment_method") or "Other",
                "timestamp": formatted_ts,
                "note": p.get("note"),
            }
            # Clean None values
            payment_payload = {k: v for k, v in payment_payload.items() if v}

            create_job_payment(headers, payload=payment_payload, dry_run=dry_run, audit=audit, audit_detail=audit_detail)
            created_payments += 1

    log.info("[SUMMARY] invoices_with_payments_processed=%d created_payments=%d skipped_no_job=%d",
             processed, created_payments, skipped_no_job)


# ---------------- CLI ----------------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Import Housecall Pro NDJSON into ServiceM8, dump, reactivate, and optionally merge addresses for existing clients")

    # Logging controls
    ap.add_argument("--quiet", action="store_true", help="Hide INFO logs (only warnings & errors)")
    ap.add_argument("--silent", action="store_true", help="Hide almost all logging (critical only)")
    ap.add_argument("--log-file", help="Write logs to this file instead of console")
    ap.add_argument("--no-console", action="store_true", help="Disable terminal logging entirely")

    # Rate limiting
    ap.add_argument("--rpm", type=int, default=DEFAULT_RPM,
                    help="Max requests per minute (default: 120, 0 disables)")

    # Dump mode
    ap.add_argument("--dump-all", action="store_true",
                    help="Fetch all Clients and Company Contacts to a single JSON file and exit")
    ap.add_argument("--dump-file",
                    help="Where to write the dump JSON (a timestamped copy will be created). Default: ./sm8_export/")

    # Reactivation mode
    ap.add_argument("--activate-inactive", choices=["off", "clients", "contacts", "both"], default="off",
                    help="Reactivate inactive items (default: off)")
    ap.add_argument("--reactivate-from", choices=["live", "hcp"],
                    help="When reactivating: 'live' scans SM8 inactive; 'hcp' matches names from your HCP export")

    # Import mode
    ap.add_argument("--ndjson-dir",
                    help="Path to NDJSON run dir (or base dir when used with --latest). Default ./hcp_export")
    ap.add_argument("--latest", action="store_true",
                    help="Pick newest timestamped subfolder under --ndjson-dir (or ./hcp_export)")
    ap.add_argument("--only", choices=["employees", "customers", "jobs", "payments"], default="employees", help="Entity to import")
    ap.add_argument("--dry-run", action="store_true", help="Print actions but do not POST")
    ap.add_argument("--limit", type=int, help="Import at most N records (per entity) OR max activations for reactivate")
    ap.add_argument("--skip", type=int, default=0, help="Skip the first N source records")
    ap.add_argument("--dotenv", help="Path to .env file to load (otherwise auto-discovered)")
    ap.add_argument("--auth-mode", choices=["oauth", "apikey"], help="Force auth mode (otherwise auto-detect)")
    ap.add_argument("--audit-file", help="Write NDJSON audit lines to a new timestamped file based on this path")
    ap.add_argument("--audit-detail", action="store_true",
                    help="Include response previews and key headers in audit lines")
    ap.add_argument("--job-id", help="Filter import to a specific Job ID (for testing)")

    # Address merge behavior for existing individual clients
    ap.add_argument("--merge-address", choices=["off", "missing", "always"], default="missing",
                    help="For existing individual clients, fill address from HCP (missing) or overwrite (always)")

    args = ap.parse_args()

    # Configure logging early
    setup_logging(quiet=args.quiet, silent=args.silent, log_file=args.log_file, no_console=args.no_console)

    # Rate limit parameter
    global _GLOBAL_RPM
    _GLOBAL_RPM = max(0, int(args.rpm or 0))

    # Load env
    load_env(args.dotenv)

    # Build headers
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "sm8-import/1.7 (+https://servicem8.com)",
    }
    headers, _mode = build_auth(headers, args.auth_mode)

    # Audit setup (roll a new file each run if provided)
    audit_base = pathlib.Path(args.audit_file).resolve() if args.audit_file else None
    audit_path = _roll_timestamped_file(audit_base, label="Audit file")  # may be None

    # REACTIVATION MODE (early exit)
    if args.activate_inactive != "off":
        if args.reactivate_from == "hcp":
            # Need run dir to read customers.ndjson
            run_dir = resolve_ndjson_dir(args.ndjson_dir, args.latest)
            log.info("HCP reactivation using NDJSON dir: %s", run_dir)
            reactivate_from_hcp(headers, run_dir=run_dir, scope=args.activate_inactive,
                                limit=args.limit, audit=audit_path, audit_detail=args.audit_detail)
        else:
            # live scan (no NDJSON needed)
            reactivate_from_live(headers, scope=args.activate_inactive,
                                 limit=args.limit, audit=audit_path, audit_detail=args.audit_detail)
        return

    # DUMP MODE
    if args.dump_all:
        # Default to a directory, but create a file IN it: clients_contacts.json
        dump_base = pathlib.Path(args.dump_file).resolve() if args.dump_file else pathlib.Path("./sm8_export/").resolve()
        dump_path = _roll_timestamped_file(dump_base, label="Dump file", default_suffix=".json",
                                           default_name="clients_contacts")
        if dump_path is None:
            sys.exit("Internal error: dump file path could not be resolved.")
        dump_clients_and_contacts(headers, out_path=dump_path, audit=audit_path, audit_detail=args.audit_detail)
        return

    # IMPORT MODE
    run_dir = resolve_ndjson_dir(args.ndjson_dir, args.latest)
    log.info("Using NDJSON dir: %s", run_dir)
    if not run_dir.exists():
        sys.exit(f"NDJSON dir not found: {run_dir}")

    default_role_uuid = os.getenv("SM8_DEFAULT_ROLE_UUID") or None

    if args.only == "employees":
        import_employees(
            run_dir, headers,
            dry_run=args.dry_run, limit=args.limit, skip=args.skip,
            default_role_uuid=default_role_uuid,
            audit=audit_path, audit_detail=args.audit_detail,
        )
    elif args.only == "customers":
        import_customers(
            run_dir, headers,
            dry_run=args.dry_run, limit=args.limit, skip=args.skip,
            merge_address_mode=args.merge_address,
            audit=audit_path, audit_detail=args.audit_detail,
        )
    elif args.only == "jobs":
        import_jobs(
            run_dir, headers,
            dry_run=args.dry_run, limit=args.limit, skip=args.skip,
            audit=audit_path, audit_detail=args.audit_detail,
            target_job_id=args.job_id,
        )
    elif args.only == "payments":
        import_payments(
            run_dir, headers,
            dry_run=args.dry_run, limit=args.limit, skip=args.skip,
            audit=audit_path, audit_detail=args.audit_detail,
        )
    else:
        log.warning("No importer implemented for the selected entity.")


if __name__ == "__main__":
    main()