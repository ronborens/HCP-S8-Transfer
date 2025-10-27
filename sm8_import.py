#!/usr/bin/env python3
"""
ServiceM8 Import & Dump Tool

What's new:
  - Logging controls:
      --quiet (hide INFO), --silent (hide almost everything),
      --log-file PATH (write logs to file), --no-console (no terminal logging).
  - Built-in rate limiter to avoid throttling (default --rpm 120).
  - Strict /company.json payload (documented fields only).
  - Sites are companies with parent_company_uuid; dedupe via parent+address.
  - Dump mode to export all Clients + Company Contacts to one JSON file.
  - Per-run, timestamped audit and dump file names.

Usage examples:
  # Dump all without printing to terminal, write logs to file, paced at 120 rpm
  python sm8_import.py --dump-all --dump-file ./export/clients_contacts.json \
    --audit-file ./audit/dump.ndjson --no-console --log-file ./logs/run.log --rpm 120

  # Customers only, dry-run, detailed audit on newest export
  python sm8_import.py --latest --only customers --limit 5 \
    --dry-run --audit-file ./audit/customers.ndjson --audit-detail --rpm 120
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


def _roll_timestamped_file(base_path: Optional[pathlib.Path], *, label: str) -> Optional[pathlib.Path]:
    """
    Given a base path (e.g., ./audit/customers.ndjson or ./export/clients_contacts.json),
    create a sibling file with a UTC timestamp suffix: <stem>_YYYYMMDDTHHMMSSZ<suffix>
    Returns the rolled path or None if base_path is None.
    """
    if not base_path:
        return None
    base_path = base_path.resolve()
    base_path.parent.mkdir(parents=True, exist_ok=True)
    stem = base_path.stem
    suffix = base_path.suffix or ".ndjson"
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    rolled = base_path.with_name(f"{stem}_{ts}{suffix}")
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
        sleep_for = window - (now - dq[0]) + \
            random.uniform(0, 0.25)  # tiny jitter
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
    assert resource.startswith(
        "/"), "resource should start with '/' (e.g. '/staff.json')"
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
                    sleep_for = min(backoff + random.uniform(0,
                                    backoff * 0.25), backoff_cap)
                log.warning("Transient HTTP %s on %s %s. Attempt %d/%d. Sleeping %.1fs.",
                            r.status_code, method.upper(), resource, attempt, retries, sleep_for)
                time.sleep(sleep_for)
                backoff = min(backoff * 2, backoff_cap)
                continue

            # Non-transient error
            if r.status_code >= 400:
                snippet = (r.text or "")[:500]
                log.error("%s %s -> HTTP %s :: %s", method.upper(),
                          resource, r.status_code, snippet)
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
            sleep_for = min(backoff + random.uniform(0,
                            backoff * 0.25), backoff_cap)
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
    raise RuntimeError(
        f"sm8_request: exhausted {retries} attempts for {method.upper()} {resource}")

# ---------------- Lookups & Creation ----------------


def pick_best_address(hcp_customer: Dict[str, Any]) -> Dict[str, str]:
    """
    Prefer a 'service' address; fall back to 'billing'. Returns keys: street, city, state, zip, country.
    """
    addrs = hcp_customer.get("addresses") or []
    service = next((a for a in addrs if (
        a.get("type") or "").lower() == "service"), None)
    billing = next((a for a in addrs if (
        a.get("type") or "").lower() == "billing"), None)
    a = service or billing or {}
    return {
        "street": a.get("street") or a.get("street_line_1") or "",
        "city": a.get("city") or "",
        "state": a.get("state") or "",
        "zip": a.get("zip") or a.get("postcode") or "",
        "country": a.get("country") or "",
    }


def map_company_payload(
    *,
    name: str,
    address: Optional[Dict[str, str]] = None,
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
        "is_individual": 1 if is_individual else 0,
    }
    if parent_company_uuid:
        obj["parent_company_uuid"] = parent_company_uuid
    return {k: v for k, v in obj.items() if v not in (None, "", [])}


def map_contact_payload(company_uuid: str, hcp_customer: Dict[str, Any]) -> Dict[str, Any]:
    first = (hcp_customer.get("first_name") or "").strip()
    last = (hcp_customer.get("last_name") or "").strip()
    email = (hcp_customer.get("email") or "").strip()
    phone = hcp_customer.get(
        "home_number") or hcp_customer.get("work_number") or ""
    mobile = hcp_customer.get("mobile_number") or ""
    obj = {
        "company_uuid": company_uuid,
        "first": first or None,
        "last": last or None,
        "email": email or None,
        "phone": phone or None,
        "mobile": mobile or None,
    }
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
                          entity="clients", action="lookup", key=f"name={name}")
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
    conds: List[Tuple[str, str, Any]] = [
        ("parent_company_uuid", "eq", parent_uuid)]
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
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> Optional[str]:
    # Email
    if email:
        flt = odata_filter([("email", "eq", email)])
        data, _ = sm8_request("GET", "/companycontact.json", headers,
                              params={"$filter": flt},
                              audit=audit, audit_detail=audit_detail,
                              entity="company_contacts", action="lookup", key=f"email={email}")
        rows = data if isinstance(data, list) else []
        if rows:
            return _row_uuid(rows[0])
    # Phones
    for field, val in (("mobile", mobile), ("phone", phone)):
        if not val:
            continue
        flt = odata_filter([(field, "eq", val)])
        data, _ = sm8_request("GET", "/companycontact.json", headers,
                              params={"$filter": flt},
                              audit=audit, audit_detail=audit_detail,
                              entity="company_contacts", action="lookup", key=f"{field}={val}")
        rows = data if isinstance(data, list) else []
        if rows:
            return _row_uuid(rows[0])
    # Name (requires both first and last to be reliable)
    if first or last:
        flt = odata_filter([("first", "eq", first), ("last", "eq", last)])
        data, _ = sm8_request("GET", "/companycontact.json", headers,
                              params={"$filter": flt},
                              audit=audit, audit_detail=audit_detail,
                              entity="company_contacts", action="lookup", key=f"name={first} {last}".strip())
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

# ---------------- Importers ----------------


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
        log.warning(
            "employees.ndjson not found in %s; nothing to import.", run_dir)
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
    except requests.HTTPError as e:
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
    audit: Optional[pathlib.Path],
    audit_detail: bool,
) -> None:
    """
    Rules:
      - If no company: create/find a single Client (individual). No Company Contact.
      - If company present: upsert Company Client (by name), upsert Site by address (parent_company_uuid + address_*),
        upsert Company Contact (dedupe by email OR phone/mobile OR first+last).
      - No heavy preloads: on-demand OData lookups per record.
    """
    src = run_dir / "customers.ndjson"
    if not src.exists():
        log.warning(
            "customers.ndjson not found in %s; nothing to import.", run_dir)
        return

    log.info("Importing customers from %s", src)

    processed = 0
    created_clients = 0
    created_sites = 0
    created_contacts = 0

    for hcp in iter_ndjson(src):
        processed += 1
        if processed <= max(skip, 0):
            continue
        if limit is not None and (processed - max(skip, 0)) > max(limit, 0):
            break

        comp = (hcp.get("company") or "").strip()
        first = (hcp.get("first_name") or "").strip()
        last = (hcp.get("last_name") or "").strip()
        full_name = (f"{first} {last}".strip() or (hcp.get("email") or "").split(
            "@")[0] if hcp.get("email") else "").strip()
        email = (hcp.get("email") or "").strip()
        phone = hcp.get("home_number") or hcp.get("work_number") or ""
        mobile = hcp.get("mobile_number") or ""
        address = pick_best_address(hcp)

        # No company: single Client (individual). No company contact.
        if not comp:
            client_name = full_name or "Unknown Customer"
            existing_uuid = find_company_by_name(
                headers, name=client_name, audit=audit, audit_detail=audit_detail)
            if existing_uuid:
                continue

            client_payload = map_company_payload(
                name=client_name,
                address=address,
                is_individual=True
            )
            uuid = create_company(headers, payload=client_payload,
                                  dry_run=dry_run, audit=audit, audit_detail=audit_detail)
            created_clients += 1
            continue

        # Company client (head office)
        company_payload = map_company_payload(
            name=comp,
            is_individual=False
        )
        parent_uuid = find_company_by_name(
            headers, name=company_payload["name"], audit=audit, audit_detail=audit_detail)
        if not parent_uuid:
            parent_uuid = create_company(
                headers, payload=company_payload, dry_run=dry_run, audit=audit, audit_detail=audit_detail)
            created_clients += 1

        # Site (by address) — dedupe on parent + address fields
        site_uuid: Optional[str] = None
        if any(address.values()):
            site_uuid = find_site_by_address(headers, parent_uuid=parent_uuid,
                                             street=address.get("street", ""), city=address.get("city", ""),
                                             state=address.get("state", ""), postcode=address.get("zip", ""),
                                             audit=audit, audit_detail=audit_detail)
            if not site_uuid:
                site_payload = map_company_payload(
                    name=comp,
                    address=address,
                    is_individual=False,
                    parent_company_uuid=parent_uuid
                )
                site_uuid = create_company(
                    headers, payload=site_payload, dry_run=dry_run, audit=audit, audit_detail=audit_detail)
                created_sites += 1

        # Company contact (keeps email/phone/mobile)
        contact_uuid = find_contact(headers, first=first, last=last, email=email,
                                    phone=phone, mobile=mobile, audit=audit, audit_detail=audit_detail)
        if not contact_uuid:
            contact_payload = map_contact_payload(parent_uuid, hcp)
            contact_uuid = create_contact(
                headers, payload=contact_payload, dry_run=dry_run, audit=audit, audit_detail=audit_detail)
            if contact_uuid:
                created_contacts += 1

    log.info("[SUMMARY] customers processed=%d created_clients=%d created_sites=%d created_contacts=%d",
             processed, created_clients, created_sites, created_contacts)

# ---------------- CLI ----------------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Import Housecall Pro NDJSON into ServiceM8, or dump Clients/Contacts to JSON")

    # Logging controls
    ap.add_argument("--quiet", action="store_true",
                    help="Hide INFO logs (only warnings & errors)")
    ap.add_argument("--silent", action="store_true",
                    help="Hide almost all logging (critical only)")
    ap.add_argument(
        "--log-file", help="Write logs to this file instead of console")
    ap.add_argument("--no-console", action="store_true",
                    help="Disable terminal logging entirely")

    # Rate limiting
    ap.add_argument("--rpm", type=int, default=DEFAULT_RPM,
                    help="Max requests per minute (default: 120, 0 disables)")

    # Dump mode
    ap.add_argument("--dump-all", action="store_true",
                    help="Fetch all Clients and Company Contacts to a single JSON file and exit")
    ap.add_argument(
        "--dump-file", help="Where to write the dump JSON (a timestamped copy will be created). Default: ./export/clients_contacts.json")

    # Import mode
    ap.add_argument(
        "--ndjson-dir", help="Path to NDJSON run dir (or base dir when used with --latest). Default ./hcp_export")
    ap.add_argument("--latest", action="store_true",
                    help="Pick newest timestamped subfolder under --ndjson-dir (or ./hcp_export)")
    ap.add_argument("--only", choices=["employees", "customers"],
                    default="employees", help="Entity to import")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print actions but do not POST")
    ap.add_argument("--limit", type=int,
                    help="Import at most N records (per entity)")
    ap.add_argument("--skip", type=int, default=0,
                    help="Skip the first N source records")
    ap.add_argument(
        "--dotenv", help="Path to .env file to load (otherwise auto-discovered)")
    ap.add_argument("--auth-mode", choices=["oauth", "apikey"],
                    help="Force auth mode (otherwise auto-detect)")
    ap.add_argument(
        "--audit-file", help="Write NDJSON audit lines to a new timestamped file based on this path")
    ap.add_argument("--audit-detail", action="store_true",
                    help="Include response previews and key headers in audit lines")

    args = ap.parse_args()

    # Configure logging early
    setup_logging(quiet=args.quiet, silent=args.silent,
                  log_file=args.log_file, no_console=args.no_console)

    # Rate limit parameter
    global _GLOBAL_RPM
    _GLOBAL_RPM = max(0, int(args.rpm or 0))

    # Load env
    load_env(args.dotenv)

    # Build headers
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "sm8-import/1.5 (+https://servicem8.com)",
    }
    headers, _mode = build_auth(headers, args.auth_mode)

    # Audit setup (roll a new file each run if provided)
    audit_base = pathlib.Path(
        args.audit_file).resolve() if args.audit_file else None
    audit_path = _roll_timestamped_file(
        audit_base, label="Audit file")  # may be None

    # DUMP MODE
    if args.dump_all:
        dump_base = pathlib.Path(args.dump_file).resolve(
        ) if args.dump_file else pathlib.Path("./sm8_export/").resolve()
        dump_path = _roll_timestamped_file(dump_base, label="Dump file")
        if dump_path is None:
            sys.exit("Internal error: dump file path could not be resolved.")
        dump_clients_and_contacts(
            headers, out_path=dump_path, audit=audit_path, audit_detail=args.audit_detail)
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
            audit=audit_path, audit_detail=args.audit_detail,
        )
    else:
        log.warning("No importer implemented for the selected entity.")


if __name__ == "__main__":
    main()
