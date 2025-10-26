#!/usr/bin/env python3
"""
ServiceM8 Import Script — Imports data from Housecall Pro NDJSON exports to ServiceM8.

Usage examples:
  # Import newest run under ./hcp_export, customers only, dry-run first
  python sm8_import.py --latest --only customers --dry-run

  # Then do the real run (employees + customers)
  python sm8_import.py --latest --only employees
  python sm8_import.py --latest --only customers

Auth:
  - OAuth bearer: set SM8_OAUTH_TOKEN (scopes like manage_staff, manage_customers, manage_customer_contacts)
  - API key: set SM8_API_KEY (your smk-... value). We send it as X-Api-Key.
  - You can force mode with --auth-mode oauth|apikey (otherwise auto-detect)

Notes:
  - “Clients” in ServiceM8 live at /company.json (also used for Company Sites via parent_company_uuid)
  - “Company Contacts” live at /companycontact.json
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
from dotenv import load_dotenv, find_dotenv

# -------- Config --------

SM8_BASE_URL = "https://api.servicem8.com/api_1.0"
TIMESTAMP_DIR_RE = re.compile(r"^\d{8}T\d{6}Z$")

TRANSIENT_STATUSES = {408, 425, 429, 500, 502, 503, 504}
TRANSIENT_EXC = (ReadTimeout, ConnectTimeout, ReqConnectionError,
                 ChunkedEncodingError, ProtocolError)

DEFAULT_CONNECT_TIMEOUT = float(os.getenv("SM8_CONNECT_TIMEOUT", "15"))
DEFAULT_READ_TIMEOUT = float(os.getenv("SM8_READ_TIMEOUT", "90"))
DEFAULT_MAX_RETRIES = int(os.getenv("SM8_MAX_RETRIES", "8"))
DEFAULT_BACKOFF_CAP = float(os.getenv("SM8_BACKOFF_MAX", "60"))

# -------- Logging --------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("sm8-import")

# -------- Env / Auth --------


def load_env(dotenv_path: Optional[str]) -> Optional[str]:
    """Load .env (explicit path or auto-discovered) and return path used (if any)."""
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


def build_auth(headers: Dict[str, str], auth_mode: Optional[str]) -> Tuple[Dict[str, str], Optional[Tuple[str, str]], str]:
    """
    Returns (headers, basic_auth_tuple_or_None, mode_str).

    Modes:
      - oauth  -> headers['Authorization'] = 'Bearer <SM8_OAUTH_TOKEN>'
      - apikey -> headers['X-Api-Key'] = <SM8_API_KEY>
    """
    forced = (auth_mode or "").strip().lower()
    oauth_token = os.getenv("SM8_OAUTH_TOKEN")
    api_key = os.getenv("SM8_API_KEY")

    if forced == "oauth":
        if not oauth_token:
            sys.exit("Auth mode forced to 'oauth' but SM8_OAUTH_TOKEN is missing.")
        headers["Authorization"] = f"Bearer {oauth_token}"
        log.info("Auth mode: oauth (Bearer token)")
        return headers, None, "oauth"

    if forced == "apikey":
        if not api_key:
            sys.exit("Auth mode forced to 'apikey' but SM8_API_KEY is missing.")
        headers["X-Api-Key"] = api_key
        log.info("Auth mode: apikey (X-Api-Key)")
        return headers, None, "apikey"

    # Auto-detect
    if oauth_token:
        headers["Authorization"] = f"Bearer {oauth_token}"
        log.info("Auth mode: oauth (Bearer token) [auto]")
        return headers, None, "oauth"
    if api_key:
        headers["X-Api-Key"] = api_key
        log.info("Auth mode: apikey (X-Api-Key) [auto]")
        return headers, None, "apikey"

    sys.exit("Missing ServiceM8 credentials. Provide SM8_OAUTH_TOKEN or SM8_API_KEY.")

# -------- Path resolution --------


def list_timestamp_dirs(root: pathlib.Path) -> list[pathlib.Path]:
    if not root.exists():
        return []
    out = []
    for child in root.iterdir():
        if child.is_dir() and TIMESTAMP_DIR_RE.match(child.name):
            out.append(child)
    return sorted(out, key=lambda p: p.name, reverse=True)


def resolve_ndjson_dir(base_or_run: Optional[str], latest: bool) -> pathlib.Path:
    """
    If latest is True:
      - base_or_run is treated as the base directory (default ./hcp_export)
      - pick newest timestamped child as the run dir
    Else:
      - base_or_run must be a specific run directory
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

# -------- HTTP client with retry/backoff --------


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


def sm8_request(
    method: str,
    resource: str,
    headers: Dict[str, str],
    *,
    json_body: Optional[Dict[str, Any]] = None,
    retries: int = DEFAULT_MAX_RETRIES,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    read_timeout: float = DEFAULT_READ_TIMEOUT,
    backoff_cap: float = DEFAULT_BACKOFF_CAP,
    params: Optional[Dict[str, Any]] = None,
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
    attempt = 0
    last_resp: Optional[requests.Response] = None

    while attempt < retries:
        attempt += 1
        try:
            r = session.request(
                method=method.upper(),
                url=url,
                json=json_body,
                params=params,
                headers=headers,
                timeout=(connect_timeout, read_timeout),
            )
        except TRANSIENT_EXC as e:
            sleep_for = min(backoff + random.uniform(0,
                            backoff * 0.25), backoff_cap)
            log.warning("Network error on %s %s (%s). Attempt %d/%d. Sleeping %.1fs.",
                        method.upper(), resource, type(e).__name__, attempt, retries, sleep_for)
            time.sleep(sleep_for)
            backoff = min(backoff * 2, backoff_cap)
            continue

        if r.status_code in TRANSIENT_STATUSES:
            last_resp = r
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

        if r.status_code >= 400:
            snippet = (r.text or "")[:500]
            log.error("%s %s -> HTTP %s :: %s", method.upper(),
                      resource, r.status_code, snippet)
            r.raise_for_status()

        try:
            return r.json(), r
        except ValueError:
            return None, r

    if last_resp is not None:
        snippet = (last_resp.text or "")[:500]
        log.error("Exhausted retries. Last HTTP %s for %s %s :: %s",
                  last_resp.status_code, method.upper(), resource, snippet)
        last_resp.raise_for_status()

    raise RuntimeError(
        f"sm8_request: exhausted {retries} attempts for {method.upper()} {resource}")

# -------- NDJSON helpers --------


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

# -------- Canonicalization / keys --------


def norm_email(s: Optional[str]) -> str:
    s = (s or "").strip().lower()
    return s


def norm_phone(s: Optional[str]) -> str:
    s = (s or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    # Normalize US 10/11 digits; leave others as-is
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits


def norm_name(s: Optional[str]) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def addr_key(street: str, city: str, state: str, postcode: str) -> str:
    def n(v: str) -> str:
        v = (v or "").strip().lower()
        v = re.sub(r"\s+", " ", v)
        return v
    return "|".join([n(street), n(city), n(state), n(postcode)])

# -------- Preload (dedupe caches) --------


def _extract_client_name(item: Dict[str, Any]) -> str:
    return item.get("name") or item.get("company_name") or item.get("company") or ""


def preload_clients(headers: Dict[str, str]) -> Dict[str, Any]:
    """
    Load existing Clients (and Sites) to dedupe:
      - name_index: name -> uuid
      - email_index: email -> uuid
      - phone_index: phone/mobile -> uuid
      - site_index: (parent_uuid, address_key) -> uuid
    """
    name_idx: Dict[str, str] = {}
    email_idx: Dict[str, str] = {}
    phone_idx: Dict[str, str] = {}
    site_idx: Dict[Tuple[str, str], str] = {}

    page = 1
    page_size = 500
    total_loaded = 0
    log.info("Preloading existing Clients from /company.json ...")
    while True:
        try:
            data, _ = sm8_request(
                "GET", "/company.json", headers, params={"page": page, "page_size": page_size})
        except requests.HTTPError as e:
            log.warning("Failed to list /company.json: HTTP %s :: %s",
                        getattr(e.response, "status_code", "?"), (getattr(e.response, "text", "") or "")[:200])
            break

        rows = data if isinstance(data, list) else []
        if not rows:
            break

        for it in rows:
            uuid = it.get("uuid") or it.get("id")
            if not uuid:
                continue
            nm = norm_name(_extract_client_name(it))
            em = norm_email(it.get("email"))
            ph = norm_phone(it.get("phone") or it.get("mobile"))

            if nm and nm not in name_idx:
                name_idx[nm] = uuid
            if em and em not in email_idx:
                email_idx[em] = uuid
            if ph and ph not in phone_idx:
                phone_idx[ph] = uuid

            parent_uuid = it.get("parent_company_uuid") or it.get(
                "parent_uuid") or ""
            if parent_uuid:
                street = it.get("address_street") or it.get("street") or ""
                city = it.get("address_city") or it.get("city") or ""
                state = it.get("address_state") or it.get("state") or ""
                postcode = it.get("address_postcode") or it.get(
                    "postcode") or it.get("post_code") or ""
                k = addr_key(street, city, state, postcode)
                if k:
                    site_idx[(parent_uuid, k)] = uuid

        total_loaded += len(rows)
        if len(rows) < page_size:
            break
        page += 1

    log.info("Preloaded %d Clients/Sites; name:%d email:%d phone:%d sites:%d",
             total_loaded, len(name_idx), len(email_idx), len(phone_idx), len(site_idx))
    return {
        "name_index": name_idx,
        "email_index": email_idx,
        "phone_index": phone_idx,
        "site_index": site_idx,
    }


def preload_company_contacts(headers: Dict[str, str]) -> Dict[str, str]:
    """
    Load existing Company Contacts to dedupe by name/email/phone.
    Returns dict of key -> uuid where key is one of:
      - "name:<norm_name>"
      - "email:<norm_email>"
      - "phone:<norm_phone>"
    """
    idx: Dict[str, str] = {}
    page = 1
    page_size = 500
    total = 0
    log.info("Preloading existing Company Contacts from /companycontact.json ...")
    while True:
        try:
            data, _ = sm8_request("GET", "/companycontact.json",
                                  headers, params={"page": page, "page_size": page_size})
        except requests.HTTPError as e:
            log.warning("Failed to list /companycontact.json: HTTP %s :: %s",
                        getattr(e.response, "status_code", "?"), (getattr(e.response, "text", "") or "")[:200])
            break

        rows = data if isinstance(data, list) else []
        if not rows:
            break

        for it in rows:
            uuid = it.get("uuid") or it.get("id")
            if not uuid:
                continue
            first = it.get("first") or it.get("firstname") or ""
            last = it.get("last") or it.get("lastname") or ""
            nm = norm_name(f"{first} {last}".strip())
            em = norm_email(it.get("email"))
            ph = norm_phone(it.get("phone") or it.get("mobile"))

            if nm:
                idx.setdefault(f"name:{nm}", uuid)
            if em:
                idx.setdefault(f"email:{em}", uuid)
            if ph:
                idx.setdefault(f"phone:{ph}", uuid)

        total += len(rows)
        if len(rows) < page_size:
            break
        page += 1

    log.info("Preloaded %d Company Contacts; index keys=%d", total, len(idx))
    return idx

# -------- Mapping: HCP -> SM8 --------


def pick_best_address(hcp_customer: Dict[str, Any]) -> Dict[str, str]:
    """
    Prefer a 'service' address; fall back to 'billing' if needed.
    Returns dict with street, city, state, zip, country (strings, may be empty).
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


def map_hcp_to_sm8_company_object(name: str, email: str, phone: str, mobile: str, address: Dict[str, str], *, is_individual: bool, parent_company_uuid: Optional[str] = None) -> Dict[str, Any]:
    """
    Build the SM8 Client payload (/company.json). When parent_company_uuid is provided, this creates a Site.
    """
    obj: Dict[str, Any] = {
        "name": name,
        "email": email or None,
        "phone": phone or None,
        "mobile": mobile or None,
        # Primary address
        "address_street": address.get("street") or None,
        "address_city": address.get("city") or None,
        "address_state": address.get("state") or None,
        "address_postcode": address.get("zip") or None,
        "address_country": address.get("country") or None,
        # Individual/business flag (SM8 booleans are often "0"/"1" strings; SM8 accepts ints too)
        "is_individual": 1 if is_individual else 0,
    }
    if parent_company_uuid:
        obj["parent_company_uuid"] = parent_company_uuid
    # Remove empty None's to keep payload lean
    return {k: v for k, v in obj.items() if v not in (None, "", [])}


def map_hcp_to_sm8_company_contact(company_uuid: str, hcp_customer: Dict[str, Any]) -> Dict[str, Any]:
    first = (hcp_customer.get("first_name") or "").strip()
    last = (hcp_customer.get("last_name") or "").strip()
    email = (hcp_customer.get("email") or "").strip()
    phone = hcp_customer.get(
        "home_number") or hcp_customer.get("work_number") or ""
    mobile = hcp_customer.get("mobile_number") or ""
    return {
        "company_uuid": company_uuid,
        "first": first or None,
        "last": last or None,
        "email": email or None,
        "phone": phone or None,
        "mobile": mobile or None,
    }

# -------- Upsert helpers --------


def upsert_client(headers: Dict[str, str], caches: Dict[str, Any], payload: Dict[str, Any]) -> str:
    """
    Dedupe by name OR email OR phone using preloaded caches.
    If exists: return uuid; else create and return new uuid.
    """
    name_idx = caches["name_index"]
    email_idx = caches["email_index"]
    phone_idx = caches["phone_index"]

    nm = norm_name(payload.get("name"))
    em = norm_email(payload.get("email"))
    ph = norm_phone(payload.get("phone") or payload.get("mobile"))

    candidate = name_idx.get(nm) or (email_idx.get(em) if em else None) or (
        phone_idx.get(ph) if ph else None)
    if candidate:
        return candidate

    data, resp = sm8_request("POST", "/company.json",
                             headers, json_body=payload)
    uuid = resp.headers.get("x-record-uuid") or (data or {}).get("uuid")
    if not uuid:
        raise RuntimeError("Client created but no uuid returned")

    # Update caches so later rows dedupe correctly within run
    if nm and nm not in name_idx:
        name_idx[nm] = uuid
    if em and em not in email_idx:
        email_idx[em] = uuid
    if ph and ph not in phone_idx:
        phone_idx[ph] = uuid

    log.info("[OK] Client upsert: %s -> %s", payload.get("name"), uuid)
    return uuid


def upsert_site(headers: Dict[str, str], caches: Dict[str, Any], parent_uuid: str, payload: Dict[str, Any]) -> str:
    """
    Dedupe site by (parent_uuid, address_key).
    """
    site_idx = caches["site_index"]

    k = addr_key(payload.get("address_street", ""), payload.get("address_city", ""),
                 payload.get("address_state", ""), payload.get("address_postcode", ""))
    if not k:
        # No address -> creating a site doesn’t make sense; fall back to parent
        return parent_uuid

    existing = site_idx.get((parent_uuid, k))
    if existing:
        return existing

    data, resp = sm8_request("POST", "/company.json",
                             headers, json_body=payload)
    uuid = resp.headers.get("x-record-uuid") or (data or {}).get("uuid")
    if not uuid:
        raise RuntimeError("Site created but no uuid returned")

    site_idx[(parent_uuid, k)] = uuid
    log.info("[OK] Site upsert for parent=%s at %s -> %s",
             parent_uuid, k, uuid)
    return uuid


def upsert_company_contact(headers: Dict[str, str], contact_idx: Dict[str, str], payload: Dict[str, Any]) -> Optional[str]:
    """
    Dedupe contact by name OR email OR phone.
    """
    first = payload.get("first") or ""
    last = payload.get("last") or ""
    email = payload.get("email") or ""
    phone = payload.get("phone") or ""
    mobile = payload.get("mobile") or ""

    nm = norm_name(f"{first} {last}".strip())
    em = norm_email(email)
    ph = norm_phone(phone or mobile)

    key = None
    for cand in [f"name:{nm}" if nm else None, f"email:{em}" if em else None, f"phone:{ph}" if ph else None]:
        if cand and cand in contact_idx:
            return contact_idx[cand]

    data, resp = sm8_request("POST", "/companycontact.json", headers,
                             json_body={k: v for k, v in payload.items() if v not in ("", None)})
    uuid = resp.headers.get("x-record-uuid") or (data or {}).get("uuid")
    if not uuid:
        return None

    if nm:
        contact_idx.setdefault(f"name:{nm}", uuid)
    if em:
        contact_idx.setdefault(f"email:{em}", uuid)
    if ph:
        contact_idx.setdefault(f"phone:{ph}", uuid)

    log.info("[OK] Company Contact upsert: %s %s -> %s", first, last, uuid)
    return uuid

# -------- Importers --------


def import_employees(run_dir: pathlib.Path, headers: Dict[str, str], *, dry_run: bool, limit: Optional[int], skip: int, default_role_uuid: Optional[str]) -> None:
    src = run_dir / "employees.ndjson"
    if not src.exists():
        log.warning(
            "employees.ndjson not found in %s; nothing to import.", run_dir)
        return

    log.info("Importing employees from %s", src)
    created = 0
    processed = 0

    # Preload existing staff to avoid dup emails (best-effort)
    # If this 401s due to scope, we'll still try to create; SM8 will error on duplicate email anyway.
    existing_emails = set()
    try:
        page, page_size = 1, 500
        log.info("Preloading existing Staff from /staff.json ...")
        while True:
            data, _ = sm8_request(
                "GET", "/staff.json", headers, params={"page": page, "page_size": page_size})
            rows = data if isinstance(data, list) else []
            if not rows:
                break
            for it in rows:
                em = norm_email(it.get("email"))
                if em:
                    existing_emails.add(em)
            if len(rows) < page_size:
                break
            page += 1
        log.info("Preloaded %d staff emails", len(existing_emails))
    except requests.HTTPError as e:
        log.warning("Failed to list /staff.json: HTTP %s :: %s",
                    getattr(e.response, "status_code", "?"), (getattr(e.response, "text", "") or "")[:200])

    for hcp in iter_ndjson(src):
        processed += 1
        if processed <= max(skip, 0):
            continue
        if limit is not None and (processed - max(skip, 0)) > max(limit, 0):
            break

        first = (hcp.get("first_name") or "").strip()[:30]
        last = (hcp.get("last_name") or "").strip()[:30]
        email = norm_email(hcp.get("email"))
        mobile = hcp.get("mobile_number") or hcp.get("phone") or ""
        job_title = hcp.get("role") or hcp.get("title") or ""
        color = hcp.get("color_hex") or ""
        if color and not color.startswith("#"):
            color = "#" + color

        staff = {
            "first": first or "Unknown",
            "last": last or "Staff",
            "email": email or f"noemail+{processed}@example.invalid",
        }
        if mobile:
            staff["mobile"] = mobile
        if job_title:
            staff["job_title"] = str(job_title)
        if color:
            staff["color"] = color
        role_uuid = os.getenv("SM8_DEFAULT_ROLE_UUID") or default_role_uuid
        if role_uuid:
            staff["security_role_uuid"] = role_uuid

        if dry_run:
            log.info("[DRY] Would POST /staff.json :: %s",
                     json.dumps(staff, ensure_ascii=False))
            created += 1
            continue

        if email and email in existing_emails:
            log.info("[SKIP] Staff exists (email): %s", email)
            continue

        try:
            _, resp = sm8_request("POST", "/staff.json",
                                  headers, json_body=staff)
            sm8_uuid = resp.headers.get("x-record-uuid")
            if email:
                existing_emails.add(email)
            log.info("[OK] Created staff %s -> %s",
                     staff["email"], sm8_uuid or "(uuid n/a)")
            created += 1
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            body = (getattr(e.response, "text", "") or "")[:300]
            log.error("Failed to create staff for email=%s: HTTP %s. Body: %s",
                      staff["email"], code, body)

    log.info("[SUMMARY] employees processed=%d created=%d skipped=%d",
             processed, created, processed - created)


def import_customers(run_dir: pathlib.Path, headers: Dict[str, str], *, dry_run: bool, limit: Optional[int], skip: int) -> None:
    """
    Implements your rules:
      - If no company: create a single Client (individual). No Company Contact.
      - If company present: upsert Client (company), upsert Site (by address), upsert Company Contact.
      - Dedupe Clients & Contacts by (name OR email OR phone). Dedupe Site by (parent, address).
    """
    src = run_dir / "customers.ndjson"
    if not src.exists():
        log.warning(
            "customers.ndjson not found in %s; nothing to import.", run_dir)
        return

    log.info("Importing customers from %s", src)

    # Preload caches
    client_caches = preload_clients(headers)
    contact_idx = preload_company_contacts(headers)

    created_clients = 0
    created_sites = 0
    created_contacts = 0
    processed = 0

    for hcp in iter_ndjson(src):
        processed += 1
        if processed <= max(skip, 0):
            continue
        if limit is not None and (processed - max(skip, 0)) >= max(limit, 0):
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

        if not comp:
            # Individual client only
            client_payload = map_hcp_to_sm8_company_object(
                name=full_name or "Unknown Customer",
                email=email, phone=phone, mobile=mobile,
                address=address, is_individual=True
            )
            if dry_run:
                log.info("[DRY] Would upsert Client (individual): %s",
                         json.dumps(client_payload, ensure_ascii=False))
            else:
                try:
                    _uuid = upsert_client(
                        headers, client_caches, client_payload)
                    created_clients += 1  # counts new; upsert_client only increments cache; we can't easily know if new vs existing -> treat as created attempt
                except requests.HTTPError as e:
                    log.error("Client (individual) failed: HTTP %s :: %s",
                              getattr(e.response, "status_code", "?"), (getattr(e.response, "text", "") or "")[:200])
            continue

        # Company path — company client
        company_payload = map_hcp_to_sm8_company_object(
            name=comp,
            email="", phone="", mobile="",
            address={"street": "", "city": "",
                     "state": "", "zip": "", "country": ""},
            is_individual=False
        )
        site_payload = map_hcp_to_sm8_company_object(
            name=comp,  # SM8 lets site share the same name; dedupe is by address+parent
            email=email, phone=phone, mobile=mobile,
            address=address, is_individual=False,
            parent_company_uuid="__PARENT__"  # placeholder; fill after we get parent uuid
        )
        contact_payload = map_hcp_to_sm8_company_contact("__PARENT__", hcp)

        if dry_run:
            log.info("[DRY] Would upsert Company Client: %s",
                     json.dumps(company_payload, ensure_ascii=False))
            log.info("[DRY] Would upsert Company Site (by address): %s", json.dumps(
                {**site_payload, "parent_company_uuid": "<parent-uuid>"}, ensure_ascii=False))
            log.info("[DRY] Would upsert Company Contact: %s", json.dumps(
                {**contact_payload, "company_uuid": "<parent-uuid>"}, ensure_ascii=False))
            continue

        try:
            parent_uuid = upsert_client(
                headers, client_caches, company_payload)
        except requests.HTTPError as e:
            log.error("Company Client failed: HTTP %s :: %s",
                      getattr(e.response, "status_code", "?"), (getattr(e.response, "text", "") or "")[:200])
            continue

        # Site upsert (address-based)
        site_payload["parent_company_uuid"] = parent_uuid
        try:
            _site_uuid = upsert_site(
                headers, client_caches, parent_uuid, site_payload)
        except requests.HTTPError as e:
            log.error("Company Site failed: HTTP %s :: %s",
                      getattr(e.response, "status_code", "?"), (getattr(e.response, "text", "") or "")[:200])

        # Contact upsert
        contact_payload["company_uuid"] = parent_uuid
        try:
            _contact_uuid = upsert_company_contact(
                headers, contact_idx, contact_payload)
        except requests.HTTPError as e:
            log.error("Company Contact failed: HTTP %s :: %s",
                      getattr(e.response, "status_code", "?"), (getattr(e.response, "text", "") or "")[:200])

    log.info("[SUMMARY] customers processed=%d", processed)

# -------- CLI --------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Import Housecall Pro NDJSON into ServiceM8")
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
    args = ap.parse_args()

    # Load env
    load_env(args.dotenv)

    # Resolve run directory
    run_dir = resolve_ndjson_dir(args.ndjson_dir, args.latest)
    log.info("Using NDJSON dir: %s", run_dir)
    if not run_dir.exists():
        sys.exit(f"NDJSON dir not found: {run_dir}")

    # Build headers (common)
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "sm8-import/1.1 (+https://servicem8.com)",
    }
    headers, _basic_unused, _mode = build_auth(headers, args.auth_mode)

    # Default SM8 security role for staff can be passed via env SM8_DEFAULT_ROLE_UUID
    default_role_uuid = os.getenv("SM8_DEFAULT_ROLE_UUID") or None

    if args.only == "employees":
        import_employees(run_dir, headers, dry_run=args.dry_run, limit=args.limit,
                         skip=args.skip, default_role_uuid=default_role_uuid)
    elif args.only == "customers":
        import_customers(run_dir, headers, dry_run=args.dry_run,
                         limit=args.limit, skip=args.skip)
    else:
        log.warning("No importer implemented for the selected entity.")


if __name__ == "__main__":
    main()
