#!/usr/bin/env python3
"""
ServiceM8 Import Script — Imports data from Housecall Pro NDJSON exports to ServiceM8.

Usage examples:
  # Import newest run under ./hcp_export, employees only, dry-run
  python sm8_import.py --latest --only employees --dry-run

  # Import specific run dir
  python sm8_import.py --ndjson-dir ./hcp_export/20251006T000000Z --only employees

  # Be explicit about .env path and log level
  python sm8_import.py --latest --dotenv ./.env --log-level DEBUG

Auth:
  - OAuth bearer: set SM8_OAUTH_TOKEN (has scopes like manage_staff)
  - API key: set SM8_API_KEY (your smk-... value); sent via X-Api-Key header
  - You can force mode with --auth-mode oauth|apikey (otherwise auto-detect)

Notes:
  - This file is generic: the import pipeline is reusable for other entities.
  - Employees import is wired up as an example (HCP employees.ndjson -> SM8 /staff.json).
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
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

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

# -------- Config / Globals --------

SM8_BASE_URL = "https://api.servicem8.com/api_1.0"
TIMESTAMP_DIR_RE = re.compile(r"^\d{8}T\d{6}Z$")

TRANSIENT_STATUSES = {408, 425, 429, 500, 502, 503, 504}
TRANSIENT_EXC = (
    ReadTimeout,
    ConnectTimeout,
    ReqConnectionError,
    ChunkedEncodingError,
    ProtocolError,
)

DEFAULT_CONNECT_TIMEOUT = float(os.getenv("SM8_CONNECT_TIMEOUT", "15"))
DEFAULT_READ_TIMEOUT = float(os.getenv("SM8_READ_TIMEOUT", "90"))
DEFAULT_MAX_RETRIES = int(os.getenv("SM8_MAX_RETRIES", "8"))
DEFAULT_BACKOFF_CAP = float(os.getenv("SM8_BACKOFF_MAX", "60"))

logger = logging.getLogger("sm8-import")

# -------- Logging Setup --------


def setup_logging(level: str) -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # requests/urllib3 can be noisy on DEBUG, keep them a notch lower
    logging.getLogger("urllib3").setLevel(max(lvl, logging.WARNING))
    logging.getLogger("requests").setLevel(max(lvl, logging.WARNING))

# -------- Env / Auth --------


def load_env(dotenv_path: Optional[str]) -> Optional[str]:
    """Load .env (explicit path or auto-discovered) and return the path used (if any)."""
    used_path = None
    if dotenv_path:
        env_file = str(pathlib.Path(dotenv_path).resolve())
        loaded = load_dotenv(env_file, override=False)
        used_path = env_file if loaded else None
    else:
        used_path = find_dotenv(usecwd=True)
        if used_path:
            load_dotenv(used_path, override=False)
    return used_path


def build_auth_headers_and_method(auth_mode: Optional[str]) -> Tuple[Dict[str, str], Optional[Tuple[str, str]], str]:
    """
    Return (headers, basic_auth_tuple_or_None, mode_str).

    Modes:
      - "oauth": Authorization: Bearer <SM8_OAUTH_TOKEN>
      - "apikey": X-Api-Key: <SM8_API_KEY>   (NOTE: header; no HTTP Basic)
    """
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "sm8-import/1.0",
    }

    forced = (auth_mode or "").strip().lower()
    oauth_token = os.getenv("SM8_OAUTH_TOKEN")
    api_key = os.getenv("SM8_API_KEY")

    if forced == "oauth":
        if not oauth_token:
            sys.exit("Auth mode forced to 'oauth' but SM8_OAUTH_TOKEN is missing.")
        headers["Authorization"] = f"Bearer {oauth_token}"
        logger.info("Auth mode: oauth (Bearer token)")
        return headers, None, "oauth"

    if forced == "apikey":
        if not api_key:
            sys.exit("Auth mode forced to 'apikey' but SM8_API_KEY is missing.")
        headers["X-Api-Key"] = api_key
        logger.info("Auth mode: apikey (X-Api-Key header)")
        return headers, None, "apikey"

    # Auto-detect: prefer OAuth if supplied, else fall back to API key
    if oauth_token:
        headers["Authorization"] = f"Bearer {oauth_token}"
        logger.info("Auth mode: oauth (Bearer token) [auto]")
        return headers, None, "oauth"

    if api_key:
        headers["X-Api-Key"] = api_key
        logger.info("Auth mode: apikey (X-Api-Key header) [auto]")
        return headers, None, "apikey"

    sys.exit("Missing ServiceM8 credentials. Provide either SM8_OAUTH_TOKEN (oauth) or SM8_API_KEY (apikey).")

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
        logger.info("Using newest run: %s", runs[0])
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
            pool_connections=10, pool_maxsize=20, max_retries=0)
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
    basic_auth: Optional[Tuple[str, str]] = None,
    retries: int = DEFAULT_MAX_RETRIES,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    read_timeout: float = DEFAULT_READ_TIMEOUT,
    backoff_cap: float = DEFAULT_BACKOFF_CAP,
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
                headers=headers,
                auth=basic_auth,  # None in apikey & oauth modes
                timeout=(connect_timeout, read_timeout),
            )
        except TRANSIENT_EXC as e:
            sleep_for = min(backoff + random.uniform(0,
                            backoff * 0.25), backoff_cap)
            logger.warning(
                "Network error on %s %s (%s). Attempt %d/%d. Sleeping %.1fs.",
                method.upper(), resource, type(e).__name__, attempt, retries, sleep_for
            )
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
            logger.warning(
                "Transient HTTP %s on %s %s. Attempt %d/%d. Sleeping %.1fs.",
                r.status_code, method.upper(), resource, attempt, retries, sleep_for
            )
            time.sleep(sleep_for)
            backoff = min(backoff * 2, backoff_cap)
            continue

        if r.status_code >= 400:
            snippet = (r.text or "")[:500]
            logger.error("%s %s -> HTTP %s :: %s", method.upper(),
                         resource, r.status_code, snippet)
            r.raise_for_status()

        try:
            return r.json(), r
        except ValueError:
            return None, r

    if last_resp is not None:
        snippet = (last_resp.text or "")[:500]
        logger.error(
            "Exhausted retries. Last HTTP %s for %s %s :: %s",
            last_resp.status_code, method.upper(), resource, snippet
        )
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

# -------- Mapping helpers (HCP -> SM8) --------


def _coalesce(*vals) -> str:
    for v in vals:
        if v:
            return str(v)
    return ""


def _name_fallback(hcp: Dict[str, Any]) -> Tuple[str, str]:
    first = (hcp.get("first_name") or "").strip()
    last = (hcp.get("last_name") or "").strip()
    if first or last:
        return first, last
    name = (hcp.get("name") or "").strip()
    if name:
        parts = name.split()
        if len(parts) >= 2:
            return parts[0], " ".join(parts[1:])
        return name, ""
    email = (hcp.get("email") or "").strip()
    if email and "@" in email:
        return email.split("@", 1)[0], ""
    return "Unknown", "Staff"


def map_hcp_employee_to_sm8_staff(hcp_employee: Dict[str, Any], *, default_role_uuid: Optional[str] = None) -> Dict[str, Any]:
    """Field mapping: HCP employee -> ServiceM8 staff body."""
    first, last = _name_fallback(hcp_employee)
    email = (hcp_employee.get("email") or "").strip()

    staff: Dict[str, Any] = {
        "first": first[:30],
        "last": last[:30],
        "email": email,
    }

    mobile = _coalesce(hcp_employee.get("mobile_number"),
                       hcp_employee.get("phone"))
    if mobile:
        staff["mobile"] = mobile

    job_title = hcp_employee.get("role") or hcp_employee.get("title")
    if job_title:
        staff["job_title"] = str(job_title)

    color = (hcp_employee.get("color_hex") or "").strip()
    if color:
        if not color.startswith("#"):
            color = f"#{color}"
        staff["color"] = color

    perms = hcp_employee.get("permissions") or {}
    if isinstance(perms, dict):
        staff["hide_from_schedule"] = 0 if perms.get(
            "can_see_full_schedule") else 1

    role_uuid = os.getenv("SM8_DEFAULT_ROLE_UUID", "") or (
        default_role_uuid or "")
    if role_uuid:
        staff["security_role_uuid"] = role_uuid

    return staff

# -------- Generic preload + import pipeline --------


def preload_existing_map(
    resource: str,
    headers: Dict[str, str],
    *,
    basic_auth: Optional[Tuple[str, str]],
    key_selector: Callable[[Dict[str, Any]], Optional[str]],
) -> Dict[str, str]:
    """
    Build a map: key -> uuid for existing records.

    key_selector should return a stable, unique key used by your importer
    (e.g., for staff: email.lower()).
    """
    logger.info("Preloading existing records from %s ...", resource)
    try:
        data, _ = sm8_request("GET", resource, headers, basic_auth=basic_auth)
    except requests.HTTPError as e:
        code = getattr(e.response, "status_code", None)
        logger.warning("Failed to list %s: HTTP %s :: %s", resource,
                       code, (getattr(e.response, "text", "") or "")[:200])
        return {}

    if not isinstance(data, list):
        logger.info(
            "Preloaded 0 existing records from %s (non-list response).", resource)
        return {}

    out: Dict[str, str] = {}
    for row in data:
        if not isinstance(row, dict):
            continue
        k = key_selector(row)
        u = row.get("uuid")
        if k and isinstance(u, str) and u:
            out[k] = u

    logger.info("Preloaded %d existing records from %s", len(out), resource)
    return out


def import_entities_generic(
    *,
    entity_name: str,
    run_dir: pathlib.Path,
    source_filename: str,
    target_resource: str,
    headers: Dict[str, str],
    basic_auth: Optional[Tuple[str, str]],
    map_func: Callable[[Dict[str, Any]], Dict[str, Any]],
    src_key_selector: Callable[[Dict[str, Any]], Optional[str]],
    existing_key_selector: Callable[[Dict[str, Any]], Optional[str]],
    on_duplicate: str = "skip",  # skip|update|ignore
    dry_run: bool = False,
    limit: Optional[int] = None,
    skip: int = 0,
    mapping_out: Optional[pathlib.Path] = None,
    preload: bool = True,
) -> None:
    """
    Generic importer:

      - Reads NDJSON rows from run_dir/source_filename
      - Preloads existing SM8 records (if preload=True) to avoid duplicates
      - For each source row:
          - map_func -> body
          - derive key via src_key_selector (e.g., email)
          - if key exists in preload map:
              - skip | update (POST with uuid) | ignore (attempt create anyway)
          - else create (POST)
      - Writes mapping file if requested (source_key -> created/updated uuid)
    """
    src = run_dir / source_filename
    if not src.exists():
        logger.warning("%s not found in %s; nothing to import.",
                       source_filename, run_dir)
        return

    logger.info("Importing %s from %s", entity_name, src)

    existing_map: Dict[str, str] = {}
    if preload:
        existing_map = preload_existing_map(
            target_resource,
            headers,
            basic_auth=basic_auth,
            key_selector=existing_key_selector,
        )

    created = 0
    updated = 0
    processed = 0
    written_map: Dict[str, str] = {}

    for row in iter_ndjson(src):
        processed += 1
        if processed <= max(skip, 0):
            continue
        if limit is not None and (created + updated) >= max(limit, 0):
            break

        src_key = src_key_selector(row)
        if not src_key:
            logger.warning("[SKIP] %s row missing selectable key", entity_name)
            continue

        body = map_func(row)
        missing = [k for k in ("first", "last", "email")
                   if target_resource == "/staff.json" and not body.get(k)]
        if missing:
            logger.warning("[SKIP] %s key=%s missing required fields: %s",
                           entity_name, src_key, ", ".join(missing))
            continue

        existing_uuid = existing_map.get(src_key)

        # Duplicate policy
        if existing_uuid:
            if dry_run:
                if on_duplicate == "update":
                    logger.info("[DRY] Would UPDATE %s key=%s uuid=%s body=%s", entity_name,
                                src_key, existing_uuid, json.dumps(body, ensure_ascii=False))
                else:
                    logger.info("[DRY] Would SKIP existing %s key=%s uuid=%s",
                                entity_name, src_key, existing_uuid)
                written_map[src_key] = existing_uuid
                if on_duplicate == "update":
                    updated += 1
                continue

            if on_duplicate == "skip":
                logger.info("[SKIP] Existing %s key=%s uuid=%s",
                            entity_name, src_key, existing_uuid)
                written_map[src_key] = existing_uuid
                continue

            if on_duplicate == "update":
                # ServiceM8 updates can be done by POSTing with "uuid" supplied.
                update_body = dict(body)
                update_body["uuid"] = existing_uuid
                try:
                    _, resp = sm8_request(
                        "POST", target_resource, headers, json_body=update_body, basic_auth=basic_auth)
                    logger.info("[OK] Updated %s key=%s uuid=%s",
                                entity_name, src_key, existing_uuid)
                    written_map[src_key] = existing_uuid
                    updated += 1
                except requests.HTTPError as e:
                    code = getattr(e.response, "status_code", None)
                    snippet = (getattr(e.response, "text", "") or "")[:300]
                    logger.error("Failed to UPDATE %s key=%s uuid=%s: HTTP %s :: %s",
                                 entity_name, src_key, existing_uuid, code, snippet)
                continue

            if on_duplicate == "ignore":
                logger.info("[INFO] Duplicate %s key=%s exists (uuid=%s) but attempting create anyway (ignore).",
                            entity_name, src_key, existing_uuid)
                # fall through to create

        # CREATE
        if dry_run:
            logger.info("[DRY] Would CREATE %s key=%s body=%s",
                        entity_name, src_key, json.dumps(body, ensure_ascii=False))
            written_map[src_key] = "dry-run-uuid"
            created += 1
            continue

        try:
            data, resp = sm8_request(
                "POST", target_resource, headers, json_body=body, basic_auth=basic_auth)
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            snippet = (getattr(e.response, "text", "") or "")[:300]
            logger.error("Failed to CREATE %s key=%s: HTTP %s :: %s",
                         entity_name, src_key, code, snippet)
            continue

        sm8_uuid = resp.headers.get(
            "x-record-uuid") or (data or {}).get("uuid")
        if sm8_uuid:
            written_map[src_key] = sm8_uuid
            logger.info("[OK] Created %s key=%s -> %s",
                        entity_name, src_key, sm8_uuid)
        else:
            logger.info(
                "[OK] Created %s key=%s (uuid not returned)", entity_name, src_key)
        created += 1

    if mapping_out:
        try:
            mapping_out.parent.mkdir(parents=True, exist_ok=True)
            mapping_out.write_text(json.dumps(
                written_map, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("Wrote mapping file: %s", mapping_out)
        except Exception as e:
            logger.warning(
                "Could not write mapping file %s: %s", mapping_out, e)

    logger.info("[SUMMARY] %s processed=%d created=%d updated=%d skipped=%d",
                entity_name, processed, created, updated, processed - (created + updated))

# -------- Employees wiring (HCP -> SM8) --------


def employees_src_key_selector(hcp: Dict[str, Any]) -> Optional[str]:
    email = (hcp.get("email") or "").strip().lower()
    return email or None


def employees_existing_key_selector(sm8_obj: Dict[str, Any]) -> Optional[str]:
    email = (sm8_obj.get("email") or "").strip().lower()
    return email or None

# -------- CLI --------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Import Housecall Pro NDJSON into ServiceM8")
    ap.add_argument(
        "--ndjson-dir", help="Path to NDJSON run dir (or base dir when used with --latest). Default ./hcp_export")
    ap.add_argument("--latest", action="store_true",
                    help="Pick newest timestamped subfolder under --ndjson-dir (or ./hcp_export)")
    ap.add_argument("--only", choices=["employees"], default="employees",
                    help="Entity to import (currently only 'employees')")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print actions but do not POST")
    ap.add_argument("--limit", type=int, help="Import at most N records")
    ap.add_argument("--skip", type=int, default=0,
                    help="Skip the first N source records")
    ap.add_argument("--mapping-out",
                    help="Write a JSON map of source-key -> SM8 uuid")
    ap.add_argument(
        "--dotenv", help="Path to .env file to load (otherwise auto-discovered)")
    ap.add_argument("--auth-mode", choices=["oauth", "apikey"],
                    help="Force auth mode (otherwise auto-detect)")
    ap.add_argument("--on-duplicate", choices=["skip", "update", "ignore"], default="skip",
                    help="Duplicate policy when a record with the same key already exists")
    ap.add_argument("--no-preload", action="store_true",
                    help="Disable preloading existing records (always try to create)")
    ap.add_argument("--log-level", default="INFO",
                    help="Logging level (DEBUG, INFO, WARNING, ERROR)")

    args = ap.parse_args()

    setup_logging(args.log_level)

    # Load env before anything else
    used_env = load_env(args.dotenv)
    if used_env:
        logger.info("Loaded env from: %s", used_env)

    # Resolve run directory
    run_dir = resolve_ndjson_dir(args.ndjson_dir, args.latest)
    logger.info("Using NDJSON dir: %s", run_dir)
    if not run_dir.exists():
        sys.exit(f"NDJSON dir not found: {run_dir}")

    # Build auth
    headers, basic_auth, _mode = build_auth_headers_and_method(args.auth_mode)

    # Optional mapping output path
    mapping_out = pathlib.Path(
        args.mapping_out).resolve() if args.mapping_out else None

    # Default SM8 security role can be passed via env SM8_DEFAULT_ROLE_UUID
    default_role_uuid = os.getenv("SM8_DEFAULT_ROLE_UUID") or None

    if args.only == "employees":
        import_entities_generic(
            entity_name="employees",
            run_dir=run_dir,
            source_filename="employees.ndjson",
            target_resource="/staff.json",
            headers=headers,
            basic_auth=basic_auth,
            map_func=lambda hcp: map_hcp_employee_to_sm8_staff(
                hcp, default_role_uuid=default_role_uuid),
            src_key_selector=employees_src_key_selector,
            existing_key_selector=employees_existing_key_selector,
            on_duplicate=args.on_duplicate,
            dry_run=args.dry_run,
            limit=args.limit,
            skip=args.skip,
            mapping_out=mapping_out,
            preload=not args.no_preload,
        )
    else:
        logger.warning("No importer implemented for the selected entity.")


if __name__ == "__main__":
    main()
