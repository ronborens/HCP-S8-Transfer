#!/usr/bin/env python3
"""
Housecall Pro data dump (GET-only) — API Key auth

Usage:
    python hcp_dump_all.py --out ./hcp_export [--resume] [--delay-ms 200]
    python hcp_dump_all.py --only jobs --out ./hcp_export
    python hcp_dump_all.py --resume --checklists-non-empty-only

Env:
    HCP_API_KEY                     required
    HCP_BASE_URL                    optional, defaults to https://api.housecallpro.com

    # Optional feature toggles (generic)
    HCP_DISABLE_GROUPS              CSV of endpoint groups to skip, e.g. "api/price_book,leads"
    HCP_DISABLE_ENDPOINTS           CSV of endpoint names to skip, e.g. "materials_from_categories,invoices"
    HCP_SKIP_ERROR_SUBSTRINGS       CSV of substrings; if HTTP error body contains any, skip that single-object child row
                                    e.g. "archived job,customer inactive,already deleted"
    HCP_TREAT_404_AS_SKIP_GROUPS    CSV of groups where a 404 should skip the whole endpoint (not crash)
                                    e.g. "api/price_book,booking_windows"

    # Archived parents
    HCP_ARCHIVED_JOB_FAST_FORWARD   1 to break a jobs-child loop on first archived-job hit (default 1)

    # Networking and retry knobs
    HCP_CONNECT_TIMEOUT             seconds, default 15
    HCP_READ_TIMEOUT                seconds, default 90
    HCP_MAX_RETRIES                 total attempts per request, default 8
    HCP_BACKOFF_MAX                 max sleep between retries, seconds, default 60

Notes:
    - Set HCP_API_KEY via a .env file if desired (python-dotenv supported).
    - Raw JSON is optional. NDJSON is always written.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import random
import re
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests import Session
from requests.exceptions import (
    ReadTimeout,
    ConnectTimeout,
    ConnectionError as ReqConnectionError,
    ChunkedEncodingError,
)
from urllib3.exceptions import ProtocolError
from dotenv import load_dotenv

load_dotenv()

# ---------------- Defaults ----------------

DEFAULT_PAGE_SIZE = 100

DEFAULT_ENDPOINTS: List[Dict[str, Any]] = [
    {"name": "customers", "path": "/customers", "params": {}, "container_key": "customers"},
    {"name": "employees", "path": "/employees", "params": {}, "container_key": "employees"},

    # Jobs with expansions included directly
    {"name": "jobs", "path": "/jobs", "params": {"page_size": DEFAULT_PAGE_SIZE, "expand": ["attachments", "appointments"]}, "container_key": "jobs"},

    # Children from jobs (single-object)
    {"name": "appointments_from_jobs", "path": "/jobs/{id}/appointments", "expand_from": "jobs", "id_field": "id",
     "params": {}, "container_key": "appointments", "single_object": True},

    {"name": "invoices_from_jobs", "path": "/jobs/{id}/invoices", "expand_from": "jobs", "id_field": "id",
     "params": {}, "container_key": "invoices", "single_object": True},

    {"name": "job_types", "path": "/job_fields/job_types", "params": {}, "container_key": "job_types"},

    {"name": "leads", "path": "/leads", "params": {}, "container_key": "leads"},
    {"name": "lead_sources", "path": "/lead_sources", "params": {}, "container_key": "lead_sources"},

    # Price book (may be permissioned; grouped by 'api/price_book')
    {"name": "material_categories", "path": "/api/price_book/material_categories", "params": {}, "container_key": "data"},
    {"name": "materials_from_categories", "path": "/api/price_book/materials", "expand_from": "material_categories", "id_field": "uuid",
     "params": {"material_category_uuid": "{uuid}"}, "container_key": "data"},
    {"name": "price_forms", "path": "/api/price_book/price_forms", "params": {}, "container_key": "data"},

    # Company
    {"name": "company", "path": "/company", "params": {}, "single_object": True, "container_key": None},
    {"name": "schedule_availability", "path": "/company/schedule_availability", "params": {}, "single_object": True, "container_key": None},
    {"name": "booking_windows", "path": "/company/schedule_availability/booking_windows", "params": {}, "container_key": "booking_windows"},

    {"name": "events", "path": "/events", "params": {}, "container_key": "events"},
    {"name": "tags", "path": "/tags", "params": {}, "container_key": "tags"},

    {"name": "invoices", "path": "/invoices", "params": {}, "container_key": "invoices"},
    {"name": "estimates", "path": "/estimates", "params": {}, "container_key": "estimates"},

    # Checklists
    {
        "name": "checklists_from_jobs",
        "path": "/checklists",
        "expand_from": "jobs",
        "id_field": "id",
        "params": {"job_uuids": ["{id}"]},
        "container_key": "checklists",
        "hydrate": {
            "path": "/checklists/{id}",
            "id_field": "id",
            "require_keys": ["sections"]
        }
    },
    {
        "name": "checklists_from_estimates",
        "path": "/checklists",
        "expand_from": "estimates",
        "id_field": "id",
        "params": {"estimate_uuids": ["{id}"]},
        "container_key": "checklists",
        "hydrate": {
            "path": "/checklists/{id}",
            "id_field": "id",
            "require_keys": ["sections"]
        }
    },

    {"name": "payments", "path": "/payments", "params": {}, "container_key": "payments"},
]

# ---------------- Utility ----------------

TIMESTAMP_DIR_RE = re.compile(r"^\d{8}T\d{6}Z$")

def list_timestamp_dirs(root: pathlib.Path) -> List[pathlib.Path]:
    if not root.exists():
        return []
    out = []
    for child in root.iterdir():
        if child.is_dir() and TIMESTAMP_DIR_RE.match(child.name):
            out.append(child)
    return sorted(out, key=lambda p: p.name, reverse=True)

def resolve_outdir(out_root: pathlib.Path, resume: bool) -> pathlib.Path:
    if resume:
        if (out_root / "manifest.json").exists():
            ensure_dir(out_root)
            return out_root
        runs = list_timestamp_dirs(out_root)
        if runs:
            return runs[0]
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    outdir = out_root / stamp
    ensure_dir(outdir)
    return outdir

def compute_resume_cutoff(outdir: pathlib.Path, ordered_names: List[str]) -> int:
    last_done = -1
    for i, name in enumerate(ordered_names):
        if (outdir / f"{name}.ndjson").exists():
            last_done = i
    return last_done

def utcnow_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")

def auth_headers() -> Dict[str, str]:
    key = os.getenv("HCP_API_KEY")
    if not key:
        sys.exit("Missing HCP_API_KEY. Set it in a .env file or your environment.")
    return {"Authorization": f"Token {key}", "Accept": "application/json"}

def get_base_url() -> str:
    return os.getenv("HCP_BASE_URL", "https://api.housecallpro.com").rstrip("/")

def encode_params(params: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (params or {}).items():
        if isinstance(v, list):
            out[f"{k}[]"] = v
        else:
            out[k] = v
    return out

def extract_items(payload: Any, container_key: Optional[str]) -> Tuple[List[Any], Optional[str]]:
    if payload is None:
        return [], None
    if isinstance(payload, dict):
        if container_key and isinstance(payload.get(container_key), list):
            return payload[container_key], container_key
        for k in ("items", "results", "data"):
            if isinstance(payload.get(k), list):
                return payload[k], k
        return [], None
    if isinstance(payload, list):
        return payload, None
    return [], None

def substitute_params(template_params: Dict[str, Any], parent_obj: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (template_params or {}).items():
        if isinstance(v, list):
            arr = []
            for item in v:
                if isinstance(item, str) and item.startswith("{") and item.endswith("}"):
                    field = item[1:-1]
                    arr.append(str(parent_obj.get(field, "")))
                else:
                    arr.append(item)
            out[k] = arr
        elif isinstance(v, str) and v.startswith("{") and v.endswith("}"):
            field = v[1:-1]
            out[k] = str(parent_obj.get(field, ""))
        else:
            out[k] = v
    return out

def ensure_dir(p: pathlib.Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def write_ndjson_line(
    nd_path: Optional[pathlib.Path],
    payload: Any,
    container_key: Optional[str],
    *,
    allow_envelope: bool = False,
) -> None:
    if nd_path is None:
        return
    items, _ = extract_items(payload, container_key)

    if not allow_envelope:
        if isinstance(items, list) and items:
            with nd_path.open("a", encoding="utf-8") as f:
                for row in items:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
        return

    with nd_path.open("a", encoding="utf-8") as f:
        if isinstance(items, list) and items:
            for row in items:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        elif isinstance(payload, dict) and payload:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

def read_parent_items_from_raw(outdir: pathlib.Path, parent_name: str, parent_container_key: Optional[str]) -> Iterable[Dict[str, Any]]:
    raw_dir = outdir / "raw" / parent_name
    if not raw_dir.exists():
        return []
    for p in sorted(raw_dir.glob("page_*.json")):
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        items, _ = extract_items(payload, parent_container_key)
        for it in items:
            if isinstance(it, dict):
                yield it

def read_parent_items_from_ndjson(outdir: pathlib.Path, parent_name: str) -> Iterable[Dict[str, Any]]:
    nd = outdir / f"{parent_name}.ndjson"
    if not nd.exists():
        return []
    with nd.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
            except Exception:
                continue

# -------- General grouping, skipping, and error policies --------

def endpoint_group(ep: dict) -> str:
    if ep.get("group"):
        return str(ep["group"])
    path = str(ep.get("path", "/")).lstrip("/")
    parts = path.split("/")
    if len(parts) >= 2 and parts[0] == "api":
        return f"api/{parts[1]}"
    return parts[0] if parts else ""

def parse_csv_env(name: str) -> set:
    raw = os.getenv(name, "") or ""
    return {x.strip() for x in raw.split(",") if x.strip()}

def is_not_found_error(exc: requests.exceptions.HTTPError) -> bool:
    resp = getattr(exc, "response", None)
    return bool(resp) and resp.status_code == 404

def matches_skip_text(exc: requests.exceptions.HTTPError, substrings: Iterable[str]) -> bool:
    resp = getattr(exc, "response", None)
    if not resp or not substrings:
        return False
    body = (resp.text or "").lower()
    return any(s.lower() in body for s in substrings)

# ---------------- Exceptions ----------------

class AuthzError(Exception):
    def __init__(self, endpoint_name: str, group: str, status_code: int):
        self.endpoint_name = endpoint_name
        self.group = group
        self.status_code = status_code
        super().__init__(f"{endpoint_name}: auth failure ({status_code}) in group '{group}'")

class NotFoundEndpointError(Exception):
    def __init__(self, endpoint_name: str, group: str, status_code: int = 404):
        self.endpoint_name = endpoint_name
        self.group = group
        self.status_code = status_code
        super().__init__(f"{endpoint_name}: not found ({status_code}) in group '{group}'")

# ---------------- HTTP + Retry ----------------

TRANSIENT_STATUSES = {408, 425, 429, 500, 502, 503, 504}
TRANSIENT_EXC = (ReadTimeout, ConnectTimeout, ReqConnectionError, ChunkedEncodingError, ProtocolError)

def get_timeout_tuple() -> Tuple[float, float]:
    connect = float(os.getenv("HCP_CONNECT_TIMEOUT", "15"))
    read = float(os.getenv("HCP_READ_TIMEOUT", "90"))
    return (connect, read)

def get_retry_knobs() -> Tuple[int, float]:
    max_retries = int(os.getenv("HCP_MAX_RETRIES", "8"))
    backoff_max = float(os.getenv("HCP_BACKOFF_MAX", "60"))
    return max_retries, backoff_max

_session: Optional[Session] = None

def get_session() -> Session:
    global _session
    if _session is None:
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=40, max_retries=0)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _session = s
    return _session

def robust_get(url: str, params: Dict[str, Any], headers: Dict[str, str], retries: Optional[int] = None) -> Tuple[Optional[Any], requests.Response]:
    if retries is None:
        retries, backoff_cap = get_retry_knobs()
    else:
        _, backoff_cap = get_retry_knobs()

    session = get_session()
    timeout = get_timeout_tuple()
    backoff = 1.0
    last_resp: Optional[requests.Response] = None
    attempt = 0

    while attempt < retries:
        attempt += 1
        try:
            r = session.get(url, params=params, headers=headers, timeout=timeout)
        except TRANSIENT_EXC as e:
            sleep_for = min(backoff + random.uniform(0, backoff * 0.25), backoff_cap)
            print(f"[WARN] Network error on GET {url} ({type(e).__name__}). Attempt {attempt}/{retries}. Sleeping {sleep_for:.1f}s then retrying.")
            time.sleep(sleep_for)
            backoff = min(backoff * 2, backoff_cap)
            continue

        if r.status_code in TRANSIENT_STATUSES:
            last_resp = r
            retry_after = r.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                sleep_for = min(int(retry_after), backoff_cap)
            else:
                sleep_for = min(backoff + random.uniform(0, backoff * 0.25), backoff_cap)
            print(f"[WARN] Transient HTTP {r.status_code} on {url}. Attempt {attempt}/{retries}. Sleeping {sleep_for:.1f}s.")
            time.sleep(sleep_for)
            backoff = min(backoff * 2, backoff_cap)
            continue

        if r.status_code >= 400:
            try:
                body = r.text[:500]
                print(f"[ERROR] {r.status_code} {url} :: {body}")
            except Exception:
                pass
            r.raise_for_status()

        try:
            return r.json(), r
        except ValueError:
            return None, r

    if last_resp is not None:
        print(f"[ERROR] Exhausted retries. Last HTTP {last_resp.status_code} for {url} :: {last_resp.text[:500]}")
        last_resp.raise_for_status()
    raise RuntimeError(f"robust_get: exhausted {retries} retries with no successful response for {url}")

# ---------------- Content predicates ----------------

def needs_hydration(item: dict, require_keys: List[str]) -> bool:
    for k in (require_keys or []):
        if k not in item:
            return True
    return False

def checklist_has_content(obj: dict) -> bool:
    secs = obj.get("sections") or []
    if not isinstance(secs, list) or not secs:
        return False
    for s in secs:
        items = (s or {}).get("items") or []
        if isinstance(items, list) and items:
            return True
    return False

def is_archived_parent_error_text(code: Optional[int], text: str) -> bool:
    if code not in (400, 404):
        return False
    t = (text or "").lower()
    return "archived job" in t or ("archived" in t and "job" in t)

def classify_single_child_fetch(
    session_headers: Dict[str, str],
    url: str,
    params: Dict[str, Any],
    *,
    skip_substrings: Iterable[str],
) -> Tuple[Optional[dict], str]:
    """
    Returns (payload, status):
      - (payload, "ok") on success
      - (None,   "archived") when HTTP 400/404 body mentions archived job
      - (None,   "skip")     for 404 or configured skip-substring matches
      - raises requests.exceptions.HTTPError for everything else
    """
    try:
        payload, _ = robust_get(url, encode_params(params), session_headers)
        return payload, "ok"
    except requests.exceptions.HTTPError as e:
        code = getattr(e.response, "status_code", None)
        text = getattr(e.response, "text", "") or ""

        if is_archived_parent_error_text(code, text):
            return None, "archived"

        if code == 404:
            return None, "skip"

        if matches_skip_text(e, skip_substrings):
            return None, "skip"

        raise

# ---------------- Pagination and expansion ----------------

def paginate_collect(
    session_headers: Dict[str, str],
    base_url: str,
    path: str,
    base_params: Dict[str, Any],
    page_size: int,
    container_key: Optional[str],
    outdir: Optional[pathlib.Path],
    name: Optional[str],
    resume: bool,
    delay_ms: int,
    max_pages: Optional[int],
    raw_json: bool,
    parent_dir_suffix: Optional[str] = None,
    current_group: Optional[str] = None,
) -> int:
    assert not resume or (outdir is not None and name is not None), "resume requires outdir and name"

    raw_dir: Optional[pathlib.Path] = None
    if raw_json and outdir and name:
        raw_dir = outdir / "raw" / (name if parent_dir_suffix is None else f"{name}/{parent_dir_suffix}")
        ensure_dir(raw_dir)

    nd_path = outdir / f"{name}.ndjson" if outdir and name else None
    treat_404_groups = parse_csv_env("HCP_TREAT_404_AS_SKIP_GROUPS")

    pages_fetched = 0
    page = 1
    while True:
        if max_pages and page > max_pages:
            print(f"[INFO] Stopping {name} at configured max-pages={max_pages}")
            break

        params = dict(base_params or {})
        params.setdefault("page", page)
        params.setdefault("page_size", page_size)

        url = f"{base_url}{path}"
        try:
            payload, resp = robust_get(url, encode_params(params), session_headers)
        except requests.exceptions.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code in (401, 403):
                raise AuthzError(name or path, current_group or "", int(code or 0)) from e
            if code == 404 and current_group in treat_404_groups:
                raise NotFoundEndpointError(name or path, current_group or "", 404) from e
            raise

        if resp.status_code == 204:
            break

        if raw_dir:
            raw_file = raw_dir / f"page_{page}.json"
            raw_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        write_ndjson_line(nd_path, payload, container_key, allow_envelope=False)

        items, _ = extract_items(payload, container_key)
        count = len(items)
        if count == 0:
            break
        pages_fetched += 1
        if count < params["page_size"]:
            break

        page += 1
        if delay_ms:
            time.sleep(delay_ms / 1000.0)

    return pages_fetched

def expand_child_collection(
    session_headers: Dict[str, str],
    base_url: str,
    listing_name: str,
    child_def: Dict[str, Any],
    page_size: int,
    outdir: pathlib.Path,
    resume: bool,
    delay_ms: int,
    max_pages: Optional[int],
    parent_container_key: Optional[str],
    raw_json: bool,
    summary_tracker: Optional[Dict[str, Any]] = None,
    checklists_non_empty_only: bool = False,
) -> None:
    """
    Expand a child endpoint for each parent row.

    Supports:
      - single_object children (one GET per parent, writes whole payload)
      - collection children (paginated), with optional per-row hydration
      - archived-job fast-forward for jobs children (HTTP 400 'Archived job')
      - optional filter to keep only non-empty checklists
      - raw JSON mirroring
    """
    id_field = child_def.get("id_field", "id")
    child_name = child_def["name"]
    container_key = child_def.get("container_key")
    single_object = child_def.get("single_object", False)
    group = endpoint_group(child_def)

    # Prefer RAW parents if available; fallback to NDJSON
    parents = list(read_parent_items_from_raw(outdir, listing_name, parent_container_key))
    if not parents:
        parents = list(read_parent_items_from_ndjson(outdir, listing_name))
    if not parents:
        print(f"[WARN] No parent items found for '{listing_name}', cannot expand '{child_name}'")
        return

    # Output prep for single-object children
    nd_path: Optional[pathlib.Path] = None
    raw_dir_single: Optional[pathlib.Path] = None
    if single_object:
        nd_path = outdir / f"{child_name}.ndjson"
        if raw_json:
            raw_dir_single = outdir / "raw" / child_name
            ensure_dir(raw_dir_single)

    # Policies
    skip_substrings = parse_csv_env("HCP_SKIP_ERROR_SUBSTRINGS")
    fast_forward_archived = os.getenv("HCP_ARCHIVED_JOB_FAST_FORWARD", "1") != "0"

    for it in parents:
        if id_field not in it:
            continue
        parent_id = it[id_field]
        call_params = substitute_params(child_def.get("params", {}), it)
        path = child_def["path"].replace("{id}", str(parent_id))
        url = f"{base_url}{path}"

        if single_object:
            # One GET per parent; never let HTTPError bubble out.
            payload, status = classify_single_child_fetch(
                session_headers, url, call_params, skip_substrings=skip_substrings
            )

            if status == "archived" and listing_name == "jobs":
                if fast_forward_archived:
                    print(f"[FAST-FWD] {child_name}: archived job at parent {parent_id}; skipping remaining parents for this child.")
                    if summary_tracker is not None:
                        summary_tracker.setdefault("fast_forwarded_due_to_archived", {}).setdefault(child_name, 0)
                        summary_tracker["fast_forwarded_due_to_archived"][child_name] += 1
                    break  # stop this child entirely
                else:
                    print(f"[WARN] {child_name}: archived job for parent {parent_id}; skipping this parent.")
                    if summary_tracker is not None:
                        summary_tracker.setdefault("child_skips", {}).setdefault(child_name, 0)
                        summary_tracker["child_skips"][child_name] += 1
                    continue

            if status == "skip":
                print(f"[WARN] Skipping {child_name} for parent {parent_id} (not found or matched skip policy).")
                if summary_tracker is not None:
                    summary_tracker.setdefault("child_skips", {}).setdefault(child_name, 0)
                    summary_tracker["child_skips"][child_name] += 1
                continue

            # Success: write
            if raw_json and raw_dir_single:
                (raw_dir_single / f"{parent_id}.json").write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            write_ndjson_line(nd_path, payload, container_key=container_key, allow_envelope=True)
            continue

        # Collection child (paginated), possibly with hydration
        hydrate = child_def.get("hydrate")
        if not hydrate:
            parent_suffix = f"{listing_name}_{parent_id}"
            pages = paginate_collect(
                session_headers,
                base_url,
                path,
                call_params,
                page_size,
                container_key,
                outdir=outdir,
                name=child_name,
                resume=resume,
                delay_ms=delay_ms,
                max_pages=max_pages,
                raw_json=raw_json,
                parent_dir_suffix=parent_suffix,
                current_group=group,
            )
            if pages == 0:
                empty_idx = outdir / f"{child_name}.empty.ndjson"
                with empty_idx.open("a", encoding="utf-8") as ef:
                    ef.write(json.dumps({"parent": listing_name, "parent_id": parent_id}) + "\n")
            continue

        # Hydration mode — need to inspect items to decide if a detail fetch is required
        nd_rows_path = outdir / f"{child_name}.ndjson"
        require_keys = hydrate.get("require_keys") or []
        detail_path_tmpl = hydrate["path"]
        detail_id_field = hydrate.get("id_field", "id")

        page = 1
        while True:
            params = dict(call_params)
            params["page"] = page
            params["page_size"] = page_size
            payload, _ = robust_get(url, encode_params(params), session_headers)
            items, _ck = extract_items(payload, container_key)

            if not items:
                if page == 1:
                    empty_idx = outdir / f"{child_name}.empty.ndjson"
                    with empty_idx.open("a", encoding="utf-8") as ef:
                        ef.write(json.dumps({"parent": listing_name, "parent_id": parent_id}) + "\n")
                break

            with nd_rows_path.open("a", encoding="utf-8") as out_f:
                for item in items:
                    if not isinstance(item, dict):
                        continue

                    # Optional checklist filter: keep only those with at least one item in any section
                    if checklists_non_empty_only and "checklists" in child_name:
                        obj_to_write = item
                        if needs_hydration(item, require_keys):
                            cid = item.get(detail_id_field)
                            if cid:
                                d_path = detail_path_tmpl.replace("{id}", str(cid))
                                try:
                                    detail, _ = robust_get(f"{base_url}{d_path}", {}, session_headers)
                                    if isinstance(detail, dict):
                                        obj_to_write = detail
                                except requests.exceptions.HTTPError:
                                    pass
                        if checklist_has_content(obj_to_write):
                            out_f.write(json.dumps(obj_to_write, ensure_ascii=False) + "\n")
                        else:
                            ei = outdir / f"{child_name}.empty.ndjson"
                            with ei.open("a", encoding="utf-8") as ef:
                                ef.write(json.dumps({
                                    "parent": listing_name,
                                    "parent_id": parent_id,
                                    "checklist_id": obj_to_write.get("id"),
                                    "title": obj_to_write.get("title")
                                }) + "\n")
                        continue

                    # Default: hydrate only when required keys are missing (missing, not empty)
                    if needs_hydration(item, require_keys):
                        cid = item.get(detail_id_field)
                        if cid:
                            d_path = detail_path_tmpl.replace("{id}", str(cid))
                            try:
                                detail, _ = robust_get(f"{base_url}{d_path}", {}, session_headers)
                                if isinstance(detail, dict):
                                    out_f.write(json.dumps(detail, ensure_ascii=False) + "\n")
                                    continue
                            except requests.exceptions.HTTPError:
                                pass
                    out_f.write(json.dumps(item, ensure_ascii=False) + "\n")

            if len(items) < page_size:
                break
            page += 1

def validate_endpoints(spec: Any) -> List[Dict[str, Any]]:
    if not isinstance(spec, list):
        raise ValueError("endpoints spec must be a list")
    out: List[Dict[str, Any]] = []
    for ep in spec:
        if not isinstance(ep, dict):
            raise ValueError("each endpoint must be a dict")
        if "name" not in ep or "path" not in ep:
            raise ValueError("each endpoint requires 'name' and 'path'")
        out.append(ep)
    return out

# ---------------- Main ----------------

def main() -> None:
    base_url = get_base_url()
    headers = auth_headers()

    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./hcp_export", help="Output directory or existing run dir when --resume")
    ap.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="page_size to request")
    ap.add_argument("--endpoints-json", help="Optional JSON file to override endpoints")
    ap.add_argument("--resume", action="store_true", help="Resume from prior run if possible")
    ap.add_argument("--delay-ms", type=int, default=0, help="Sleep between requests (ms)")
    ap.add_argument("--only", help="Only run a single endpoint by name")
    ap.add_argument("--max-pages", type=int, help="Limit pages fetched per endpoint (testing)")
    ap.add_argument("--raw-json", action="store_true", help="Write raw JSON pages to disk")
    ap.add_argument("--checklists-non-empty-only", action="store_true",
                    help="For checklist endpoints, only write rows that have at least one section with items; index empties separately")
    args = ap.parse_args()

    out_root = pathlib.Path(args.out).resolve()
    outdir = resolve_outdir(out_root, args.resume)
    ensure_dir(outdir)

    endpoints = DEFAULT_ENDPOINTS
    if args.endpoints_json:
        try:
            with open(args.endpoints_json, "r", encoding="utf-8") as f:
                endpoints = validate_endpoints(json.load(f))
        except FileNotFoundError:
            print(f"[WARN] Endpoints JSON {args.endpoints_json} not found, falling back to defaults")
        except Exception as e:
            sys.exit(f"Invalid endpoints JSON: {e}")

    if args.only:
        endpoints = [ep for ep in endpoints if ep["name"] == args.only]
        if not endpoints:
            sys.exit(f"--only '{args.only}' did not match any endpoint")

    # Normalize page size
    normalized: List[Dict[str, Any]] = []
    for ep in endpoints:
        ep = dict(ep)
        p = dict(ep.get("params") or {})
        p.setdefault("page_size", args.page_size)
        ep["params"] = p
        normalized.append(ep)
    endpoints = normalized

    ordered_names = [ep["name"] for ep in endpoints]
    last_done_idx = compute_resume_cutoff(outdir, ordered_names) if args.resume else -1
    if args.resume and last_done_idx >= 0:
        print(f"[INFO] Resume enabled. Detected progress up to '{ordered_names[last_done_idx]}' (index {last_done_idx}).")
        print("[INFO] All endpoints at or before that index will be skipped.")

    disabled_groups: set[str] = set() | parse_csv_env("HCP_DISABLE_GROUPS")
    disabled_endpoints = parse_csv_env("HCP_DISABLE_ENDPOINTS")
    default_404_groups = {"api/price_book"}
    disabled_404_groups = set() | parse_csv_env("HCP_TREAT_404_AS_SKIP_GROUPS")
    os.environ["HCP_TREAT_404_AS_SKIP_GROUPS"] = ",".join(sorted(default_404_groups | disabled_404_groups))

    if disabled_endpoints:
        endpoints = [ep for ep in endpoints if ep.get("name") not in disabled_endpoints]

    summary = {
        "skipped_due_to_resume": [],
        "skipped_due_to_group": [],
        "skipped_due_to_404_policy": [],
        "child_skips": {},
        "auth_failures": [],
        "processed_endpoints": [],
        "fast_forwarded_due_to_archived": {},
    }

    for idx, ep in enumerate(endpoints):
        name = ep["name"]
        path = ep["path"]
        params = ep.get("params", {})
        container_key = ep.get("container_key")
        group = endpoint_group(ep)

        if args.resume and idx <= last_done_idx:
            print(f"[SKIP] {name} (resume cutoff reached at index {last_done_idx})")
            summary["skipped_due_to_resume"].append(name)
            continue

        if endpoint_group(ep) in disabled_groups:
            print(f"[SKIP] {name} (group '{group}' disabled)")
            summary["skipped_due_to_group"].append({"name": name, "group": group})
            continue

        if "expand_from" in ep:
            parent_name = ep["expand_from"]
            parent_def = next((e for e in endpoints if e["name"] == parent_name and "expand_from" not in e), None) \
                         or next((e for e in DEFAULT_ENDPOINTS if e["name"] == parent_name and "expand_from" not in e), None)
            if not parent_def:
                print(f"[WARN] Missing parent definition for '{parent_name}', skipping child '{name}'")
                summary["skipped_due_to_group"].append({"name": name, "group": f"missing_parent:{parent_name}"})
                continue

            parent_ck = parent_def.get("container_key")
            parent_out_exists = (outdir / f"{parent_name}.ndjson").exists() or (outdir / "raw" / parent_name).exists()
            if not parent_out_exists:
                print(f"[INFO] Fetching parent listing '{parent_name}' from {parent_def['path']}")
                try:
                    paginate_collect(
                        headers, base_url, parent_def["path"], parent_def.get("params", {}), args.page_size,
                        parent_ck, outdir=outdir, name=parent_name, resume=args.resume, delay_ms=args.delay_ms,
                        max_pages=args.max_pages, raw_json=args.raw_json, current_group=endpoint_group(parent_def)
                    )
                except AuthzError as ae:
                    print(f"[WARN] {ae.endpoint_name}: auth failure ({ae.status_code}). Disabling group '{ae.group}' for this run.")
                    disabled_groups.add(ae.group)
                    summary["auth_failures"].append((ae.endpoint_name, ae.group, ae.status_code))
                    summary["skipped_due_to_group"].append({"name": name, "group": ae.group})
                    continue
                except NotFoundEndpointError as ne:
                    print(f"[WARN] {ne.endpoint_name}: 404 Not Found; skipping parent endpoint (group policy '{ne.group}').")
                    summary["skipped_due_to_404_policy"].append({"name": ne.endpoint_name, "group": ne.group})
                    continue

            print(f"[INFO] Expanding child collection {name} via {parent_name}")
            print(f"[DBG] single_object={ep.get('single_object', False)}, child={ep.get('name')}, url={ep.get('path')}")
            try:
                expand_child_collection(
                    headers, base_url, parent_name, ep, args.page_size, outdir, args.resume, args.delay_ms,
                    args.max_pages, parent_container_key=parent_ck, raw_json=args.raw_json, summary_tracker=summary,
                    checklists_non_empty_only=args.checklists_non_empty_only
                )
            except requests.exceptions.HTTPError as e:
                code = getattr(e.response, "status_code", None)
                text = getattr(e.response, "text", "") or ""
                if parent_name == "jobs" and is_archived_parent_error_text(code, text):
                    if os.getenv("HCP_ARCHIVED_JOB_FAST_FORWARD", "1") != "0":
                        print(f"[FAST-FWD] {name}: encountered archived job during expansion; skipping remaining parents for this child.")
                        summary.setdefault("fast_forwarded_due_to_archived", {}).setdefault(name, 0)
                        summary["fast_forwarded_due_to_archived"][name] += 1
                    else:
                        print(f"[WARN] {name}: archived job during expansion; skipping this parent and continuing.")
                else:
                    raise
            summary["processed_endpoints"].append(name)
            continue

        print(f"[INFO] Fetching {name} from {path}")
        try:
            paginate_collect(
                headers, base_url, path, params, args.page_size, container_key,
                outdir=outdir, name=name, resume=args.resume, delay_ms=args.delay_ms,
                max_pages=args.max_pages, raw_json=args.raw_json, current_group=group
            )
            summary["processed_endpoints"].append(name)
        except AuthzError as ae:
            print(f"[WARN] {ae.endpoint_name}: auth failure ({ae.status_code}). Disabling group '{ae.group}' for this run.")
            disabled_groups.add(ae.group)
            summary["auth_failures"].append((ae.endpoint_name, ae.group, ae.status_code))
            summary["skipped_due_to_group"].append({"name": name, "group": ae.group})
            continue
        except NotFoundEndpointError as ne:
            print(f"[WARN] {ne.endpoint_name}: 404 Not Found; skipping endpoint (group policy '{ne.group}').")
            summary["skipped_due_to_404_policy"].append({"name": ne.endpoint_name, "group": ne.group})
            continue

    manifest = {
        "base_url": base_url,
        "generated_at_utc": utcnow_iso(),
        "page_size": args.page_size,
        "endpoints": endpoints,
        "raw_json": bool(args.raw_json),
        "resume_cutoff_index": last_done_idx,
        "disabled_groups": sorted(disabled_groups),
        "disabled_endpoints": sorted(disabled_endpoints),
        "skip_error_substrings": sorted(parse_csv_env("HCP_SKIP_ERROR_SUBSTRINGS")),
        "treat_404_as_skip_groups": sorted(parse_csv_env("HCP_TREAT_404_AS_SKIP_GROUPS")),
        "summary": summary,
    }
    (outdir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n=== Run Summary ===")
    print(f"Output dir: {outdir}")
    print(f"Processed endpoints ({len(summary['processed_endpoints'])}): {', '.join(summary['processed_endpoints']) or '-'}")

    if summary["skipped_due_to_resume"]:
        print(f"Skipped due to resume ({len(summary['skipped_due_to_resume'])}): {', '.join(summary['skipped_due_to_resume'])}")
    if summary["skipped_due_to_group"]:
        grouped = {}
        for item in summary["skipped_due_to_group"]:
            grouped.setdefault(item['group'], []).append(item['name'])
        for grp, names in grouped.items():
            print(f"Skipped by disabled group '{grp}' ({len(names)}): {', '.join(names)}")
    if summary["skipped_due_to_404_policy"]:
        grouped_404 = {}
        for item in summary["skipped_due_to_404_policy"]:
            grouped_404.setdefault(item['group'], []).append(item['name'])
        for grp, names in grouped_404.items():
            print(f"Skipped due to 404 policy for group '{grp}' ({len(names)}): {', '.join(names)}")
    if summary["child_skips"]:
        for child_name, cnt in summary["child_skips"].items():
            print(f"Single-object child skips for '{child_name}': {cnt}")
    if summary["fast_forwarded_due_to_archived"]:
        for child_name, cnt in summary["fast_forwarded_due_to_archived"].items():
            print(f"Fast-forwarded '{child_name}' due to archived jobs: {cnt} time(s)")
    if manifest["disabled_groups"]:
        print(f"Disabled groups this run: {', '.join(manifest['disabled_groups'])}")
    if summary["auth_failures"]:
        details = ", ".join([f"{ep} [{grp}] {code}" for ep, grp, code in summary["auth_failures"]])
        print(f"Auth failures (caused group disable): {details}")

    print("Done.")

if __name__ == "__main__":
    main()
