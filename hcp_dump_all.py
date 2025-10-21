#!/usr/bin/env python3
"""
Housecall Pro data dump (GET-only) — API Key auth

Usage:
    python hcp_dump_all.py --out ./hcp_export [--resume] [--delay-ms 200]
    python hcp_dump_all.py --only jobs --out ./hcp_export

Env:
    HCP_API_KEY                     required
    HCP_BASE_URL                    optional, defaults to https://api.housecallpro.com

    # Optional feature toggles (all generic; no hardcoding of specific endpoints)
    HCP_DISABLE_GROUPS              CSV of endpoint groups to skip, e.g. "api/price_book,leads"
    HCP_DISABLE_ENDPOINTS           CSV of endpoint names to skip, e.g. "materials_from_categories,invoices"
    HCP_SKIP_ERROR_SUBSTRINGS       CSV of substrings; if HTTP error body contains any, skip that single-object child row
                                    e.g. "archived job,customer inactive,already deleted"
    HCP_TREAT_404_AS_SKIP_GROUPS    CSV of groups where a 404 should skip the whole endpoint (not crash)
                                    e.g. "api/price_book,booking_windows"

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
from dotenv import load_dotenv

load_dotenv()

# ---------------- Defaults ----------------

DEFAULT_PAGE_SIZE = 100

DEFAULT_ENDPOINTS: List[Dict[str, Any]] = [
    {"name": "customers", "path": "/customers", "params": {}, "container_key": "customers"},
    {"name": "employees", "path": "/employees", "params": {}, "container_key": "employees"},

    # Jobs with expansions included directly
    {"name": "jobs", "path": "/jobs", "params": {"page_size": DEFAULT_PAGE_SIZE, "expand": ["attachments", "appointments"]}, "container_key": "jobs"},

    # Children from jobs
    {"name": "appointments_from_jobs", "path": "/jobs/{id}/appointments", "expand_from": "jobs", "id_field": "id",
     "params": {}, "container_key": "appointments", "single_object": True},

    {"name": "invoices_from_jobs", "path": "/jobs/{id}/invoices", "expand_from": "jobs", "id_field": "id",
     "params": {}, "container_key": "invoices", "single_object": True},

    {"name": "job_types", "path": "/job_fields/job_types", "params": {}, "container_key": "job_types"},

    {"name": "leads", "path": "/leads", "params": {}, "container_key": "leads"},

    {"name": "lead_sources", "path": "/lead_sources", "params": {}, "container_key": "lead_sources"},

    {"name": "material_categories", "path": "/api/price_book/material_categories", "params": {}, "container_key": "data"},

    # Paginated child off categories
    {"name": "materials_from_categories", "path": "/api/price_book/materials", "expand_from": "material_categories", "id_field": "uuid",
     "params": {"material_category_uuid": "{uuid}"}, "container_key": "data"},

    {"name": "price_forms", "path": "/api/price_book/price_forms", "params": {}, "container_key": "data"},

    {"name": "company", "path": "/company", "params": {}, "single_object": True, "container_key": None},

    {"name": "schedule_availability", "path": "/company/schedule_availability", "params": {}, "single_object": True, "container_key": None},

    {"name": "booking_windows", "path": "/company/schedule_availability/booking_windows", "params": {}, "container_key": "booking_windows"},

    {"name": "events", "path": "/events", "params": {}, "container_key": "events"},

    {"name": "tags", "path": "/tags", "params": {}, "container_key": "tags"},

    {"name": "invoices", "path": "/invoices", "params": {}, "container_key": "invoices"},

    {"name": "estimates", "path": "/estimates", "params": {}, "container_key": "estimates"},

    # Checklist expansions
    {"name": "checklists_from_jobs", "path": "/checklists", "expand_from": "jobs", "id_field": "id",
     "params": {"job_uuids": ["{id}"]}, "container_key": "checklists"},
    {"name": "checklists_from_estimates", "path": "/checklists", "expand_from": "estimates", "id_field": "id",
     "params": {"estimate_uuids": ["{id}"]}, "container_key": "checklists"},

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
    # sort descending (newest first)
    return sorted(out, key=lambda p: p.name, reverse=True)

def resolve_outdir(out_root: pathlib.Path, resume: bool) -> pathlib.Path:
    """
    If resume:
      - If out_root is an existing run dir containing manifest.json, use it.
      - Else if out_root contains timestamped run subdirs, use the newest.
      - Else create a new timestamped subdir.
    If not resume:
      - Always create a new timestamped subdir under out_root (may be equal to out_root if user points to a timestamp dir).
    """
    if resume:
        if (out_root / "manifest.json").exists():
            ensure_dir(out_root)
            return out_root
        runs = list_timestamp_dirs(out_root)
        if runs:
            return runs[0]
        # fall through to create new
    # create new timestamp dir
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    outdir = out_root / stamp
    ensure_dir(outdir)
    return outdir

def compute_resume_cutoff(outdir: pathlib.Path, ordered_names: List[str]) -> int:
    """
    Return the highest index i for which '<name>.ndjson' exists in outdir.
    If none exist, return -1.
    """
    last_done = -1
    for i, name in enumerate(ordered_names):
        nd = outdir / f"{name}.ndjson"
        if nd.exists():
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
    """Encode list params with [] suffix and pass through scalars."""
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

def write_ndjson_line(nd_path: Optional[pathlib.Path], payload: Any, container_key: Optional[str]) -> None:
    if nd_path is None:
        return
    items, _ = extract_items(payload, container_key)
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
    """
    Group endpoints by feature so a single auth failure disables the whole family.
    Priority:
      1) explicit 'group' field in endpoint spec (if present)
      2) '/api/<feature>/...' -> 'api/<feature>'
      3) first path segment, e.g. '/jobs' -> 'jobs'
    """
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

def is_authz_error(exc: requests.exceptions.HTTPError) -> bool:
    resp = getattr(exc, "response", None)
    return bool(resp) and resp.status_code in (401, 403)

def is_not_found_error(exc: requests.exceptions.HTTPError) -> bool:
    resp = getattr(exc, "response", None)
    return bool(resp) and resp.status_code == 404

def matches_skip_text(exc: requests.exceptions.HTTPError, substrings: Iterable[str]) -> bool:
    resp = getattr(exc, "response", None)
    if not resp or not substrings:
        return False
    body = (resp.text or "").lower()
    return any(s.lower() in body for s in substrings)

def should_skip_endpoint_due_to_group(ep: dict, disabled_groups: set) -> bool:
    return endpoint_group(ep) in disabled_groups

# ---------------- HTTP ----------------

TRANSIENT_STATUSES = {408, 425, 429, 500, 502, 503, 504}

def robust_get(url: str, params: Dict[str, Any], headers: Dict[str, str], retries: int = 6) -> Tuple[Optional[Any], requests.Response]:
    backoff = 1.0
    last: Optional[requests.Response] = None
    for attempt in range(1, retries + 1):
        r = requests.get(url, params=params, headers=headers, timeout=60)
        if r.status_code in TRANSIENT_STATUSES:
            last = r
            retry_after = r.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                time.sleep(int(retry_after))
            else:
                # jitter to reduce thundering herd
                sleep_for = backoff + random.uniform(0, backoff * 0.25)
                time.sleep(sleep_for)
                backoff = min(backoff * 2, 30)
            continue
        if r.status_code >= 400:
            # Emit limited server text for diagnosis, then raise
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
    if last is not None:
        print(f"[ERROR] {last.status_code} {url} :: {last.text[:500]}")
        last.raise_for_status()
    raise RuntimeError("robust_get: exhausted retries with no response")

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
) -> int:
    """
    Returns the number of pages fetched.
    """
    assert not resume or (outdir is not None and name is not None), "resume requires outdir and name"

    # Prepare raw directory
    raw_dir: Optional[pathlib.Path] = None
    if raw_json and outdir and name:
        raw_dir = outdir / "raw" / (name if parent_dir_suffix is None else f"{name}/{parent_dir_suffix}")
        ensure_dir(raw_dir)

    nd_path = outdir / f"{name}.ndjson" if outdir and name else None

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
        payload, resp = robust_get(url, encode_params(params), session_headers)
        if resp.status_code == 204:
            break

        # Write raw page
        if raw_dir:
            raw_file = raw_dir / f"page_{page}.json"
            raw_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        # Append to NDJSON
        write_ndjson_line(nd_path, payload, container_key)

        # Stop conditions
        items, _ = extract_items(payload, container_key)
        count = len(items)
        if count == 0:
            break
        pages_fetched += 1

        # Heuristic: many APIs stop returning a full page on the last page
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
) -> None:
    id_field = child_def.get("id_field", "id")
    child_name = child_def["name"]
    container_key = child_def.get("container_key")
    single_object = child_def.get("single_object", False)

    # Read parents from disk. Prefer raw pages when available to avoid needing NDJSON backfill ordering.
    parents = list(read_parent_items_from_raw(outdir, listing_name, parent_container_key))
    if not parents:
        parents = list(read_parent_items_from_ndjson(outdir, listing_name))

    if not parents:
        print(f"[WARN] No parent items found for '{listing_name}', cannot expand '{child_name}'")
        return

    # Prepare single-object outputs
    nd_path: Optional[pathlib.Path] = None
    raw_dir_single: Optional[pathlib.Path] = None
    if single_object:
        nd_path = outdir / f"{child_name}.ndjson"
        if raw_json:
            raw_dir_single = outdir / "raw" / child_name
            ensure_dir(raw_dir_single)

    # env-configurable skip reasons for single-object calls
    skip_substrings = parse_csv_env("HCP_SKIP_ERROR_SUBSTRINGS")

    for idx, it in enumerate(parents, 1):
        if id_field not in it:
            continue
        parent_id = it[id_field]
        call_params = substitute_params(child_def.get("params", {}), it)
        path = child_def["path"].replace("{id}", str(parent_id))
        url = f"{base_url}{path}"

        if single_object:
            try:
                payload, _ = robust_get(url, encode_params(call_params), session_headers)
            except requests.exceptions.HTTPError as e:
                # Generic skip-once rules:
                # - 404: often means no child object exists
                # - error text contains any configured substrings
                if is_not_found_error(e) or matches_skip_text(e, skip_substrings):
                    print(f"[WARN] Skipping {child_name} for parent {parent_id} due to {getattr(e.response, 'status_code', None)} / matched skip text")
                    if summary_tracker is not None:
                        summary_tracker.setdefault("child_skips", {}).setdefault(child_name, 0)
                        summary_tracker["child_skips"][child_name] += 1
                    continue
                # Anything else: bubble up so the caller can decide (may disable group on 401/403)
                raise

            if raw_json and raw_dir_single:
                (raw_dir_single / f"{parent_id}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            write_ndjson_line(nd_path, payload, container_key=container_key)
        else:
            # Paginated child collection. Important: isolate raw pages per parent to avoid collisions.
            parent_suffix = f"{listing_name}_{parent_id}"
            paginate_collect(
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
            )

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
    args = ap.parse_args()

    out_root = pathlib.Path(args.out).resolve()
    # Choose the folder to WRITE INTO
    outdir = resolve_outdir(out_root, args.resume)
    ensure_dir(outdir)

    # Load/validate endpoints
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

    # Normalize page size on every endpoint (if not set explicitly)
    normalized: List[Dict[str, Any]] = []
    for ep in endpoints:
        ep = dict(ep)
        p = dict(ep.get("params") or {})
        p.setdefault("page_size", args.page_size)
        ep["params"] = p
        normalized.append(ep)
    endpoints = normalized

    # Resume cutoff
    ordered_names = [ep["name"] for ep in endpoints]
    last_done_idx = compute_resume_cutoff(outdir, ordered_names) if args.resume else -1
    if args.resume and last_done_idx >= 0:
        print(f"[INFO] Resume enabled. Detected progress up to '{ordered_names[last_done_idx]}' (index {last_done_idx}).")
        print("[INFO] All endpoints at or before that index will be skipped.")

    # Generic disables
    disabled_groups: set[str] = set()
    disabled_groups |= parse_csv_env("HCP_DISABLE_GROUPS")  # e.g. api/price_book
    disabled_endpoints = parse_csv_env("HCP_DISABLE_ENDPOINTS")  # by endpoint name

    if disabled_endpoints:
        endpoints = [ep for ep in endpoints if ep.get("name") not in disabled_endpoints]

    # Tracking for end-of-run summary
    summary = {
        "skipped_due_to_resume": [],
        "skipped_due_to_group": [],
        "skipped_due_to_404_policy": [],
        "child_skips": {},  # child_name -> count
        "auth_failures": [],  # (endpoint, group, status)
        "processed_endpoints": [],
    }

    # Process endpoints
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

        if should_skip_endpoint_due_to_group(ep, disabled_groups):
            print(f"[SKIP] {name} (group '{group}' disabled)")
            summary["skipped_due_to_group"].append({"name": name, "group": group})
            continue

        try:
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
                            max_pages=args.max_pages, raw_json=args.raw_json
                        )
                    except requests.exceptions.HTTPError as e:
                        # If we can’t even fetch the parent due to auth, disable the parent’s group and skip this child.
                        pgroup = endpoint_group(parent_def)
                        if is_authz_error(e):
                            code = getattr(e.response, "status_code", None)
                            print(f"[WARN] {parent_name}: auth failure ({code}). Disabling group '{pgroup}' for this run.")
                            disabled_groups.add(pgroup)
                            summary["auth_failures"].append((parent_name, pgroup, code))
                            summary["skipped_due_to_group"].append({"name": name, "group": pgroup})
                            continue
                        # Optional policy: treat 404 as skip endpoint for certain groups
                        treat_404_groups = parse_csv_env("HCP_TREAT_404_AS_SKIP_GROUPS")
                        if is_not_found_error(e) and pgroup in treat_404_groups:
                            print(f"[WARN] {parent_name}: 404 Not Found; skipping parent endpoint (group policy).")
                            summary["skipped_due_to_404_policy"].append({"name": parent_name, "group": pgroup})
                            continue
                        raise

                print(f"[INFO] Expanding child collection {name} via {parent_name}")
                expand_child_collection(
                    headers, base_url, parent_name, ep, args.page_size, outdir, args.resume, args.delay_ms,
                    args.max_pages, parent_container_key=parent_ck, raw_json=args.raw_json, summary_tracker=summary
                )
                summary["processed_endpoints"].append(name)

            else:
                print(f"[INFO] Fetching {name} from {path}")
                paginate_collect(
                    headers, base_url, path, params, args.page_size, container_key,
                    outdir=outdir, name=name, resume=args.resume, delay_ms=args.delay_ms,
                    max_pages=args.max_pages, raw_json=args.raw_json
                )
                summary["processed_endpoints"].append(name)

        except requests.exceptions.HTTPError as e:
            # 401/403: disable entire group for this run and continue
            if is_authz_error(e):
                code = getattr(e.response, "status_code", None)
                print(f"[WARN] {name}: auth failure ({code}). Disabling group '{group}' for this run.")
                disabled_groups.add(group)
                summary["auth_failures"].append((name, group, code))
                summary["skipped_due_to_group"].append({"name": name, "group": group})
                continue

            # Optional: treat 404 as “skip this endpoint” (not whole group). Opt-in per group via env.
            treat_404_groups = parse_csv_env("HCP_TREAT_404_AS_SKIP_GROUPS")
            if is_not_found_error(e) and group in treat_404_groups:
                print(f"[WARN] {name}: 404 Not Found; skipping endpoint (group policy).")
                summary["skipped_due_to_404_policy"].append({"name": name, "group": group})
                continue

            # Anything else: fail loud.
            raise


    # Write or update manifest
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

    # ------------ End-of-run concise summary (printed) ------------
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
    if manifest["disabled_groups"]:
        print(f"Disabled groups this run: {', '.join(manifest['disabled_groups'])}")
    if summary["auth_failures"]:
        details = ", ".join([f"{ep} [{grp}] {code}" for ep, grp, code in summary["auth_failures"]])
        print(f"Auth failures (caused group disable): {details}")

    print("Done.")

if __name__ == "__main__":
    main()
