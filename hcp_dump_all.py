#!/usr/bin/env python3
"""
Housecall Pro data dump (GET-only) — API Key auth
Usage:
    python hcp_dump_all.py --out ./hcp_export [--resume] [--delay-ms 200]
    python hcp_dump_all.py --only jobs --out ./hcp_export

NOTE:
    Requires HCP_API_KEY environment variable to be set (use a .env file with python-dotenv).
"""
import os, sys, json, time, argparse, pathlib, datetime, requests, re
from dotenv import load_dotenv
load_dotenv()

# -------- Endpoint definitions (verbose defaults) --------
DEFAULT_ENDPOINTS = [
    {"name": "customers", "path": "/customers", "params": {}, "container_key": "customers"},
    {"name": "employees", "path": "/employees", "params": {}, "container_key": "employees"},

    # Jobs with expansions for attachments and appointments included directly
    {"name": "jobs", "path": "/jobs", "params": {"page_size": 100, "expand": ["attachments", "appointments"]}, "container_key": "jobs"},

    # Appointments from jobs (non-paginated child)
    {"name": "appointments_from_jobs", "path": "/jobs/{id}/appointments", "expand_from": "jobs", "id_field": "id",
     "params": {}, "container_key": "appointments", "single_object": True},

    # Invoices from jobs (non-paginated child)
    {"name": "invoices_from_jobs", "path": "/jobs/{id}/invoices", "expand_from": "jobs", "id_field": "id",
     "params": {}, "container_key": "invoices", "single_object": True},

    {"name": "job_types", "path": "/job_fields/job_types", "params": {}, "container_key": "job_types"},

    {"name": "leads", "path": "/leads", "params": {}, "container_key": "leads"},

    {"name": "lead_sources", "path": "/lead_sources", "params": {}, "container_key": "lead_sources"},

    {"name": "application", "path": "/application", "params": {}, "single_object": True, "container_key": None},

    {"name": "material_categories", "path": "/api/price_book/material_categories", "params": {}, "container_key": "data"},

    # Materials from material categories (paginated child)
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

    # Checklist expansions (paginated)
    {"name": "checklists_from_jobs", "path": "/checklists", "expand_from": "jobs", "id_field": "id",
     "params": {"job_uuids": ["{id}"]}, "container_key": "checklists"},
    {"name": "checklists_from_estimates", "path": "/checklists", "expand_from": "estimates", "id_field": "id",
     "params": {"estimate_uuids": ["{id}"]}, "container_key": "checklists"},

    {"name": "payments", "path": "/payments", "params": {}, "container_key": "payments"},
]

# -------- Helpers --------

def auth_headers():
    key = os.getenv("HCP_API_KEY")
    if not key:
        sys.exit("Missing HCP_API_KEY. Set it in a .env file or your environment.")
    return {"Authorization": f"Token {key}", "Accept": "application/json"}

def get_base_url():
    return os.getenv("HCP_BASE_URL", "https://api.housecallpro.com").rstrip("/")

def robust_get(url, params, headers, retries=5, timeout=120):
    backoff = 1.0
    last = None
    for _ in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
        except requests.exceptions.Timeout as e:
            print(f"[WARN] Timeout occurred for {url}, retrying...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            last = r
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        if r.status_code >= 400:
            # Print server complaint to help debugging, then raise
            try:
                print(f"[ERROR] {r.status_code} {url} :: {r.text[:500]}")
            except Exception:
                pass
            r.raise_for_status()
        try:
            return r.json(), r
        except ValueError:
            # Some endpoints may return empty body (204 handled earlier), or non-JSON
            return None, r
    if last is not None:
        print(f"[ERROR] {last.status_code} {url} :: {last.text[:500]}")
        last.raise_for_status()
    raise RuntimeError("robust_get: exhausted retries")

def extract_items(payload, container_key=None):
    if payload is None:
        return [], None
    if container_key and isinstance(payload, dict) and container_key in payload and isinstance(payload[container_key], list):
        return payload[container_key], container_key
    if isinstance(payload, list):
        return payload, None
    if isinstance(payload, dict):
        for key in ("items", "results", "data"):
            if key in payload and isinstance(payload[key], list):
                return payload[key], key
        return [], None
    return [], None

def substitute_params(template_params, parent_obj):
    params = {}
    for k, v in (template_params or {}).items():
        if isinstance(v, list):
            arr = []
            for item in v:
                if isinstance(item, str) and item.startswith("{") and item.endswith("}"):
                    field = item[1:-1]
                    arr.append(str(parent_obj.get(field, "")))
                else:
                    arr.append(item)
            params[k] = arr
        elif isinstance(v, str) and v.startswith("{") and v.endswith("}"):
            field = v[1:-1]
            params[k] = str(parent_obj.get(field, ""))
        else:
            params[k] = v
    return params

def existing_pages_for(outdir, name):
    raw_dir = outdir / "raw" / name
    if not raw_dir.exists():
        return set()
    pages = set()
    for p in raw_dir.glob("page_*.json"):
        m = re.match(r"page_(\d+)\.json$", p.name)
        if m:
            pages.add(int(m.group(1)))
    return pages

def encode_params(params: dict) -> dict:
    """Encode list params with [] suffix (e.g., expand[])."""
    out = {}
    for k, v in (params or {}).items():
        if isinstance(v, list):
            out[f"{k}[]"] = v  # requests => k[]=a&k[]=b
        else:
            out[k] = v
    return out

def write_ndjson_line(nd_path, payload, container_key):
    """Append one page/object worth of rows to <name>.ndjson."""
    if nd_path is None:
        return
    items, _ = extract_items(payload, container_key)
    with nd_path.open("a", encoding="utf-8") as f:
        if isinstance(items, list) and items:
            for row in items:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        elif isinstance(payload, dict) and payload:
            # For single-object children or fallback
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

def iter_parent_items(outdir, parent_name, parent_container_key):
    raw_dir = outdir / "raw" / parent_name
    if not raw_dir.exists():
        return
    for p in sorted(raw_dir.glob("page_*.json")):
        with p.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        items, _ = extract_items(payload, parent_container_key)
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    yield it

# -------- Pagination and child expansion --------

def paginate_collect(session_headers, base_url, path, base_params, page_size, container_key=None,
                     outdir=None, name=None, resume=False, delay_ms=0, max_pages=None):
    assert not resume or (outdir is not None and name is not None), "resume needs outdir and name"
    raw_dir = outdir / "raw" / name if outdir and name else None
    if raw_dir:
        raw_dir.mkdir(parents=True, exist_ok=True)

    seen = existing_pages_for(outdir, name) if resume else set()
    nd_path = outdir / f"{name}.ndjson" if outdir and name else None
    page = 1
    while True:
        if max_pages and page > max_pages:
            print(f"[TEST] Stopping {name} at page {max_pages} for testing")
            break

        if resume and page in seen:
            # Backfill NDJSON from already-saved raw page so ndjson stays in sync
            if nd_path and raw_dir:
                raw_file = raw_dir / f"page_{page}.json"
                if raw_file.exists():
                    with raw_file.open("r", encoding="utf-8") as rf:
                        payload = json.load(rf)
                    write_ndjson_line(nd_path, payload, container_key)
            page += 1
            continue

        params = dict(base_params or {})
        params.setdefault("page", page)
        params.setdefault("page_size", page_size)
        url = f"{base_url}{path}"
        payload, resp = robust_get(url, encode_params(params), session_headers)
        if resp.status_code == 204:
            break

        # Write raw page immediately
        if raw_dir:
            with (raw_dir / f"page_{page}.json").open("w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

        # Append to NDJSON
        write_ndjson_line(nd_path, payload, container_key)

        # Stop when no items found
        items, _ = extract_items(payload, container_key)
        if not items:
            break

        page += 1
        if delay_ms:
            time.sleep(delay_ms / 1000.0)

def expand_child_collection(session_headers, base_url, listing_name, listing_pages, child_def,
                            page_size, outdir, resume, delay_ms, max_pages=None, parent_container_key=None):
    """
    Handles both paginated children (e.g., /jobs/{id}/appointments) and single-object children
    (e.g., /jobs/{id} with expand lists).
    """
    id_field = child_def.get("id_field", "id")
    child_name = child_def["name"]
    container_key = child_def.get("container_key")
    single_object = child_def.get("single_object", False)

    # Collect parent items from in-memory pages first
    parent_items = []
    for pg in (listing_pages or []):
        payload = pg.get("payload")
        items, _ = extract_items(payload, parent_container_key)
        if isinstance(items, list):
            parent_items.extend([it for it in items if isinstance(it, dict)])

    # Fallback: read parents from disk if memory cache had no items
    if not parent_items and parent_container_key:
        for it in iter_parent_items(outdir, listing_name, parent_container_key):
            parent_items.append(it)

    if not parent_items:
        print(f"[WARN] No parent items found for '{listing_name}', cannot expand '{child_name}'")
        return

    # Prepare output locations for single-object children
    if single_object:
        raw_dir = outdir / "raw" / child_name
        raw_dir.mkdir(parents=True, exist_ok=True)
        nd_path = outdir / f"{child_name}.ndjson"

    for it in parent_items:
        if id_field not in it:
            continue
        parent_id = it[id_field]
        call_params = substitute_params(child_def.get("params", {}), it)
        path = child_def["path"].replace("{id}", str(parent_id))
        url = f"{base_url}{path}"

        if single_object:
            # One GET per parent; write raw as <parent_id>.json and append object to NDJSON
            try:
                payload, _ = robust_get(url, encode_params(call_params), session_headers)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 400 and "archived job" in e.response.text.lower():
                    break
                else:
                    raise
            with (raw_dir / f"{parent_id}.json").open("w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            write_ndjson_line(nd_path, payload, container_key=container_key)  # Use container_key here
        else:
            # Paginated child collection
            paginate_collect(session_headers, base_url, path, call_params, page_size,
                             container_key, outdir=outdir, name=child_name,
                             resume=resume, delay_ms=delay_ms, max_pages=max_pages)

# -------- Main --------

def main():
    base_url = get_base_url()
    headers = auth_headers()

    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./hcp_export", help="Output directory")
    ap.add_argument("--page-size", type=int, default=100, help="page_size to request")
    ap.add_argument("--endpoints-json", help="Optional JSON file to override endpoints")
    ap.add_argument("--resume", action="store_true", help="Skip already saved pages (and backfill NDJSON)")
    ap.add_argument("--delay-ms", type=int, default=0, help="Sleep between requests (ms)")
    ap.add_argument("--only", help="Only run a single endpoint by name (e.g. jobs)")
    ap.add_argument("--max-pages", type=int, help="Limit pages fetched per endpoint (testing)")
    args = ap.parse_args()

    outroot = pathlib.Path(args.out).resolve()
    outdir = outroot / datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    outdir.mkdir(parents=True, exist_ok=True)

    endpoints = DEFAULT_ENDPOINTS
    if args.endpoints_json:
        try:
            with open(args.endpoints_json, "r", encoding="utf-8") as f:
                endpoints = json.load(f)
        except FileNotFoundError:
            print(f"[WARN] Endpoints JSON {args.endpoints_json} not found, falling back to DEFAULT_ENDPOINTS")

    if args.only:
        endpoints = [ep for ep in endpoints if ep["name"] == args.only]

    cache = {}
    for ep in endpoints:
        name = ep["name"]
        path = ep["path"]
        params = ep.get("params", {})
        container_key = ep.get("container_key")

        if "expand_from" in ep:
            parent_name = ep["expand_from"]
            parent_pages = cache.get(parent_name)

            # find the parent container_key for disk fallback
            parent_def = next((e for e in endpoints if e["name"] == parent_name and "expand_from" not in e), None)
            if not parent_def:
                parent_def = next((e for e in DEFAULT_ENDPOINTS if e["name"] == parent_name and "expand_from" not in e), None)
            parent_ck = parent_def.get("container_key") if parent_def else None

            if parent_pages is None:
                # Auto-fetch parent listing (writes raw pages; we then read from disk)
                print(f"[INFO] Auto-fetching parent listing '{parent_name}' from {parent_def['path'] if parent_def else '(unknown)'}")
                paginate_collect(headers, base_url, parent_def["path"], parent_def.get("params", {}), args.page_size,
                                 parent_def.get("container_key"), outdir=outdir, name=parent_name,
                                 resume=args.resume, delay_ms=args.delay_ms, max_pages=args.max_pages)
                # Put a sentinel in cache to indicate "available"; items will be read from disk
                cache[parent_name] = [{"payload": {}}]

            print(f"[INFO] Expanding child collection {name} via {parent_name}")
            expand_child_collection(headers, base_url, parent_name, cache.get(parent_name), ep,
                                    args.page_size, outdir, args.resume, args.delay_ms, args.max_pages,
                                    parent_container_key=parent_ck)
            continue

        print(f"[INFO] Fetching {name} from {path}")
        paginate_collect(headers, base_url, path, params, args.page_size, container_key,
                         outdir=outdir, name=name, resume=args.resume, delay_ms=args.delay_ms,
                         max_pages=args.max_pages)
        # We don't cache payloads in-memory (large). Disk is the source of truth for children.

    manifest = {
        "base_url": base_url,
        "generated_at_utc": datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z"),
        "page_size": args.page_size,
        "endpoints": endpoints,
    }
    with (outdir / "manifest.json").open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"Done. Export written to: {outdir}")

if __name__ == "__main__":
    main()