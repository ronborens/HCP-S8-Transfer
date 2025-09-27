#!/usr/bin/env python3
"""
Housecall Pro data dump (GET-only) — API Key auth
Usage:
    python hcp_dump_all.py --out ./hcp_export [--resume] [--delay-ms 200]
    python hcp_dump_all.py --only jobs --out ./hcp_export

NOTE:
    Requires HCP_API_KEY environment variable to be set.
"""
import os, sys, json, time, argparse, pathlib, datetime, requests, re
from dotenv import load_dotenv
load_dotenv()

DEFAULT_ENDPOINTS = [
    {"name": "customers", "path": "/customers", "params": {}, "container_key": "customers"},
    {"name": "employees", "path": "/employees", "params": {}, "container_key": "employees"},
    {"name": "jobs", "path": "/jobs", "params": { "expand": ["attachments"],"page_size": 100 }, "container_key": "jobs"},
    {"name": "job_attachments", "path": "/jobs/{id}/attachments",
    "expand_from": "jobs", "id_field": "id", "params": {}, "container_key": "attachments"},
    {"name": "estimates", "path": "/estimates", "params": {}, "container_key": "estimates"},
    {"name": "checklists_from_jobs", "path": "/checklists", "expand_from": "jobs", "id_field": "id",
     "params": {"job_uuids": ["{id}"]}, "container_key": "checklists"},
    {"name": "checklists_from_estimates", "path": "/checklists", "expand_from": "estimates", "id_field": "id",
     "params": {"estimate_uuids": ["{id}"]}, "container_key": "checklists"},
    {"name": "appointments_from_jobs", "path": "/jobs/{id}/appointments", "expand_from": "jobs",
     "id_field": "id", "params": {}, "container_key": "appointments"},
]

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


def write_ndjson_line(nd_path, payload, container_key):
    """Append one page's worth of rows to <name>.ndjson."""
    if nd_path is None:
        return
    items, _ = extract_items(payload, container_key)
    with nd_path.open("a", encoding="utf-8") as f:
        if isinstance(items, list) and items:
            for row in items:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        elif isinstance(payload, dict) and payload:
            # Fallback: write whole object if we couldn't find a list
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def fetch_parent_if_needed(headers, base_url, endpoints, cache, parent_name, page_size, outdir, resume, delay_ms):
    if cache.get(parent_name) is not None:
        return cache[parent_name]
    # Look in loaded endpoints first…
    parent_def = next((e for e in endpoints if e["name"] == parent_name and "expand_from" not in e), None)
    # …then fall back to DEFAULT_ENDPOINTS so --only still works
    if not parent_def:
        parent_def = next((e for e in DEFAULT_ENDPOINTS if e["name"] == parent_name and "expand_from" not in e), None)
    if not parent_def:
        print(f"[WARN] Parent '{parent_name}' has no standalone endpoint in your endpoints list; cannot auto-fetch.")
        return None
    print(f"[INFO] Auto-fetching parent listing '{parent_name}' from {parent_def['path']}")
    paginate_collect(headers, base_url, parent_def["path"], parent_def.get("params", {}), page_size,
                     parent_def.get("container_key"), outdir=outdir, name=parent_name, resume=resume, delay_ms=delay_ms, max_pages=page_size)
    cache[parent_name] = [{"payload": {}}]
    return cache[parent_name]



def auth_headers():
    key = os.getenv("HCP_API_KEY")
    if not key:
        sys.exit("Missing HCP_API_KEY. Please set it with: export HCP_API_KEY=your_api_key")
    return {"Authorization": f"Token {key}", "Accept": "application/json"}

def get_base_url():
    return os.getenv("HCP_BASE_URL", "https://api.housecallpro.com").rstrip("/")

def robust_get(url, params, headers, retries=5):
    backoff = 1.0
    for _ in range(retries):
        r = requests.get(url, params=params, headers=headers, timeout=60)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        r.raise_for_status()
        try:
            return r.json(), r
        except ValueError:
            return None, r
    r.raise_for_status()

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
    out = {}
    for k, v in (params or {}).items():
        if isinstance(v, list):
            out[f"{k}[]"] = v  # -> expand[]=attachments&expand[]=appointments
        else:
            out[k] = v
    return out

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
            # Backfill NDJSON from already-saved raw page
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
        items, _ = extract_items(payload, container_key)
        if raw_dir:
            with (raw_dir / f"page_{page}.json").open("w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        write_ndjson_line(nd_path, payload, container_key)
        items, _ = extract_items(payload, container_key)
        if not items:
            break
        page += 1
        if delay_ms:
            time.sleep(delay_ms / 1000.0)

def expand_child_collection(session_headers, base_url, listing_name, listing_pages, child_def,
                            page_size, outdir, resume, delay_ms, parent_container_key=None):
    id_field = child_def.get("id_field", "id")
    child_name = child_def["name"]

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

    for it in parent_items:
        if id_field not in it:
            continue
        parent_id = it[id_field]
        call_params = substitute_params(child_def.get("params", {}), it)
        path = child_def["path"].replace("{id}", str(parent_id))
        paginate_collect(session_headers, base_url, path, call_params, page_size,
                         child_def.get("container_key"),
                         outdir=outdir, name=child_name, resume=resume, delay_ms=delay_ms)


def main():
    base_url = get_base_url()
    headers = auth_headers()

    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./hcp_export", help="Output directory")
    ap.add_argument("--page-size", type=int, default=100, help="page_size to request")
    ap.add_argument("--endpoints-json", help="Optional JSON file to override endpoints")
    ap.add_argument("--resume", action="store_true", help="Skip already saved pages")
    ap.add_argument("--delay-ms", type=int, default=0, help="Sleep between requests (ms)")
    ap.add_argument("--only", help="Only run a single endpoint by name (e.g. jobs)")
    ap.add_argument("--max-pages", type=int, help="Limit pages fetched per endpoint (for testing)")

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
            parent_def = next((e for e in endpoints if e["name"] == parent_name and "expand_from" not in e), None)
            if not parent_def:
                parent_def = next((e for e in DEFAULT_ENDPOINTS if e["name"] == parent_name and "expand_from" not in e), None)
            parent_ck = parent_def.get("container_key") if parent_def else None
            if parent_pages is None:
                parent_pages = fetch_parent_if_needed(headers, base_url, endpoints, cache,
                                              parent_name, args.page_size, outdir,
                                              args.resume, args.delay_ms)
                if parent_pages is None:
                    print(f"[WARN] Skipping {name} because parent {parent_name} could not be fetched.")
                    continue
            print(f"[INFO] Expanding child collection {name} via {parent_name}")
            expand_child_collection(headers, base_url, parent_name, parent_pages, ep,
                            args.page_size, outdir, args.resume, args.delay_ms,
                            parent_container_key=parent_ck)
            continue

        print(f"[INFO] Fetching {name} from {path}")
        pages = []
        paginate_collect(headers, base_url, path, params, args.page_size, container_key,
                         outdir=outdir, name=name, resume=args.resume, delay_ms=args.delay_ms, max_pages=args.max_pages)
        cache[name] = pages

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
