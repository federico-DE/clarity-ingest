#!/usr/bin/env python3
"""Daily Microsoft Clarity Data Export ingest.

Pulls the configured calls (numOfDays=1), stores raw JSON per call under
data/raw/<date>/, writes a long-format CSV to data/clean/<date>.csv and a
manifest with status, row counts and truncation flags.

Stdlib only. Env:
  CLARITY_API_TOKEN  required (GitHub secret)
  CLARITY_ENDPOINT   optional override (testing)
"""
import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CFG = json.loads((ROOT / "config" / "ingest.json").read_text())
PT = json.loads((ROOT / "config" / "page_types.json").read_text())

LANGS = set(PT["locale_languages"])
PREFIX_RE = re.compile(r"^/([a-z]{2})(?:-([a-z]{2}))?(?=/|$)")
RULES = [(r["page_type"], re.compile(r["pattern"])) for r in PT["rules"]]

CSV_COLS = [
    "date", "call_id", "metric", "row", "url", "path", "market", "language",
    "country", "page_type", "device", "channel", "field", "value",
]


def classify(url: str) -> dict:
    """Split a Clarity URL into path, locale prefix and page type."""
    if not url:
        return {"path": "", "market": "", "language": "", "country": "", "page_type": ""}
    parsed = urllib.parse.urlparse(url if "://" in url else "https://x" + (url if url.startswith("/") else "/" + url))
    path = parsed.path.lower().rstrip("/") or "/"
    market, lang, country = "default", "", ""
    m = PREFIX_RE.match(path)
    if m and m.group(1) in LANGS:
        lang, country = m.group(1), m.group(2) or ""
        market = m.group(0).lstrip("/")
        path = path[m.end():] or "/"
    page_type = PT["fallback"]
    for name, rx in RULES:
        if rx.search(path):
            page_type = name
            break
    return {"path": path, "market": market, "language": lang, "country": country, "page_type": page_type}


def fetch(dimensions, token, endpoint, num_days):
    params = {"numOfDays": num_days}
    for i, d in enumerate(dimensions, 1):
        params[f"dimension{i}"] = d
    url = endpoint + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    last_err = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.status, json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")[:500]
            if e.code in (400, 401, 403, 429):  # not retryable; 429 would burn quota
                return e.code, {"error": body}
            last_err = (e.code, {"error": body})
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = (0, {"error": str(e)})
        time.sleep(5 * (attempt + 1))
    return last_err


def flatten(date, call_id, dims, payload, warnings):
    """Long format: one row per (metric, response row, numeric/text field)."""
    out = []
    counts = {}
    if not isinstance(payload, list):
        warnings.append(f"{call_id}: unexpected payload type {type(payload).__name__}")
        return out, counts
    for metric in payload:
        name = metric.get("metricName", "")
        rows = metric.get("information") or []
        counts[name] = len(rows)
        for i, row in enumerate(rows):
            dimvals = {d: row.get(d) for d in dims}
            missing = [d for d in dims if d not in row]
            if missing and i == 0:
                warnings.append(f"{call_id}/{name}: dimension key(s) {missing} not in rows; keys={sorted(row)}")
            url = dimvals.get("URL") or ""
            c = classify(url) if "URL" in dims else {"path": "", "market": "", "language": "", "country": "", "page_type": ""}
            base = {
                "date": date, "call_id": call_id, "metric": name, "row": i, "url": url,
                **c,
                "device": dimvals.get("Device") or "",
                "channel": dimvals.get("Channel") or "",
            }
            for k, v in row.items():
                if k in dims:
                    continue
                out.append({**base, "field": k, "value": v})
    return out, counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="Label date YYYY-MM-DD (default: yesterday UTC)")
    ap.add_argument("--force", action="store_true", help="Re-fetch calls already stored for the date")
    ap.add_argument("--rebuild-only", action="store_true", help="Rebuild CSV/manifest from stored raw JSON, no API calls")
    args = ap.parse_args()

    date = args.date or (dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)).isoformat()
    raw_dir = ROOT / "data" / "raw" / date
    clean_dir = ROOT / "data" / "clean"
    raw_dir.mkdir(parents=True, exist_ok=True)
    clean_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = raw_dir / "_manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {"date": date, "calls": {}}
    manifest["num_of_days"] = CFG["num_of_days"]

    token = os.environ.get("CLARITY_API_TOKEN", "")
    endpoint = os.environ.get("CLARITY_ENDPOINT", CFG["endpoint"])
    failed = False

    if not args.rebuild_only:
        if not token:
            print("CLARITY_API_TOKEN not set", file=sys.stderr)
            return 2
        for call in CFG["calls"]:
            cid, dims = call["id"], call["dimensions"]
            raw_file = raw_dir / f"{cid}.json"
            if raw_file.exists() and not args.force:
                print(f"skip {cid}: already stored")
                continue
            status, payload = fetch(dims, token, endpoint, CFG["num_of_days"])
            entry = {
                "dimensions": dims,
                "http_status": status,
                "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            }
            if status == 200:
                raw_file.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
                entry["ok"] = True
            else:
                entry["ok"] = False
                entry["error"] = payload.get("error") if isinstance(payload, dict) else str(payload)
                failed = True
                print(f"FAIL {cid}: HTTP {status} {entry['error'][:200]}", file=sys.stderr)
                if status in (401, 403, 429):
                    manifest["calls"][cid] = entry
                    break  # token or quota problem: stop, don't burn remaining calls
            manifest["calls"][cid] = entry

    # Build clean CSV from whatever raw files exist for the date
    all_rows, warnings = [], []
    for call in CFG["calls"]:
        cid, dims = call["id"], call["dimensions"]
        raw_file = raw_dir / f"{cid}.json"
        if not raw_file.exists():
            continue
        rows, counts = flatten(date, cid, dims, json.loads(raw_file.read_text()), warnings)
        all_rows.extend(rows)
        entry = manifest["calls"].setdefault(cid, {"dimensions": dims, "ok": True})
        entry["row_counts"] = counts
        entry["truncated_metrics"] = [m for m, n in counts.items() if n >= CFG["row_cap"]]

    with open(clean_dir / f"{date}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS)
        w.writeheader()
        w.writerows(all_rows)

    manifest["warnings"] = warnings
    manifest["complete"] = all((raw_dir / f"{c['id']}.json").exists() for c in CFG["calls"])
    manifest_path.write_text(json.dumps(manifest, indent=1))
    print(f"{date}: {len(all_rows)} rows, complete={manifest['complete']}, warnings={len(warnings)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
