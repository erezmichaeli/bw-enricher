import os, json, time, csv, io, base64, hashlib, requests
from flask import Flask, render_template, request, Response, jsonify
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

ENVIRONMENTS = {
    "production": "https://rest.bridgewise.com",
    "stage":      "https://rest.stage-bridgewise.com",
    "dev":        "https://rest.dev-bridgewise.com",
}

# ── helpers ──────────────────────────────────────────────────────────────────

def bw_headers(token):
    return {"Authorization": f"Bearer {token.strip()}", "Accept": "application/json"}

def fetch_with_retry(url, headers, params, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=20)
            if r.status_code == 429:
                time.sleep(2 ** attempt); continue
            if r.status_code >= 500:
                time.sleep(1); continue
            return r
        except requests.RequestException:
            time.sleep(1)
    return None

def resolve(conf, row):
    if not conf:
        return None
    t = conf.get("type")
    if t == "skip":
        return None
    v = conf.get("value", "")
    if t == "csv":
        cell = row.get(v, "")
        return None if (cell == "" or cell is None) else str(cell)
    return str(v) if v != "" else None

def parse_error_body(resp):
    """Extract the richest possible error description from an API error response."""
    if resp is None:
        return "Request timed out after retries"
    try:
        body = resp.json()
    except Exception:
        # Not JSON — return raw text, cleaned up
        raw = resp.text.strip()
        return raw[:500] if raw else f"HTTP {resp.status_code} (empty body)"

    if not isinstance(body, dict):
        return str(body)[:500]

    parts = []

    # Top-level message / detail
    for key in ("message", "detail", "error", "error_description", "title"):
        val = body.get(key)
        if val and isinstance(val, str):
            parts.append(val)
            break  # one top-level message is enough

    # Validation errors — common in BW: {"errors": [{"field": "x", "message": "y"}]}
    for key in ("errors", "validation_errors", "fields"):
        errs = body.get(key)
        if isinstance(errs, list):
            for e in errs[:5]:  # cap at 5
                if isinstance(e, dict):
                    field = e.get("field") or e.get("loc") or e.get("param") or ""
                    msg   = e.get("message") or e.get("msg") or e.get("description") or str(e)
                    parts.append(f"  • {field}: {msg}" if field else f"  • {msg}")
                else:
                    parts.append(f"  • {e}")
        elif isinstance(errs, dict):
            for field, msg in list(errs.items())[:5]:
                parts.append(f"  • {field}: {msg}")

    # Any other top-level keys that look informative
    for key in ("reason", "code", "status", "type"):
        val = body.get(key)
        if val and str(val) not in " ".join(parts):
            parts.append(f"[{key}: {val}]")

    if not parts:
        # Fallback: dump the whole body compactly, capped at 400 chars
        dumped = json.dumps(body)
        parts.append(dumped[:400] + ("…" if len(dumped) > 400 else ""))

    return "\n".join(parts)


def extract(payload, path):
    """Walk dot-separated path through payload."""
    val = payload
    for k in path.split("."):
        k = k.strip()
        if isinstance(val, dict):
            val = val.get(k, "")
        elif isinstance(val, list) and k.isdigit():
            i = int(k)
            val = val[i] if i < len(val) else ""
        else:
            return ""
    return json.dumps(val) if isinstance(val, (dict, list)) else val

# Priority-ordered list of fields to use as array item suffix.
# First field found in an item wins. Ordered from most-specific to most-generic.
_SUFFIX_FIELD_PRIORITY = [
    # BW-specific discriminators
    "paragraph_type",       # /paragraphs endpoints
    "section_type",         # section-based arrays
    "period_type",          # period arrays (quarterly/annually)
    "change_type",          # policy change arrays
    "policy_type",          # policy arrays
    "event_type",           # event arrays
    "metric_type",          # metric arrays
    "analysis_type",        # analysis arrays (only if unique per item)
    # ID-based discriminators (unique but long)
    "event_id",             # event lists
    "trading_item_id",      # trading item arrays
    "security_id",          # security arrays
    "company_id",           # company lists
    "fund_id",              # fund lists
    # Date-based discriminators
    "effective_date",       # policy/change effective date
    "filing_date",          # filing date
    "date",                 # generic date
    # Name-based (less reliable — may not be unique)
    "analyst_name",         # price target analyst
    "exchange_name",        # exchange arrays
    "category",             # generic category
    "scoring_method",       # esg scoring method
    # Final fallbacks
    "type",
    "name",
    "code",
    "id",
]

def _make_array_suffix(item, index, full_array):
    """
    Pick the best unique suffix for a fan-out column name.
    Priority:
      1. paragraph_type / section_type / type discriminator fields
      2. year + quarter  (e.g. 2024_Q3)
      3. year only
      4. numeric index fallback
    """
    # Try discriminator fields first (paragraph_type, section_type, etc.)
    for field in _SUFFIX_FIELD_PRIORITY:
        val = item.get(field)
        if val and isinstance(val, str) and val.strip():
            return val.strip().replace(" ", "_")

    # Fall back to year+quarter
    yr  = item.get("year")
    qtr = item.get("quarter")
    if yr is not None and qtr is not None:
        return f"{int(yr)}_Q{int(qtr)}"
    if yr is not None:
        return str(int(yr))

    # Final fallback: numeric index
    return str(index)


def is_identifier_search(url_template):
    """identifier-search is special: returns an array but we want the first
    matching result's scalar fields, not fan-out columns."""
    return "identifier-search" in url_template

def extract_first_match(data, j_key):
    """For identifier-search arrays: walk items and return the first
    non-empty value found for j_key. Prefers primary_flag=True items."""
    if not isinstance(data, list):
        return extract(data, j_key)
    # prefer primary item
    primary = next((i for i in data if isinstance(i, dict) and i.get("primary_flag")), None)
    candidates = ([primary] if primary else []) + [i for i in data if i != primary and isinstance(i, dict)]
    for item in candidates:
        val = extract(item, j_key)
        # Explicitly check for missing — False, 0, empty list are valid values
        if val != "" and val is not None:
            return val
    return ""

def process_row(row, steps, base_url, token):
    row = dict(row)
    row.setdefault("enricher_error", "")
    row.setdefault("_debug_log", [])   # per-row debug trace
    hdrs = bw_headers(token)

    for idx, step in enumerate(steps):
        label = step.get("name", f"Step {idx+1}")
        is_id_search = is_identifier_search(step.get("url_template", ""))
        try:
            url_tpl = step["url_template"]
            skip = False

            # ── path params ──────────────────────────────────────────────
            for p, conf in step.get("path_map", {}).items():
                val = resolve(conf, row)
                if val is None:
                    # Give a clear diagnostic: what column was expected, what it resolved to
                    src_col = conf.get("value", "?") if conf else "?"
                    actual  = row.get(src_col, "<column missing>") if conf and conf.get("type") == "csv" else None
                    detail  = f"column '{src_col}' = '{actual}'" if actual is not None else f"static value was empty"
                    row["enricher_error"] += f"[{label}: missing path '{p}' ({detail})] "
                    row["_debug_log"].append(f"Step {idx+1} '{label}': SKIP — path param '{p}' empty. {detail}")
                    skip = True; break
                url_tpl = url_tpl.replace(f"{{{p}}}", val)
            if skip:
                continue

            full_url = base_url.rstrip("/") + "/" + url_tpl.lstrip("/")
            params = {}

            # ── query params ─────────────────────────────────────────────
            for q, conf in step.get("query_map", {}).items():
                val = resolve(conf, row)
                req = step.get("required_params", {}).get(q, False)
                if val is None and req:
                    src_col = conf.get("value", "?") if conf else "?"
                    actual  = row.get(src_col, "<column missing>") if conf and conf.get("type") == "csv" else None
                    detail  = f"column '{src_col}' = '{actual}'" if actual is not None else "empty"
                    row["enricher_error"] += f"[{label}: missing required query '{q}' ({detail})] "
                    row["_debug_log"].append(f"Step {idx+1} '{label}': SKIP — query param '{q}' empty. {detail}")
                    skip = True; break
                if val is not None:
                    params[q] = val
            if skip:
                continue

            param_str_dbg = "&".join(f"{k}={v}" for k, v in params.items())
            row["_debug_log"].append(
                f"Step {idx+1} '{label}': GET {full_url}"
                + (f"?{param_str_dbg}" if param_str_dbg else "")
            )

            resp = fetch_with_retry(full_url, hdrs, params)
            if resp and resp.status_code == 200:
                try:
                    data = resp.json()
                except Exception:
                    data = {}

                # ── identifier-search: first-match extraction ─────────────
                if is_id_search:
                    if not data or (isinstance(data, list) and len(data) == 0):
                        row["enricher_error"] += f"[{label}: identifier not found in BW] "
                        row["_debug_log"].append(f"  → identifier-search returned empty — identifier not in BW")
                    else:
                        for out in step.get("output_map", []):
                            j_key = (out.get("json_field") or "").strip()
                            c_col = (out.get("csv_column") or "").strip()
                            if not j_key or not c_col:
                                continue
                            val = extract_first_match(data, j_key)
                            row[c_col] = val
                            row["_debug_log"].append(f"  → identifier-search: '{j_key}' = '{val}' → col '{c_col}'")
                            # Only flag as error if truly missing (not just falsy)
                            if val == "" or val is None:
                                row["enricher_error"] += f"[{label}: field '{j_key}' not found in identifier-search result — check field name (fund_id vs company_id)] "

                # ── array: fan out or take first based on array_mode ──────
                # Also handles paginated screener responses: {data: [...], total_count: N}
                elif (
                    (isinstance(data, list) and data and isinstance(data[0], dict)) or
                    (isinstance(data, dict) and isinstance(data.get('data'), list) and data['data'])
                ):
                    # Unwrap screener-style {data: [...]} envelope
                    if isinstance(data, dict) and isinstance(data.get('data'), list):
                        data = data['data']
                    array_mode = step.get("array_mode", "columns")
                    # "first"   — take only the first item (single value per column)
                    # "columns" — fan out: one column per item, suffixed by discriminator
                    # "concat"  — join all values with " | " into one column
                    for out in step.get("output_map", []):
                        j_key = (out.get("json_field") or "").strip()
                        c_col = (out.get("csv_column") or "").strip()
                        if not j_key or not c_col:
                            continue

                        if array_mode == "first":
                            row[c_col] = extract(data[0], j_key)

                        elif array_mode == "concat":
                            sep = step.get("array_concat_sep", " | ")
                            vals = [str(extract(item, j_key)) for item in data
                                    if isinstance(item, dict)]
                            row[c_col] = sep.join(v for v in vals if v)

                        else:  # "columns" — default fan-out
                            seen_suffixes = {}
                            for i, item in enumerate(data):
                                if not isinstance(item, dict):
                                    continue
                                suffix = _make_array_suffix(item, i, data)
                                if suffix in seen_suffixes:
                                    suffix = f"{suffix}_{i}"
                                seen_suffixes[suffix] = True
                                val = extract(item, j_key)
                                # Skip columns where value == suffix (e.g. paragraph_type
                                # fanning out: col "paragraph_type_fund_highlights_rec"
                                # with value "fund_highlights_rec" — pure noise, type is
                                # already in the column name)
                                if str(val) == suffix:
                                    continue
                                row[f"{c_col}_{suffix}"] = val

                # ── single object ─────────────────────────────────────────
                else:
                    payload = data[0] if isinstance(data, list) and data else data
                    for out in step.get("output_map", []):
                        j_key = (out.get("json_field") or "").strip()
                        c_col = (out.get("csv_column") or "").strip()
                        if not j_key or not c_col:
                            continue
                        val = extract(payload, j_key)
                        row[c_col] = val
                        row["_debug_log"].append(f"  → '{j_key}' = '{str(val)[:60]}' → col '{c_col}'")

            else:
                code = resp.status_code if resp else "Timeout"
                error_body = parse_error_body(resp)
                # Build full reproducible curl for debug log
                param_str = "&".join(f"{k}={v}" for k, v in params.items())
                curl = f"GET {full_url}" + (f"?{param_str}" if param_str else "")
                row["enricher_error"] += f"[{label}: HTTP {code} — {error_body}] "
                row["_debug_log"].append(
                    f"Step {idx+1} '{label}': HTTP {code} | URL: {curl} | Error: {error_body}"
                )

        except Exception as e:
            row["enricher_error"] += f"[{label}: {e}] "
            row["_debug_log"].append(f"Step {idx+1} '{label}': EXCEPTION {e}")

    # expose debug log as a readable string column (only if there were errors)
    if row.get("enricher_error"):
        row["_debug_log"] = " | ".join(row["_debug_log"])
    else:
        del row["_debug_log"]   # clean column — don't add noise to successful rows
    return row

# ── routes ───────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    return render_template("index.html")

@app.get("/api/health")
def health():
    return jsonify({"ok": True})

@app.get("/api/swagger")
def swagger():
    """Proxy swagger fetch — avoids CORS in browser."""
    url = request.args.get("url", "")
    if not url:
        return jsonify({"error": "missing url"}), 400
    try:
        r = requests.get(url, headers={"Accept": "application/json",
                                        "User-Agent": "BW-Enricher/1.0"},
                         timeout=20)
        return Response(r.content, status=r.status_code,
                        content_type="application/json")
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.post("/api/run")
def run():
    """
    Accepts JSON: {token, env, pipeline, csv_text, concurrency, filename}
    Streams SSE events:
      {"type":"progress","done":N,"total":N,"errors":N,"eta":F}
      {"type":"done","csv_b64":"...","filename":"...","errors":N}
      {"type":"error","message":"..."}
    """
    body = request.get_json(force=True)
    token       = body.get("token", "").strip()
    env         = body.get("env", "production").lower()
    pipeline    = body.get("pipeline", [])
    csv_text    = body.get("csv_text", "")
    concurrency = max(1, min(int(body.get("concurrency", 5)), 20))
    filename    = body.get("filename", "output")

    if not token:
        return jsonify({"error": "missing token"}), 400
    base_url = ENVIRONMENTS.get(env, ENVIRONMENTS["production"])

    def generate():
        # parse CSV
        try:
            reader = csv.DictReader(io.StringIO(csv_text))
            rows = list(reader)
            fieldnames = reader.fieldnames or []
        except Exception as e:
            yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"
            return

        if not rows:
            yield f"data: {json.dumps({'type':'error','message':'CSV is empty'})}\n\n"
            return

        total   = len(rows)
        done    = 0
        errors  = 0
        t0      = time.time()
        out_cols = list(fieldnames)
        for step in pipeline:
            for o in step.get("output_map", []):
                col = (o.get("csv_column") or "").strip()
                if col and col not in out_cols:
                    out_cols.append(col)
        if "enricher_error" not in out_cols:
            out_cols.append("enricher_error")

        result_rows = [None] * total

        with ThreadPoolExecutor(max_workers=concurrency) as exe:
            future_to_idx = {
                exe.submit(process_row, rows[i], pipeline, base_url, token): i
                for i in range(total)
            }
            for fut in as_completed(future_to_idx):
                i = future_to_idx[fut]
                try:
                    result_rows[i] = fut.result()
                    if result_rows[i].get("enricher_error"):
                        errors += 1
                except Exception as e:
                    result_rows[i] = dict(rows[i])
                    result_rows[i]["enricher_error"] = f"CRITICAL: {e}"
                    errors += 1

                done += 1
                elapsed = max(time.time() - t0, 0.001)
                speed   = done / elapsed
                eta     = (total - done) / speed if speed > 0 else 0

                # Dynamically add any new columns from fan-out
                for col in result_rows[i].keys():
                    if col not in out_cols:
                        out_cols.append(col)

                row_err = (result_rows[i] or {}).get("enricher_error","").strip()
                evt = {"type":"progress","done":done,"total":total,
                       "errors":errors,"eta":round(eta,1),
                       "row_error": row_err if row_err else None}
                yield f"data: {json.dumps(evt)}\n\n"

        # Build output CSV
        buf = io.StringIO()
        # exclude internal debug column from output CSV
        csv_cols = [c for c in out_cols if c != "_debug_log"]
        writer = csv.DictWriter(buf, fieldnames=csv_cols, extrasaction="ignore")
        writer.writeheader()
        for r in result_rows:
            if r:
                writer.writerow(r)
        csv_b64 = base64.b64encode(buf.getvalue().encode()).decode()
        from datetime import datetime
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_filename = f"{filename}_enriched_{ts}.csv"

        # collect per-row error details + debug log for the frontend summary panel
        error_details = []
        for i, r in enumerate(result_rows):
            if r and r.get("enricher_error","").strip():
                error_details.append({
                    "row": i+1,
                    "error": r["enricher_error"].strip(),
                    "debug": r.get("_debug_log", ""),
                })

        yield f"data: {json.dumps({'type':'done','csv_b64':csv_b64,'filename':out_filename,'errors':errors,'error_details':error_details})}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no"})

def bw_get(path, token, params=None):
    """Generic authenticated GET to BW production API. Returns (data, status_code, error)."""
    url = f"https://rest.bridgewise.com{path}"
    hdrs = bw_headers(token)
    try:
        r = requests.get(url, headers=hdrs, params=params or {}, timeout=20)
        if r.status_code == 200:
            try:
                return r.json(), 200, None
            except Exception:
                return r.text, 200, None
        return None, r.status_code, parse_error_body(r)
    except Exception as e:
        return None, 502, str(e)


def cors_json(data, status=200):
    """Return a JSON response with CORS headers so the suite can call the Railway proxy."""
    resp = jsonify(data)
    resp.status_code = status
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "X-BW-Token, Content-Type"
    return resp


@app.before_request
def handle_options():
    """Allow CORS preflight for all /api/* routes."""
    if request.method == "OPTIONS":
        resp = Response("", status=204)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "X-BW-Token, Content-Type, Authorization"
        return resp


@app.get("/api/tenants")
def get_tenants():
    """Return all tenants (name + id) for the dropdown."""
    token = request.headers.get("X-BW-Token", "").strip()
    if not token:
        return cors_json({"error": "Missing X-BW-Token header"}, 401)
    data, code, err = bw_get("/tenants", token, {"size": 200, "page": 1, "show_all": "true"})
    if err:
        return cors_json({"error": err}, code)
    # BW returns either a plain list or {"data": [...], "total": N}
    if isinstance(data, dict):
        items = data.get("data") or data.get("tenants") or data.get("items") or []
    elif isinstance(data, list):
        items = data
    else:
        items = []
    tenants = sorted(
        [{"id": t["id"], "name": t.get("name") or t.get("display_name") or t["id"]}
         for t in items if isinstance(t, dict) and t.get("id")],
        key=lambda x: x["name"].lower()
    )
    return cors_json(tenants)


@app.get("/api/tenant-policy/<tenant_id>")
def get_tenant_policy(tenant_id):
    """Return investment-policies-config for a given tenant."""
    token = request.headers.get("X-BW-Token", "").strip()
    if not token:
        return cors_json({"error": "Missing X-BW-Token header"}, 401)
    data, code, err = bw_get(f"/tenants/{tenant_id}/investment-policies-config", token)
    if err:
        return cors_json({"error": err}, code)
    return cors_json(data)


@app.get("/api/filters")
def get_filters():
    """Proxy /instruments/filters — resolves country/exchange/currency IDs to names."""
    token = request.headers.get("X-BW-Token", "").strip()
    filter_type = request.args.get("type", "countries")
    language = request.args.get("language", "en-US")
    url = "https://rest.bridgewise.com/instruments/filters"
    hdrs = {"Accept": "application/json"}
    if token:
        hdrs["Authorization"] = f"Bearer {token}"
    try:
        r = requests.get(url, headers=hdrs,
                         params={"type": filter_type, "language": language}, timeout=20)
        resp = Response(r.content, status=r.status_code, content_type="application/json")
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    except Exception as e:
        return cors_json({"error": str(e)}, 502)


@app.get("/api/funds/<int:fund_id>")
def get_fund(fund_id):
    """Proxy /instruments/funds/{id} — resolves index/fund ID to a name."""
    token = request.headers.get("X-BW-Token", "").strip()
    language = request.args.get("language", "en-US")
    if not token:
        return cors_json({"error": "Missing X-BW-Token header"}, 401)
    data, code, err = bw_get(f"/instruments/funds/{fund_id}", token, {"language": language})
    if err:
        return cors_json({"error": err}, code)
    return cors_json(data)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
