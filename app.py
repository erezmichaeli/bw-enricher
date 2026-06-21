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
    v = conf.get("value", "")
    if t == "csv":
        cell = row.get(v, "")
        return None if (cell == "" or cell is None) else str(cell)
    return str(v) if v != "" else None

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

def process_row(row, steps, base_url, token):
    row = dict(row)
    row.setdefault("enricher_error", "")
    hdrs = bw_headers(token)

    for idx, step in enumerate(steps):
        label = step.get("name", f"Step {idx+1}")
        try:
            url_tpl = step["url_template"]
            skip = False

            # path params
            for p, conf in step.get("path_map", {}).items():
                val = resolve(conf, row)
                if val is None:
                    row["enricher_error"] += f"[{label}: missing path {p}] "
                    skip = True; break
                url_tpl = url_tpl.replace(f"{{{p}}}", val)
            if skip:
                continue

            full_url = base_url.rstrip("/") + "/" + url_tpl.lstrip("/")
            params = {}

            # query params
            for q, conf in step.get("query_map", {}).items():
                val = resolve(conf, row)
                req = step.get("required_params", {}).get(q, False)
                if val is None and req:
                    row["enricher_error"] += f"[{label}: missing query {q}] "
                    skip = True; break
                if val is not None:
                    params[q] = val
            if skip:
                continue

            resp = fetch_with_retry(full_url, hdrs, params)
            if resp and resp.status_code == 200:
                try:
                    data = resp.json()
                except Exception:
                    data = {}

                for out in step.get("output_map", []):
                    j_key = (out.get("json_field") or "").strip()
                    c_col = (out.get("csv_column") or "").strip()
                    if not j_key or not c_col:
                        continue

                    # Array root → fan out into year_Q# columns
                    if isinstance(data, list):
                        for item in data:
                            if not isinstance(item, dict):
                                continue
                            yr  = item.get("year")
                            qtr = item.get("quarter")
                            if yr is not None and qtr is not None:
                                suffix = f"{int(yr)}_Q{int(qtr)}"
                            elif yr is not None:
                                suffix = str(int(yr))
                            else:
                                suffix = str(data.index(item))
                            row[f"{c_col}_{suffix}"] = extract(item, j_key)
                    else:
                        row[c_col] = extract(data, j_key)
            else:
                code = resp.status_code if resp else "Timeout"
                msg = ""
                if resp:
                    try:
                        j = resp.json()
                        msg = j.get("message") or j.get("detail") or ""
                    except Exception:
                        msg = resp.text[:120]
                row["enricher_error"] += f"[{label}: HTTP {code} {msg}] "
        except Exception as e:
            row["enricher_error"] += f"[{label}: {e}] "
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

                evt = {"type":"progress","done":done,"total":total,
                       "errors":errors,"eta":round(eta,1)}
                yield f"data: {json.dumps(evt)}\n\n"

        # Build output CSV
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=out_cols, extrasaction="ignore")
        writer.writeheader()
        for r in result_rows:
            if r:
                writer.writerow(r)
        csv_b64 = base64.b64encode(buf.getvalue().encode()).decode()
        out_filename = f"{filename}_enriched.csv"

        yield f"data: {json.dumps({'type':'done','csv_b64':csv_b64,'filename':out_filename,'errors':errors})}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
