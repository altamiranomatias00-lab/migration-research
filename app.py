#!/usr/bin/env python3
"""
Migration Pathway Explorer — Flask backend.
3D globe UI + Gemini AI-powered real-time search + SQLite persistence.
Three relational tables: Países → Maestrias → Becas.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from flask import Flask, request, jsonify, send_from_directory
from db import (
    init_db, seed_from_json, search_programs, search_programs_by_ids,
    get_program_detail, get_all_countries, get_conn, upsert_country,
    upsert_program, upsert_scholarship, link_program_scholarship, upsert_pathway,
)
from ai_search import ai_search, ANTHROPIC_API_KEY
from formulas import (
    legal_gap as calc_legal_gap,
    viability_cost_index,
    coverage_ratio,
    labor_pressure_index,
    compute_alerts,
    is_within_days,
)
from scraper import Country, Program, Scholarship
import time
import urllib.request
from collections import defaultdict

app = Flask(__name__, static_folder="static")

# ─── SECURITY ────────────────────────────────────────
# Rate limiting: max requests per IP per minute
_rate_limit = defaultdict(list)
RATE_LIMIT_MAX = 30  # requests
RATE_LIMIT_WINDOW = 60  # seconds

def _check_rate_limit(ip: str) -> bool:
    now = time.time()
    _rate_limit[ip] = [t for t in _rate_limit[ip] if now - t < RATE_LIMIT_WINDOW]
    if len(_rate_limit[ip]) >= RATE_LIMIT_MAX:
        return False
    _rate_limit[ip].append(now)
    return True

@app.before_request
def security_checks():
    if not _check_rate_limit(request.remote_addr):
        return jsonify({"error": "rate limit exceeded"}), 429

@app.after_request
def security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://unpkg.com https://cdn.jsdelivr.net; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "img-src 'self' data:; "
        "connect-src 'self'"
    )
    return response

LIVING_COSTS = {
    "DE": 1100, "NL": 1400, "CA": 1500, "AU": 1600, "US": 1800,
    "GB": 1700, "FR": 1300, "SE": 1400, "NO": 1600, "FI": 1200,
    "DK": 1500, "CH": 2200, "AT": 1200, "BE": 1200, "IE": 1500,
    "NZ": 1400, "JP": 1100, "KR": 1000, "SG": 1800, "HK": 1900,
    "ES": 1100, "IT": 1200, "PT": 1000, "CZ": 900, "PL": 800,
    "HU": 750, "GR": 850, "RO": 700, "IS": 1800, "LU": 1600,
    "MX": 700, "BR": 800, "AR": 600, "CL": 900, "CO": 600,
    "IN": 500, "CN": 900, "TW": 800, "MY": 600, "TH": 600,
    "IL": 1500, "AE": 1800, "SA": 1200, "ZA": 700, "EG": 500,
}

CITY_COORDS = {
    "Berlin": [52.52, 13.41], "Munich": [48.14, 11.58], "Aachen": [50.78, 6.08],
    "Karlsruhe": [49.01, 8.40], "Stuttgart": [48.78, 9.18], "Frankfurt": [50.11, 8.68],
    "Hamburg": [53.55, 9.99], "Heidelberg": [49.40, 8.69], "Dresden": [51.05, 13.74],
    "Delft": [52.01, 4.36], "Amsterdam": [52.37, 4.90], "Eindhoven": [51.44, 5.47],
    "Enschede": [52.22, 6.89], "Leiden": [52.16, 4.49], "Rotterdam": [51.92, 4.48],
    "Groningen": [53.22, 6.57], "Utrecht": [52.09, 5.12],
    "Vancouver": [49.28, -123.12], "Toronto": [43.65, -79.38], "Waterloo": [43.47, -80.52],
    "Montreal": [45.50, -73.57], "Ottawa": [45.42, -75.70], "Calgary": [51.05, -114.07],
    "Edmonton": [53.55, -113.49], "Quebec City": [46.81, -71.21],
    "Melbourne": [-37.81, 144.96], "Sydney": [-33.87, 151.21], "Lima": [-12.05, -77.04],
    "Brisbane": [-27.47, 153.03], "Perth": [-31.95, 115.86], "Adelaide": [-34.93, 138.60],
    "Auckland": [-36.85, 174.76], "Wellington": [-41.29, 174.78],
    "London": [51.51, -0.13], "Oxford": [51.75, -1.26], "Cambridge": [52.21, 0.12],
    "Edinburgh": [55.95, -3.19], "Manchester": [53.48, -2.24], "Bristol": [51.45, -2.59],
    "Glasgow": [55.86, -4.25], "Birmingham": [52.49, -1.90],
    "Paris": [48.86, 2.35], "Lyon": [45.76, 4.84], "Toulouse": [43.60, 1.44],
    "Zurich": [47.38, 8.54], "Geneva": [46.20, 6.14], "Lausanne": [46.52, 6.63],
    "Bern": [46.95, 7.45], "Basel": [47.56, 7.59],
    "Stockholm": [59.33, 18.07], "Lund": [55.70, 13.19], "Gothenburg": [57.71, 11.97],
    "Uppsala": [59.86, 17.64],
    "Helsinki": [60.17, 24.94], "Copenhagen": [55.68, 12.57],
    "Vienna": [48.21, 16.37], "Brussels": [50.85, 4.35], "Dublin": [53.35, -6.26],
    "Tokyo": [35.69, 139.69], "Kyoto": [35.01, 135.77], "Osaka": [34.69, 135.50],
    "Seoul": [37.57, 126.98], "Singapore": [1.35, 103.82],
    "Beijing": [39.91, 116.40], "Shanghai": [31.23, 121.47],
    "Hong Kong": [22.32, 114.17], "Taipei": [25.03, 121.57],
    "Barcelona": [41.39, 2.17], "Madrid": [40.42, -3.70], "Salamanca": [40.97, -5.66],
    "Milan": [45.46, 9.19], "Rome": [41.90, 12.50], "Bologna": [44.49, 11.34],
    "Oslo": [59.91, 10.75], "Lisbon": [38.72, -9.14], "Porto": [41.15, -8.61],
    "Prague": [50.08, 14.44], "Warsaw": [52.23, 21.01], "Budapest": [47.50, 19.04],
    "New York": [40.71, -74.01], "San Francisco": [37.77, -122.42], "Boston": [42.36, -71.06],
    "Los Angeles": [34.05, -118.24], "Chicago": [41.88, -87.63],
    "Pittsburgh": [40.44, -79.99], "Austin": [30.27, -97.74], "Seattle": [47.61, -122.33],
    "São Paulo": [-23.55, -46.63], "Buenos Aires": [-34.60, -58.38],
    "Mexico City": [19.43, -99.13], "Bogota": [4.71, -74.07], "Santiago": [-33.45, -70.67],
    "Mumbai": [19.08, 72.88], "New Delhi": [28.61, 77.21], "Bangalore": [12.97, 77.59],
    "Kuala Lumpur": [3.14, 101.69], "Bangkok": [13.76, 100.50],
    "Tel Aviv": [32.08, 34.78], "Dubai": [25.20, 55.27], "Riyadh": [24.71, 46.68],
    "Cape Town": [-33.93, 18.42], "Johannesburg": [-26.20, 28.04],
    "Cairo": [30.04, 31.24], "Nairobi": [-1.29, 36.82],
}

init_db()
seed_from_json()


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/countries")
def api_countries():
    return jsonify(get_all_countries())


@app.route("/api/coords")
def api_coords():
    return jsonify(CITY_COORDS)


@app.route("/api/config")
def api_config():
    return jsonify({"ai_available": bool(ANTHROPIC_API_KEY)})


@app.route("/api/geo")
def api_geo():
    """Detect user country from IP. Returns country code and name."""
    try:
        ip = request.headers.get("X-Forwarded-For", request.remote_addr)
        if ip and "," in ip:
            ip = ip.split(",")[0].strip()
        # Local/private IPs can't be geolocated
        if ip in ("127.0.0.1", "::1", "localhost") or ip.startswith("192.168.") or ip.startswith("10."):
            return jsonify({"country_code": "PE", "country_name": "Peru", "detected": False})
        req = urllib.request.Request(
            f"http://ip-api.com/json/{ip}?fields=status,countryCode,country",
            headers={"User-Agent": "MigrationExplorer/1.0"}
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read())
        if data.get("status") == "success":
            return jsonify({
                "country_code": data["countryCode"],
                "country_name": data["country"],
                "detected": True
            })
    except Exception:
        pass
    return jsonify({"country_code": "PE", "country_name": "Peru", "detected": False})


@app.route("/api/db-stats")
def api_db_stats():
    """Return table counts only (no sample data exposed)."""
    import os
    admin_key = request.args.get("key", "")
    expected = os.environ.get("ADMIN_KEY", "")
    if not expected or admin_key != expected:
        return jsonify({"error": "unauthorized"}), 403

    conn = get_conn()
    stats = {}
    for table in ["countries", "programs", "scholarships", "program_scholarships", "viability_pathways"]:
        row = conn.execute(f"SELECT COUNT(*) as cnt FROM {table}").fetchone()
        stats[table] = row["cnt"]
    conn.close()
    return jsonify(stats)


@app.route("/api/search")
def api_search():
    keyword = request.args.get("q", "").strip() or None
    degrees = request.args.getlist("degree") or None
    countries = request.args.getlist("country") or None
    fresh = request.args.get("fresh", "false") == "true"
    search_lang = request.args.get("lang", "en")

    if degrees:
        degrees = [d for d in degrees if d] or None
    if countries:
        countries = [c for c in countries if c] or None

    print(f"[SEARCH] q={keyword}, degrees={degrees}, countries={countries}, fresh={fresh}, api_key={'YES' if ANTHROPIC_API_KEY else 'NO'}", flush=True)

    # 1. Check cache first
    cached = search_programs(keyword=keyword, degree_levels=degrees, country_ids=countries)
    print(f"[SEARCH] cached results: {len(cached)}", flush=True)

    # 2. If no cached results or fresh requested, use AI
    if (not cached or fresh) and keyword and ANTHROPIC_API_KEY:
        ai_data = ai_search(keyword, degrees or ["masters"], countries, lang=search_lang)
        if ai_data:
            try:
                _ingest_ai_results(ai_data)
                print(f"[SEARCH] Ingest OK", flush=True)
            except Exception as e:
                print(f"[SEARCH] Ingest ERROR: {e}", flush=True)
                import traceback; traceback.print_exc()
            ai_program_ids = [p["program_id"] for p in ai_data.get("programs", [])]
            if ai_program_ids:
                cached = search_programs_by_ids(ai_program_ids, degree_levels=degrees, country_ids=countries)
            else:
                cached = search_programs(keyword=keyword, degree_levels=degrees, country_ids=countries)
            print(f"[SEARCH] after ingest: {len(cached)} results", flush=True)

    # Parse JSON strings and add coordinates
    for r in cached:
        for field in ("alerts", "scholarship_providers", "unverified_fields"):
            if isinstance(r.get(field), str):
                try:
                    r[field] = json.loads(r[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        city = r.get("city", "")
        r["coords"] = CITY_COORDS.get(city)

    return jsonify(cached)


@app.route("/api/program/<program_id>")
def api_program(program_id):
    detail = get_program_detail(program_id)
    if not detail:
        return jsonify({"error": "not found"}), 404
    for field in ("alerts", "scholarship_providers", "unverified_fields",
                  "country_source_urls", "candidate_type", "eligible_country_ids"):
        if isinstance(detail.get(field), str):
            try:
                detail[field] = json.loads(detail[field])
            except (json.JSONDecodeError, TypeError):
                pass
    for s in detail.get("scholarships", []):
        for field in ("candidate_type", "eligible_country_ids", "unverified_fields"):
            if isinstance(s.get(field), str):
                try:
                    s[field] = json.loads(s[field])
                except (json.JSONDecodeError, TypeError):
                    pass
    detail["coords"] = CITY_COORDS.get(detail.get("city", ""))
    return jsonify(detail)


def _ingest_ai_results(data: dict):
    """Store AI research results into SQLite and compute viability.
    Populates all three relational tables: countries, programs, scholarships."""
    conn = get_conn()

    # ─── 1. PAÍSES ───────────────────────────────────────
    country_map = {}
    for c in data.get("countries", []):
        upsert_country(conn, c)
        country_map[c["country_id"]] = c

    # ─── 2. BECAS (before programs for FK links) ────────
    schol_map = {}
    for s in data.get("scholarships", []):
        upsert_scholarship(conn, s)
        schol_map[s["scholarship_id"]] = s

    # ─── 3. MAESTRIAS ────────────────────────────────────
    for p in data.get("programs", []):
        upsert_program(conn, p)

    # ─── 4. RELATIONS: Link scholarships ↔ programs ─────
    for s in data.get("scholarships", []):
        for pid in s.get("applicable_program_ids", []):
            link_program_scholarship(conn, pid, s["scholarship_id"])

    conn.commit()

    # ─── 5. COMPUTE VIABILITY PATHWAYS ──────────────────
    for p in data.get("programs", []):
        cid = p.get("country_id")
        c_data = country_map.get(cid)
        if not c_data:
            continue

        country_obj = Country(**{k: c_data.get(k) for k in
            ["country_id", "country_name", "months_to_pr", "study_visa_months",
             "post_study_extension_months", "solvency_buffer_usd",
             "work_permit_allowed", "max_hours_per_week", "embassy_in_peru"]
        })
        program_obj = Program(**{k: p.get(k) for k in
            ["program_id", "program_name", "university", "city", "country_id"]},
            full_tuition_usd=p.get("full_tuition_usd"),
            duration_months=p.get("duration_months"),
        )

        # Find best scholarship for this program
        applicable = [schol_map[s["scholarship_id"]] for s in data.get("scholarships", [])
                      if p["program_id"] in s.get("applicable_program_ids", [])]
        best_s_data = max(applicable, key=lambda s: s.get("coverage_pct") or 0, default=None)
        best_schol = None
        if best_s_data:
            best_schol = Scholarship(**{k: best_s_data.get(k) for k in
                ["scholarship_id", "scholarship_name", "provider_organization"]},
                coverage_pct=best_s_data.get("coverage_pct"),
                monthly_stipend_usd=best_s_data.get("monthly_stipend_usd"),
            )

        living = LIVING_COSTS.get(cid, 1300)
        gap = calc_legal_gap(country_obj, program_obj)
        vci = viability_cost_index(country_obj, program_obj, best_schol, living)
        cov = coverage_ratio(vci, best_schol) if vci else None
        lpi = labor_pressure_index(country_obj)
        alerts = compute_alerts(country_obj, program_obj, best_schol, lpi, cov)

        pathway_id = f"{cid.lower()}-{p['program_id']}"
        if best_s_data:
            pathway_id += f"-{best_s_data['scholarship_id']}"

        pw = {
            "pathway_id": pathway_id,
            "country_id": cid,
            "program_id": p["program_id"],
            "best_scholarship_id": best_s_data["scholarship_id"] if best_s_data else None,
            "program_name": f"{p['program_name']} — {p['university']}",
            "legal_gap_months": gap.get("legal_gap_months"),
            "viability_status": gap.get("status"),
            "vci_usd": vci,
            "employment_months_required": gap.get("employment_months_required", 0),
            "coverage_ratio_pct": cov,
            "labor_pressure_index": lpi,
            "alerts": alerts,
            "recommended_action": "",
            "embassy_warning": "NO_EMBASSY_IN_PERU" in alerts,
            "deadline_urgent": "DEADLINE_URGENT" in alerts,
        }
        upsert_pathway(conn, pw)

    conn.commit()

    # Recompute VCI ranks
    rows = conn.execute(
        "SELECT pathway_id, vci_usd FROM viability_pathways ORDER BY "
        "CASE WHEN vci_usd IS NULL THEN 1 ELSE 0 END, vci_usd ASC"
    ).fetchall()
    for i, row in enumerate(rows, 1):
        conn.execute("UPDATE viability_pathways SET vci_rank = ? WHERE pathway_id = ?",
                      (i, row["pathway_id"]))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5080, debug=False)
