"""
SQLite database layer for migration research.
Three relational tables matching Notion schema: Países → Maestrias → Becas.
"""
import sqlite3
import json
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "migration.db"


def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()

    # ─── PAÍSES ──────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS countries (
        country_id TEXT PRIMARY KEY,
        country_name TEXT NOT NULL,
        region TEXT,
        months_to_pr INTEGER,
        study_visa_months INTEGER,
        post_study_extension_months INTEGER,
        solvency_buffer_usd REAL,
        work_permit_allowed INTEGER,
        max_hours_per_week INTEGER,
        embassy_in_peru INTEGER,
        link_pr TEXT DEFAULT '',
        link_study_visa TEXT DEFAULT '',
        link_visa_extension TEXT DEFAULT '',
        link_work_permit TEXT DEFAULT '',
        source_urls TEXT DEFAULT '{}',
        unverified_fields TEXT DEFAULT '[]',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # ─── MAESTRIAS ───────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS programs (
        program_id TEXT PRIMARY KEY,
        program_name TEXT NOT NULL,
        university TEXT NOT NULL,
        city TEXT NOT NULL,
        country_id TEXT NOT NULL REFERENCES countries(country_id),
        faculty_or_department TEXT,
        degree_level TEXT DEFAULT 'masters',
        duration_months INTEGER,
        language_of_instruction TEXT DEFAULT 'English',
        full_tuition_usd REAL,
        tuition_annual_usd REAL,
        program_url TEXT DEFAULT '',
        coverage_pct REAL,
        stipend_monthly_usd REAL,
        university_scholarship TEXT DEFAULT '',
        max_coverage_pct REAL,
        max_stipend_usd REAL,
        scholarship_providers TEXT DEFAULT '[]',
        unverified_fields TEXT DEFAULT '[]',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # ─── BECAS ───────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS scholarships (
        scholarship_id TEXT PRIMARY KEY,
        scholarship_name TEXT NOT NULL,
        provider_organization TEXT NOT NULL,
        candidate_type TEXT DEFAULT '[]',
        coverage_pct REAL,
        monthly_stipend_usd REAL,
        annual_stipend_usd REAL,
        covers_mobility_expenses INTEGER,
        covers_medical_insurance INTEGER,
        application_deadline TEXT,
        application_status TEXT DEFAULT 'No Iniciada',
        scholarship_url TEXT DEFAULT '',
        eligible_country_ids TEXT DEFAULT '[]',
        peru_eligible INTEGER,
        unverified_fields TEXT DEFAULT '[]',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # ─── RELATIONS ───────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS program_scholarships (
        program_id TEXT NOT NULL REFERENCES programs(program_id),
        scholarship_id TEXT NOT NULL REFERENCES scholarships(scholarship_id),
        PRIMARY KEY (program_id, scholarship_id)
    )""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS viability_pathways (
        pathway_id TEXT PRIMARY KEY,
        country_id TEXT NOT NULL REFERENCES countries(country_id),
        program_id TEXT NOT NULL REFERENCES programs(program_id),
        scholarship_id TEXT REFERENCES scholarships(scholarship_id),
        legal_gap_months INTEGER,
        viability_status TEXT,
        vci_usd REAL,
        vci_rank INTEGER,
        employment_months_required INTEGER,
        coverage_ratio_pct REAL,
        labor_pressure_index REAL,
        alerts TEXT DEFAULT '[]',
        recommended_action TEXT DEFAULT '',
        embassy_warning INTEGER DEFAULT 0,
        deadline_urgent INTEGER DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # Full-text search index on programs (standalone, no content sync)
    c.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS programs_fts USING fts5(
        program_id, program_name, university, city, faculty_or_department,
        degree_level, language_of_instruction
    )""")

    # Run migrations for new columns on existing tables
    _migrate(conn)

    conn.commit()
    conn.close()


def _migrate(conn):
    """Add new columns to existing tables if missing."""
    # Countries new columns
    _add_column(conn, "countries", "region", "TEXT")
    _add_column(conn, "countries", "link_pr", "TEXT DEFAULT ''")
    _add_column(conn, "countries", "link_study_visa", "TEXT DEFAULT ''")
    _add_column(conn, "countries", "link_visa_extension", "TEXT DEFAULT ''")
    _add_column(conn, "countries", "link_work_permit", "TEXT DEFAULT ''")
    # Programs new columns
    _add_column(conn, "programs", "coverage_pct", "REAL")
    _add_column(conn, "programs", "stipend_monthly_usd", "REAL")
    _add_column(conn, "programs", "university_scholarship", "TEXT DEFAULT ''")
    # Scholarships new columns
    _add_column(conn, "scholarships", "application_status", "TEXT DEFAULT 'No Iniciada'")
    _add_column(conn, "scholarships", "scholarship_url", "TEXT DEFAULT ''")


def _add_column(conn, table, column, coltype):
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
    except sqlite3.OperationalError:
        pass  # Column already exists


def upsert_country(conn, c):
    conn.execute("""
    INSERT INTO countries (country_id, country_name, region, months_to_pr, study_visa_months,
        post_study_extension_months, solvency_buffer_usd, work_permit_allowed,
        max_hours_per_week, embassy_in_peru, link_pr, link_study_visa,
        link_visa_extension, link_work_permit, source_urls, unverified_fields)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(country_id) DO UPDATE SET
        country_name=excluded.country_name, region=excluded.region,
        months_to_pr=excluded.months_to_pr,
        study_visa_months=excluded.study_visa_months,
        post_study_extension_months=excluded.post_study_extension_months,
        solvency_buffer_usd=excluded.solvency_buffer_usd,
        work_permit_allowed=excluded.work_permit_allowed,
        max_hours_per_week=excluded.max_hours_per_week,
        embassy_in_peru=excluded.embassy_in_peru,
        link_pr=excluded.link_pr,
        link_study_visa=excluded.link_study_visa,
        link_visa_extension=excluded.link_visa_extension,
        link_work_permit=excluded.link_work_permit,
        source_urls=excluded.source_urls,
        unverified_fields=excluded.unverified_fields,
        updated_at=CURRENT_TIMESTAMP
    """, (
        c["country_id"], c["country_name"], c.get("region"),
        c.get("months_to_pr"), c.get("study_visa_months"),
        c.get("post_study_extension_months"), c.get("solvency_buffer_usd"),
        c.get("work_permit_allowed"), c.get("max_hours_per_week"),
        c.get("embassy_in_peru"),
        c.get("link_pr", ""), c.get("link_study_visa", ""),
        c.get("link_visa_extension", ""), c.get("link_work_permit", ""),
        json.dumps(c.get("source_urls", {})),
        json.dumps(c.get("unverified_fields", []))
    ))


def upsert_program(conn, p):
    annual = None
    if p.get("full_tuition_usd") and p.get("duration_months"):
        years = p["duration_months"] / 12
        annual = round(p["full_tuition_usd"] / years, 2) if years > 0 else None

    conn.execute("""
    INSERT INTO programs (program_id, program_name, university, city, country_id,
        faculty_or_department, degree_level, duration_months, language_of_instruction,
        full_tuition_usd, tuition_annual_usd, program_url, coverage_pct,
        stipend_monthly_usd, university_scholarship,
        max_coverage_pct, max_stipend_usd, scholarship_providers, unverified_fields)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(program_id) DO UPDATE SET
        program_name=excluded.program_name, university=excluded.university,
        city=excluded.city, country_id=excluded.country_id,
        faculty_or_department=excluded.faculty_or_department,
        degree_level=excluded.degree_level, duration_months=excluded.duration_months,
        language_of_instruction=excluded.language_of_instruction,
        full_tuition_usd=excluded.full_tuition_usd,
        tuition_annual_usd=excluded.tuition_annual_usd,
        program_url=excluded.program_url,
        coverage_pct=excluded.coverage_pct,
        stipend_monthly_usd=excluded.stipend_monthly_usd,
        university_scholarship=excluded.university_scholarship,
        max_coverage_pct=excluded.max_coverage_pct,
        max_stipend_usd=excluded.max_stipend_usd,
        scholarship_providers=excluded.scholarship_providers,
        unverified_fields=excluded.unverified_fields,
        updated_at=CURRENT_TIMESTAMP
    """, (
        p["program_id"], p["program_name"], p["university"], p["city"],
        p["country_id"], p.get("faculty_or_department"),
        p.get("degree_level", "masters"), p.get("duration_months"),
        p.get("language_of_instruction", "English"),
        p.get("full_tuition_usd"), annual,
        p.get("program_url", ""),
        p.get("coverage_pct"), p.get("stipend_monthly_usd"),
        p.get("university_scholarship", ""),
        p.get("max_coverage_pct"), p.get("max_stipend_usd"),
        json.dumps(p.get("scholarship_providers", [])),
        json.dumps(p.get("unverified_fields", []))
    ))

    # Update FTS
    conn.execute("DELETE FROM programs_fts WHERE program_id = ?", (p["program_id"],))
    conn.execute("""
    INSERT INTO programs_fts (program_id, program_name, university, city,
        faculty_or_department, degree_level, language_of_instruction)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        p["program_id"], p["program_name"], p["university"], p["city"],
        p.get("faculty_or_department", ""), p.get("degree_level", "masters"),
        p.get("language_of_instruction", "English")
    ))


def upsert_scholarship(conn, s):
    annual_stipend = None
    if s.get("monthly_stipend_usd"):
        annual_stipend = round(s["monthly_stipend_usd"] * 12, 2)

    conn.execute("""
    INSERT INTO scholarships (scholarship_id, scholarship_name, provider_organization,
        candidate_type, coverage_pct, monthly_stipend_usd, annual_stipend_usd,
        covers_mobility_expenses, covers_medical_insurance, application_deadline,
        application_status, scholarship_url,
        eligible_country_ids, peru_eligible, unverified_fields)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(scholarship_id) DO UPDATE SET
        scholarship_name=excluded.scholarship_name,
        provider_organization=excluded.provider_organization,
        candidate_type=excluded.candidate_type,
        coverage_pct=excluded.coverage_pct,
        monthly_stipend_usd=excluded.monthly_stipend_usd,
        annual_stipend_usd=excluded.annual_stipend_usd,
        covers_mobility_expenses=excluded.covers_mobility_expenses,
        covers_medical_insurance=excluded.covers_medical_insurance,
        application_deadline=excluded.application_deadline,
        application_status=excluded.application_status,
        scholarship_url=excluded.scholarship_url,
        eligible_country_ids=excluded.eligible_country_ids,
        peru_eligible=excluded.peru_eligible,
        unverified_fields=excluded.unverified_fields,
        updated_at=CURRENT_TIMESTAMP
    """, (
        s["scholarship_id"], s["scholarship_name"], s["provider_organization"],
        json.dumps(s.get("candidate_type", [])),
        s.get("coverage_pct"), s.get("monthly_stipend_usd"), annual_stipend,
        s.get("covers_mobility_expenses"), s.get("covers_medical_insurance"),
        s.get("application_deadline"),
        s.get("application_status", "No Iniciada"),
        s.get("scholarship_url", ""),
        json.dumps(s.get("eligible_country_ids", [])),
        s.get("peru_eligible"),
        json.dumps(s.get("unverified_fields", []))
    ))


def link_program_scholarship(conn, program_id, scholarship_id):
    conn.execute("""
    INSERT OR IGNORE INTO program_scholarships (program_id, scholarship_id)
    VALUES (?, ?)
    """, (program_id, scholarship_id))


def upsert_pathway(conn, pw):
    conn.execute("""
    INSERT INTO viability_pathways (pathway_id, country_id, program_id, scholarship_id,
        legal_gap_months, viability_status, vci_usd, vci_rank,
        employment_months_required, coverage_ratio_pct, labor_pressure_index,
        alerts, recommended_action, embassy_warning, deadline_urgent)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(pathway_id) DO UPDATE SET
        country_id=excluded.country_id, program_id=excluded.program_id,
        scholarship_id=excluded.scholarship_id,
        legal_gap_months=excluded.legal_gap_months,
        viability_status=excluded.viability_status,
        vci_usd=excluded.vci_usd, vci_rank=excluded.vci_rank,
        employment_months_required=excluded.employment_months_required,
        coverage_ratio_pct=excluded.coverage_ratio_pct,
        labor_pressure_index=excluded.labor_pressure_index,
        alerts=excluded.alerts,
        recommended_action=excluded.recommended_action,
        embassy_warning=excluded.embassy_warning,
        deadline_urgent=excluded.deadline_urgent,
        updated_at=CURRENT_TIMESTAMP
    """, (
        pw["pathway_id"], pw["country_id"], pw["program_id"],
        pw.get("best_scholarship_id"), pw.get("legal_gap_months"),
        pw.get("viability_status"), pw.get("vci_usd"), pw.get("vci_rank"),
        pw.get("employment_months_required"), pw.get("coverage_ratio_pct"),
        pw.get("labor_pressure_index"),
        json.dumps(pw.get("alerts", [])),
        pw.get("recommended_action", ""),
        pw.get("embassy_warning", 0), pw.get("deadline_urgent", 0)
    ))


def seed_from_json():
    """Load existing JSON data files into SQLite."""
    data_dir = Path(__file__).resolve().parent.parent / "data"
    conn = get_conn()

    # Countries first
    cpath = data_dir / "countries.json"
    if cpath.exists():
        for c in json.loads(cpath.read_text()):
            upsert_country(conn, c)

    # Programs (without links yet)
    ppath = data_dir / "programs.json"
    if ppath.exists():
        for p in json.loads(ppath.read_text()):
            upsert_program(conn, p)

    # Scholarships (before linking)
    spath = data_dir / "scholarships.json"
    if spath.exists():
        for s in json.loads(spath.read_text()):
            upsert_scholarship(conn, s)

    # Now link programs ↔ scholarships
    if ppath.exists():
        for p in json.loads(ppath.read_text()):
            for sid in p.get("scholarship_ids", []):
                link_program_scholarship(conn, p["program_id"], sid)
    if spath.exists():
        for s in json.loads(spath.read_text()):
            for pid in s.get("applicable_program_ids", []):
                link_program_scholarship(conn, pid, s["scholarship_id"])

    # Viability report
    vpath = data_dir / "viability_report.json"
    if vpath.exists():
        for pw in json.loads(vpath.read_text()):
            upsert_pathway(conn, pw)

    conn.commit()
    conn.close()


def search_programs(keyword=None, degree_levels=None, country_ids=None):
    """Search programs with filters. Returns programs joined with country and pathway data."""
    conn = get_conn()

    query = """
    SELECT p.*, c.country_name, c.region, c.months_to_pr, c.study_visa_months,
           c.post_study_extension_months, c.solvency_buffer_usd,
           c.work_permit_allowed, c.max_hours_per_week, c.embassy_in_peru,
           c.link_pr, c.link_study_visa, c.link_visa_extension, c.link_work_permit,
           v.legal_gap_months, v.viability_status, v.vci_usd, v.vci_rank,
           v.employment_months_required, v.coverage_ratio_pct,
           v.labor_pressure_index, v.alerts, v.recommended_action,
           v.embassy_warning, v.deadline_urgent, v.scholarship_id as best_scholarship_id,
           bs.scholarship_name as best_scholarship_name,
           bs.coverage_pct as best_coverage_pct,
           bs.monthly_stipend_usd as best_stipend_monthly,
           bs.annual_stipend_usd as best_stipend_annual
    FROM programs p
    JOIN countries c ON p.country_id = c.country_id
    LEFT JOIN viability_pathways v ON v.program_id = p.program_id
    LEFT JOIN scholarships bs ON bs.scholarship_id = v.scholarship_id
    """

    conditions = []
    params = []

    if keyword and keyword.strip():
        matching_ids = []
        for row in conn.execute(
            "SELECT program_id FROM programs_fts WHERE programs_fts MATCH ?",
            (f'"{keyword.strip()}"',)
        ).fetchall():
            matching_ids.append(row["program_id"])
        like_rows = conn.execute(
            """SELECT program_id FROM programs WHERE
               program_name LIKE ? OR university LIKE ? OR
               faculty_or_department LIKE ? OR city LIKE ?""",
            tuple(f"%{keyword.strip()}%" for _ in range(4))
        ).fetchall()
        for row in like_rows:
            if row["program_id"] not in matching_ids:
                matching_ids.append(row["program_id"])

        if matching_ids:
            placeholders = ",".join("?" for _ in matching_ids)
            conditions.append(f"p.program_id IN ({placeholders})")
            params.extend(matching_ids)
        else:
            conn.close()
            return []

    if degree_levels:
        placeholders = ",".join("?" for _ in degree_levels)
        conditions.append(f"p.degree_level IN ({placeholders})")
        params.extend(degree_levels)

    if country_ids:
        placeholders = ",".join("?" for _ in country_ids)
        conditions.append(f"p.country_id IN ({placeholders})")
        params.extend(country_ids)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY v.vci_usd ASC NULLS LAST"

    rows = conn.execute(query, params).fetchall()
    results = [dict(r) for r in rows]
    conn.close()
    return results


def search_programs_by_ids(program_ids, degree_levels=None, country_ids=None):
    """Fetch programs by specific IDs, with optional filters."""
    conn = get_conn()
    placeholders = ",".join("?" for _ in program_ids)
    query = f"""
    SELECT p.*, c.country_name, c.region, c.months_to_pr, c.study_visa_months,
           c.post_study_extension_months, c.solvency_buffer_usd,
           c.work_permit_allowed, c.max_hours_per_week, c.embassy_in_peru,
           c.link_pr, c.link_study_visa, c.link_visa_extension, c.link_work_permit,
           v.legal_gap_months, v.viability_status, v.vci_usd, v.vci_rank,
           v.employment_months_required, v.coverage_ratio_pct,
           v.labor_pressure_index, v.alerts, v.recommended_action,
           v.embassy_warning, v.deadline_urgent, v.scholarship_id as best_scholarship_id,
           bs.scholarship_name as best_scholarship_name,
           bs.coverage_pct as best_coverage_pct,
           bs.monthly_stipend_usd as best_stipend_monthly,
           bs.annual_stipend_usd as best_stipend_annual
    FROM programs p
    JOIN countries c ON p.country_id = c.country_id
    LEFT JOIN viability_pathways v ON v.program_id = p.program_id
    LEFT JOIN scholarships bs ON bs.scholarship_id = v.scholarship_id
    WHERE p.program_id IN ({placeholders})
    """
    params = list(program_ids)

    if degree_levels:
        ph = ",".join("?" for _ in degree_levels)
        query += f" AND p.degree_level IN ({ph})"
        params.extend(degree_levels)
    if country_ids:
        ph = ",".join("?" for _ in country_ids)
        query += f" AND p.country_id IN ({ph})"
        params.extend(country_ids)

    query += " ORDER BY v.vci_usd ASC NULLS LAST"
    rows = conn.execute(query, params).fetchall()
    results = [dict(r) for r in rows]
    conn.close()
    return results


def get_program_detail(program_id):
    """Get full program detail with all related scholarships and country info."""
    conn = get_conn()

    row = conn.execute("""
    SELECT p.*, c.country_name, c.region, c.months_to_pr, c.study_visa_months,
           c.post_study_extension_months, c.solvency_buffer_usd,
           c.work_permit_allowed, c.max_hours_per_week, c.embassy_in_peru,
           c.link_pr, c.link_study_visa, c.link_visa_extension, c.link_work_permit,
           c.source_urls as country_source_urls,
           v.legal_gap_months, v.viability_status, v.vci_usd, v.vci_rank,
           v.employment_months_required, v.coverage_ratio_pct,
           v.labor_pressure_index, v.alerts, v.recommended_action,
           v.embassy_warning, v.deadline_urgent
    FROM programs p
    JOIN countries c ON p.country_id = c.country_id
    LEFT JOIN viability_pathways v ON v.program_id = p.program_id
    WHERE p.program_id = ?
    """, (program_id,)).fetchone()

    if not row:
        conn.close()
        return None

    program = dict(row)

    schols = conn.execute("""
    SELECT s.*
    FROM scholarships s
    JOIN program_scholarships ps ON s.scholarship_id = ps.scholarship_id
    WHERE ps.program_id = ?
    ORDER BY s.coverage_pct DESC NULLS LAST
    """, (program_id,)).fetchall()

    program["scholarships"] = [dict(s) for s in schols]
    conn.close()
    return program


def get_all_countries():
    conn = get_conn()
    rows = conn.execute("SELECT country_id, country_name, region FROM countries ORDER BY country_name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


if __name__ == "__main__":
    init_db()
    seed_from_json()
    print("Database initialized and seeded.")
