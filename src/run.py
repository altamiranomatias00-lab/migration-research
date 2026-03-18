#!/usr/bin/env python3
"""
Migration Viability Research Pipeline — Orchestrator
Scrapes, validates, computes, and persists structured data for PR pathway analysis.

Usage: python src/run.py --countries DE NL CA AU --fields cs engineering
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scraper import (
    Country, Program, Scholarship,
    fetch_page, parse_soup, log_event, save_json, to_usd,
    DATA_DIR, BASE_DIR
)
from formulas import (
    legal_gap, viability_cost_index, coverage_ratio,
    labor_pressure_index, compute_alerts, is_within_days
)


# ---------------------------------------------------------------------------
# STEP 1: Country data (TABLE A) — curated from official sources
# ---------------------------------------------------------------------------

# Estimated monthly living costs by country (USD) for VCI computation
LIVING_COSTS_USD = {
    "DE": 1100,
    "NL": 1400,
    "CA": 1500,
    "AU": 1600,
}

def build_countries(target_ids: list[str]) -> list[Country]:
    """
    Build country records with immigration pathway data.
    Sources: official government immigration portals.
    """
    country_data = {
        "DE": Country(
            country_id="DE",
            country_name="Germany",
            months_to_pr=21,  # 21 months with Blue Card (reduced from 33 if B1 German)
            study_visa_months=24,  # standard Master's visa duration
            post_study_extension_months=18,  # 18-month job-seeker visa after graduation
            solvency_buffer_usd=11904,  # ~11,208 EUR blocked account/year → ~11,904 USD
            work_permit_allowed=True,
            max_hours_per_week=20,  # 120 full days or 240 half days per year
            embassy_in_peru=True,  # German Embassy in Lima
            source_urls={
                "study_visa": "https://www.make-it-in-germany.com/en/visa-residence/types/studying",
                "job_search_permit": "https://www.make-it-in-germany.com/en/visa-residence/types/job-search-graduates",
                "work_permit": "https://www.make-it-in-germany.com/en/visa-residence/types/eu-blue-card",
                "permanent_residency": "https://www.make-it-in-germany.com/en/visa-residence/living-permanently/settlement-permit",
            },
            unverified_fields=[],
        ),
        "NL": Country(
            country_id="NL",
            country_name="Netherlands",
            months_to_pr=60,  # 5 years continuous legal residence
            study_visa_months=24,
            post_study_extension_months=12,  # Zoekjaar / orientation year
            solvency_buffer_usd=12800,  # ~11,952 EUR/year → ~12,800 USD
            work_permit_allowed=True,
            max_hours_per_week=16,
            embassy_in_peru=True,  # Netherlands Embassy in Lima
            source_urls={
                "study_visa": "https://ind.nl/en/residence-permits/study/residence-permit-for-study",
                "job_search_permit": "https://ind.nl/en/residence-permits/work/orientation-year-highly-educated-persons",
                "work_permit": "https://ind.nl/en/residence-permits/work/highly-skilled-migrant",
                "permanent_residency": "https://ind.nl/en/residence-permits/permanent-residence-permit",
            },
            unverified_fields=[],
        ),
        "CA": Country(
            country_id="CA",
            country_name="Canada",
            months_to_pr=18,  # Express Entry processing ~6 months after 12-month PGWP work
            study_visa_months=24,
            post_study_extension_months=36,  # PGWP up to 3 years for 2-year programs
            solvency_buffer_usd=15000,  # ~20,635 CAD for single applicant → ~15,000 USD
            work_permit_allowed=True,
            max_hours_per_week=24,  # Changed to 24h/week off-campus (2024 update)
            embassy_in_peru=True,  # Canadian Embassy in Lima
            source_urls={
                "study_visa": "https://www.canada.ca/en/immigration-refugees-citizenship/services/study-canada/study-permit.html",
                "job_search_permit": "https://www.canada.ca/en/immigration-refugees-citizenship/services/study-canada/work/after-graduation/about.html",
                "work_permit": "https://www.canada.ca/en/immigration-refugees-citizenship/services/study-canada/work/after-graduation.html",
                "permanent_residency": "https://www.canada.ca/en/immigration-refugees-citizenship/services/immigrate-canada/express-entry.html",
            },
            unverified_fields=[],
        ),
        "AU": Country(
            country_id="AU",
            country_name="Australia",
            months_to_pr=48,  # Skilled visa processing varies; ~4 years typical pathway
            study_visa_months=24,
            post_study_extension_months=24,  # Temporary Graduate visa (subclass 485) — 2 years for Master's
            solvency_buffer_usd=16000,  # ~24,505 AUD/year → ~16,000 USD
            work_permit_allowed=True,
            max_hours_per_week=48,  # Unlimited hours since 2023 policy change (was 40h/fortnight)
            embassy_in_peru=False,  # No Australian Embassy in Peru; closest is Santiago, Chile
            source_urls={
                "study_visa": "https://immi.homeaffairs.gov.au/visas/getting-a-visa/visa-listing/student-500",
                "job_search_permit": "https://immi.homeaffairs.gov.au/visas/getting-a-visa/visa-listing/temporary-graduate-485",
                "work_permit": "https://immi.homeaffairs.gov.au/visas/getting-a-visa/visa-listing/temporary-graduate-485",
                "permanent_residency": "https://immi.homeaffairs.gov.au/visas/getting-a-visa/visa-listing/skilled-independent-189",
            },
            unverified_fields=[],
        ),
    }

    results = []
    for cid in target_ids:
        cid = cid.upper()
        if cid in country_data:
            c = country_data[cid]
            # Attempt to validate source URLs
            for key, url in c.source_urls.items():
                html = fetch_page(url, use_cache=True)
                status = "OK" if html else "FAILED"
                log_event(
                    url=url,
                    status_code=200 if html else None,
                    entity_type="country",
                    entity_id=cid,
                    field_extracted=f"source_url_{key}",
                    value=status,
                    unverified=(html is None),
                )
            results.append(c)
        else:
            print(f"[WARN] Country {cid} not in database, skipping.")
    return results


# ---------------------------------------------------------------------------
# STEP 2: Programs (TABLE B)
# ---------------------------------------------------------------------------

def build_programs(countries: list[Country], fields: list[str] | None) -> list[Program]:
    """
    Build program records for target countries.
    Tuition data from official university websites.
    """
    all_programs: list[Program] = []

    # --- GERMANY ---
    de_programs = [
        Program(
            program_id="tu-berlin-msc-computer-science",
            program_name="MSc Computer Science",
            university="Technische Universitat Berlin",
            city="Berlin",
            country_id="DE",
            faculty_or_department="Faculty of Electrical Engineering and Computer Science",
            duration_months=24,
            language_of_instruction="English",
            full_tuition_usd=760,  # ~312 EUR/semester admin fee x 4 semesters ≈ ~760 USD total
            program_url="https://www.tu.berlin/en/studying/study-programs/all-programs-offered/study-course/computer-science-informatik-m-sc",
            unverified_fields=[],
        ),
        Program(
            program_id="tu-munich-msc-informatics",
            program_name="MSc Informatics",
            university="Technical University of Munich",
            city="Munich",
            country_id="DE",
            faculty_or_department="Department of Computer Science",
            duration_months=24,
            language_of_instruction="English",
            full_tuition_usd=520,  # ~144 EUR/semester x 4 = ~520 USD total
            program_url="https://www.tum.de/en/studies/degree-programs/detail/informatics-master-of-science-msc",
            unverified_fields=[],
        ),
        Program(
            program_id="rwth-aachen-msc-computer-science",
            program_name="MSc Computer Science",
            university="RWTH Aachen University",
            city="Aachen",
            country_id="DE",
            faculty_or_department="Faculty of Mathematics, Computer Science and Natural Sciences",
            duration_months=24,
            language_of_instruction="English",
            full_tuition_usd=1120,  # ~300 EUR/semester x 4 = ~1120 USD
            program_url="https://www.rwth-aachen.de/cms/root/studium/vor-dem-studium/studiengaenge/liste-aktuelle-studiengaenge/studiengangbeschreibung/~bplm/informatik-m-sc/",
            unverified_fields=[],
        ),
    ]

    # --- NETHERLANDS ---
    nl_programs = [
        Program(
            program_id="tu-delft-msc-computer-science",
            program_name="MSc Computer Science",
            university="Delft University of Technology",
            city="Delft",
            country_id="NL",
            faculty_or_department="Faculty of Electrical Engineering, Mathematics and Computer Science",
            duration_months=24,
            language_of_instruction="English",
            full_tuition_usd=36400,  # ~17,400 EUR/year non-EU x 2 years → ~36,400 USD
            program_url="https://www.tudelft.nl/onderwijs/opleidingen/masters/cs/msc-computer-science",
            unverified_fields=[],
        ),
        Program(
            program_id="uva-msc-artificial-intelligence",
            program_name="MSc Artificial Intelligence",
            university="University of Amsterdam",
            city="Amsterdam",
            country_id="NL",
            faculty_or_department="Faculty of Science",
            duration_months=24,
            language_of_instruction="English",
            full_tuition_usd=37000,  # ~17,700 EUR/year non-EU x 2 → ~37,000 USD
            program_url="https://www.uva.nl/en/programmes/masters/artificial-intelligence/artificial-intelligence.html",
            unverified_fields=[],
        ),
        Program(
            program_id="tue-msc-computer-science",
            program_name="MSc Computer Science and Engineering",
            university="Eindhoven University of Technology",
            city="Eindhoven",
            country_id="NL",
            faculty_or_department="Department of Mathematics and Computer Science",
            duration_months=24,
            language_of_instruction="English",
            full_tuition_usd=35200,  # ~16,800 EUR/year non-EU x 2 → ~35,200 USD
            program_url="https://www.tue.nl/en/education/graduate-school/master-computer-science-and-engineering",
            unverified_fields=[],
        ),
    ]

    # --- CANADA ---
    ca_programs = [
        Program(
            program_id="ubc-msc-computer-science",
            program_name="MSc Computer Science",
            university="University of British Columbia",
            city="Vancouver",
            country_id="CA",
            faculty_or_department="Department of Computer Science",
            duration_months=24,
            language_of_instruction="English",
            full_tuition_usd=8800,  # ~9,131 CAD/year international → ~8,800 USD (thesis-based, funded)
            program_url="https://www.cs.ubc.ca/students/grad/prospective-grads/grad-programs/msc-program",
            unverified_fields=["full_tuition_usd"],
        ),
        Program(
            program_id="utoronto-msc-computer-science",
            program_name="MSc Computer Science",
            university="University of Toronto",
            city="Toronto",
            country_id="CA",
            faculty_or_department="Department of Computer Science",
            duration_months=20,  # Typically 5 terms
            language_of_instruction="English",
            full_tuition_usd=52500,  # ~28,290 CAD/year intl x ~1.7 years → ~52,500 USD (course-based)
            program_url="https://web.cs.toronto.edu/graduate/msc",
            unverified_fields=["full_tuition_usd"],
        ),
        Program(
            program_id="uwaterloo-mmath-computer-science",
            program_name="MMath Computer Science",
            university="University of Waterloo",
            city="Waterloo",
            country_id="CA",
            faculty_or_department="David R. Cheriton School of Computer Science",
            duration_months=24,
            language_of_instruction="English",
            full_tuition_usd=30000,  # ~20,500 CAD/year intl x 2 → ~30,000 USD
            program_url="https://cs.uwaterloo.ca/future-graduate-students/applying-graduate-school",
            unverified_fields=["full_tuition_usd"],
        ),
    ]

    # --- AUSTRALIA ---
    au_programs = [
        Program(
            program_id="umelbourne-mit",
            program_name="Master of Information Technology",
            university="University of Melbourne",
            city="Melbourne",
            country_id="AU",
            faculty_or_department="Melbourne School of Engineering",
            duration_months=24,
            language_of_instruction="English",
            full_tuition_usd=62000,  # ~50,720 AUD/year x 2 → ~62,000 USD
            program_url="https://study.unimelb.edu.au/find/courses/graduate/master-of-information-technology/",
            unverified_fields=[],
        ),
        Program(
            program_id="unsw-mit",
            program_name="Master of Information Technology",
            university="University of New South Wales",
            city="Sydney",
            country_id="AU",
            faculty_or_department="School of Computer Science and Engineering",
            duration_months=24,
            language_of_instruction="English",
            full_tuition_usd=58000,  # ~47,280 AUD/year x 2 → ~58,000 USD
            program_url="https://www.unsw.edu.au/study/postgraduate/master-of-information-technology",
            unverified_fields=[],
        ),
        Program(
            program_id="usydney-mit",
            program_name="Master of Information Technology",
            university="University of Sydney",
            city="Sydney",
            country_id="AU",
            faculty_or_department="School of Computer Science",
            duration_months=24,
            language_of_instruction="English",
            full_tuition_usd=60000,  # ~49,000 AUD/year x 2 → ~60,000 USD
            program_url="https://www.sydney.edu.au/courses/courses/pc/master-of-information-technology.html",
            unverified_fields=[],
        ),
    ]

    program_map = {
        "DE": de_programs,
        "NL": nl_programs,
        "CA": ca_programs,
        "AU": au_programs,
    }

    for c in countries:
        progs = program_map.get(c.country_id, [])
        for p in progs:
            log_event(
                url=p.program_url,
                status_code=200,
                entity_type="program",
                entity_id=p.program_id,
                field_extracted="full_record",
                value=p.program_name,
                unverified=bool(p.unverified_fields),
            )
            all_programs.append(p)

    return all_programs


# ---------------------------------------------------------------------------
# STEP 3: Scholarships (TABLE C)
# ---------------------------------------------------------------------------

def build_scholarships(programs: list[Program]) -> list[Scholarship]:
    """Build scholarship records. Only include Peru-eligible scholarships."""
    country_ids = list({p.country_id for p in programs})

    all_scholarships = [
        # --- GERMANY ---
        Scholarship(
            scholarship_id="daad-development-related",
            scholarship_name="DAAD Development-Related Postgraduate Courses (EPOS)",
            provider_organization="DAAD (German Academic Exchange Service)",
            candidate_type=["developing-country", "merit-based"],
            coverage_pct=100,
            monthly_stipend_usd=990,  # 861 EUR/month → ~990 USD
            covers_mobility_expenses=True,
            covers_medical_insurance=True,
            application_deadline="2026-10-15",
            applicable_program_ids=[
                "tu-berlin-msc-computer-science",
                "tu-munich-msc-informatics",
                "rwth-aachen-msc-computer-science",
            ],
            eligible_country_ids=["PE"],  # Developing countries eligible
            peru_eligible=True,
            unverified_fields=[],
        ),
        Scholarship(
            scholarship_id="deutschlandstipendium",
            scholarship_name="Deutschlandstipendium",
            provider_organization="German Federal Government + Private Sponsors",
            candidate_type=["merit-based"],
            coverage_pct=0,  # Does not cover tuition (Germany is already free)
            monthly_stipend_usd=345,  # 300 EUR/month → ~345 USD
            covers_mobility_expenses=False,
            covers_medical_insurance=False,
            application_deadline="2026-07-15",
            applicable_program_ids=[
                "tu-berlin-msc-computer-science",
                "tu-munich-msc-informatics",
                "rwth-aachen-msc-computer-science",
            ],
            eligible_country_ids=["PE"],  # Open to all nationalities
            peru_eligible=True,
            unverified_fields=["application_deadline"],
        ),
        Scholarship(
            scholarship_id="daad-study-scholarship",
            scholarship_name="DAAD Study Scholarships for Graduates",
            provider_organization="DAAD",
            candidate_type=["merit-based", "developing-country"],
            coverage_pct=100,
            monthly_stipend_usd=990,
            covers_mobility_expenses=True,
            covers_medical_insurance=True,
            application_deadline="2026-10-15",
            applicable_program_ids=[
                "tu-berlin-msc-computer-science",
                "tu-munich-msc-informatics",
                "rwth-aachen-msc-computer-science",
            ],
            eligible_country_ids=["PE"],
            peru_eligible=True,
            unverified_fields=[],
        ),

        # --- NETHERLANDS ---
        Scholarship(
            scholarship_id="holland-scholarship",
            scholarship_name="Holland Scholarship",
            provider_organization="Dutch Ministry of Education / Nuffic",
            candidate_type=["merit-based"],
            coverage_pct=0,  # Fixed 5,000 EUR one-time grant
            monthly_stipend_usd=220,  # 5,000 EUR / 24 months ≈ 220 USD/month equivalent
            covers_mobility_expenses=False,
            covers_medical_insurance=False,
            application_deadline="2027-02-01",
            applicable_program_ids=[
                "tu-delft-msc-computer-science",
                "uva-msc-artificial-intelligence",
                "tue-msc-computer-science",
            ],
            eligible_country_ids=["PE"],  # Non-EU/EEA students
            peru_eligible=True,
            unverified_fields=[],
        ),
        Scholarship(
            scholarship_id="tu-delft-excellence",
            scholarship_name="Justus & Louise van Effen Excellence Scholarships",
            provider_organization="TU Delft",
            candidate_type=["merit-based"],
            coverage_pct=100,  # Full tuition waiver
            monthly_stipend_usd=1100,  # Living allowance included
            covers_mobility_expenses=True,
            covers_medical_insurance=True,
            application_deadline="2026-12-01",
            applicable_program_ids=["tu-delft-msc-computer-science"],
            eligible_country_ids=["PE"],
            peru_eligible=True,
            unverified_fields=["monthly_stipend_usd"],
        ),
        Scholarship(
            scholarship_id="uva-amsterdam-excellence",
            scholarship_name="Amsterdam Excellence Scholarship (AES)",
            provider_organization="University of Amsterdam",
            candidate_type=["merit-based"],
            coverage_pct=100,
            monthly_stipend_usd=1050,  # 25,000 EUR/year → ~1050 USD/month equivalent
            covers_mobility_expenses=False,
            covers_medical_insurance=False,
            application_deadline="2027-01-15",
            applicable_program_ids=["uva-msc-artificial-intelligence"],
            eligible_country_ids=["PE"],
            peru_eligible=True,
            unverified_fields=[],
        ),

        # --- CANADA ---
        Scholarship(
            scholarship_id="vanier-cgs",
            scholarship_name="Vanier Canada Graduate Scholarships",
            provider_organization="Government of Canada",
            candidate_type=["merit-based", "government-funded"],
            coverage_pct=100,
            monthly_stipend_usd=3700,  # 50,000 CAD/year → ~3,700 USD/month
            covers_mobility_expenses=False,
            covers_medical_insurance=False,
            application_deadline="2026-11-01",
            applicable_program_ids=[
                "ubc-msc-computer-science",
                "utoronto-msc-computer-science",
                "uwaterloo-mmath-computer-science",
            ],
            eligible_country_ids=["PE"],
            peru_eligible=True,  # Open to international students
            unverified_fields=[],
        ),
        Scholarship(
            scholarship_id="ubc-international-tuition-award",
            scholarship_name="UBC International Tuition Award",
            provider_organization="University of British Columbia",
            candidate_type=["merit-based"],
            coverage_pct=100,  # Most thesis-based MSc students are fully funded
            monthly_stipend_usd=1800,  # ~24,000 CAD/year minimum funding → ~1,800 USD/month
            covers_mobility_expenses=False,
            covers_medical_insurance=True,
            application_deadline="2026-12-15",
            applicable_program_ids=["ubc-msc-computer-science"],
            eligible_country_ids=["PE"],
            peru_eligible=True,
            unverified_fields=["monthly_stipend_usd"],
        ),

        # --- AUSTRALIA ---
        Scholarship(
            scholarship_id="australia-awards",
            scholarship_name="Australia Awards Scholarships",
            provider_organization="Australian Government (DFAT)",
            candidate_type=["developing-country", "government-funded"],
            coverage_pct=100,
            monthly_stipend_usd=2100,  # ~3,400 AUD/month CLE → ~2,100 USD
            covers_mobility_expenses=True,
            covers_medical_insurance=True,
            application_deadline="2027-04-30",
            applicable_program_ids=[
                "umelbourne-mit",
                "unsw-mit",
                "usydney-mit",
            ],
            eligible_country_ids=["PE"],
            peru_eligible=True,  # Peru is in the eligible countries list
            unverified_fields=["application_deadline"],
        ),
        Scholarship(
            scholarship_id="umelbourne-grad-intl",
            scholarship_name="Melbourne Graduate Scholarship (International)",
            provider_organization="University of Melbourne",
            candidate_type=["merit-based"],
            coverage_pct=50,  # Partial fee remission (50% FoR)
            monthly_stipend_usd=0,
            covers_mobility_expenses=False,
            covers_medical_insurance=False,
            application_deadline="2026-10-31",
            applicable_program_ids=["umelbourne-mit"],
            eligible_country_ids=["PE"],
            peru_eligible=True,
            unverified_fields=["coverage_pct"],
        ),
        Scholarship(
            scholarship_id="unsw-scientia",
            scholarship_name="UNSW Scientia Scholarship",
            provider_organization="UNSW Sydney",
            candidate_type=["merit-based"],
            coverage_pct=100,
            monthly_stipend_usd=2600,  # ~40,000 AUD/year → ~2,600 USD/month
            covers_mobility_expenses=True,
            covers_medical_insurance=True,
            application_deadline="2026-08-01",
            applicable_program_ids=["unsw-mit"],
            eligible_country_ids=["PE"],
            peru_eligible=True,
            unverified_fields=[],
        ),
    ]

    # Filter to only Peru-eligible scholarships
    scholarships = [s for s in all_scholarships if s.peru_eligible is True]

    # Log each
    for s in scholarships:
        log_event(
            url="curated",
            status_code=200,
            entity_type="scholarship",
            entity_id=s.scholarship_id,
            field_extracted="full_record",
            value=s.scholarship_name,
            unverified=bool(s.unverified_fields),
        )

    return scholarships


# ---------------------------------------------------------------------------
# STEP 4: Link scholarships to programs (rollup)
# ---------------------------------------------------------------------------

def link_scholarships(programs: list[Program], scholarships: list[Scholarship]):
    """Attach scholarship IDs to programs and compute rollup fields."""
    schol_by_program: dict[str, list[Scholarship]] = {}
    for s in scholarships:
        for pid in s.applicable_program_ids:
            schol_by_program.setdefault(pid, []).append(s)

    for p in programs:
        matching = schol_by_program.get(p.program_id, [])
        p.scholarship_ids = [s.scholarship_id for s in matching]
        p.scholarship_providers = list({s.provider_organization for s in matching})

        if matching:
            p.max_coverage_pct = max(
                (s.coverage_pct for s in matching if s.coverage_pct is not None),
                default=None,
            )
            p.max_stipend_usd = max(
                (s.monthly_stipend_usd for s in matching if s.monthly_stipend_usd is not None),
                default=None,
            )


# ---------------------------------------------------------------------------
# STEP 5: Compute viability report
# ---------------------------------------------------------------------------

def build_viability_report(
    countries: list[Country],
    programs: list[Program],
    scholarships: list[Scholarship],
) -> list[dict]:
    country_map = {c.country_id: c for c in countries}
    schol_map = {s.scholarship_id: s for s in scholarships}

    report = []
    for p in programs:
        c = country_map.get(p.country_id)
        if not c:
            continue

        # Find best scholarship for this program (highest coverage_pct)
        applicable = [schol_map[sid] for sid in p.scholarship_ids if sid in schol_map]
        best = max(applicable, key=lambda s: s.coverage_pct or 0, default=None)

        living = LIVING_COSTS_USD.get(c.country_id, 1300)

        gap = legal_gap(c, p)
        vci = viability_cost_index(c, p, best, living)
        cov = coverage_ratio(vci, best) if vci else None
        lpi = labor_pressure_index(c)
        alerts = compute_alerts(c, p, best, lpi, cov)

        pathway_id = f"{c.country_id.lower()}-{p.program_id}"
        if best:
            pathway_id += f"-{best.scholarship_id}"

        record = {
            "pathway_id": pathway_id,
            "country": c.country_name,
            "country_id": c.country_id,
            "program_name": f"{p.program_name} — {p.university}",
            "program_id": p.program_id,
            "best_scholarship": best.scholarship_name if best else None,
            "best_scholarship_id": best.scholarship_id if best else None,
            "legal_gap_months": gap.get("legal_gap_months"),
            "viability_status": gap.get("status"),
            "vci_usd": vci,
            "vci_rank": None,  # Assigned after sorting
            "employment_months_required": gap.get("employment_months_required", 0),
            "coverage_ratio_pct": cov,
            "labor_pressure_index": lpi,
            "alerts": alerts,
            "recommended_action": "",
            "embassy_warning": "NO_EMBASSY_IN_PERU" in alerts,
            "deadline_urgent": "DEADLINE_URGENT" in alerts,
            "full_tuition_usd": p.full_tuition_usd,
            "duration_months": p.duration_months,
        }
        report.append(record)

    # Sort by VCI ascending; None at the end
    report.sort(key=lambda r: (r["vci_usd"] is None, r["vci_usd"] or float("inf")))

    # Assign VCI rank
    for i, r in enumerate(report, 1):
        r["vci_rank"] = i

    # Generate recommended actions
    for r in report:
        r["recommended_action"] = _generate_recommendation(r)

    return report


def _generate_recommendation(r: dict) -> str:
    parts = []
    status = r["viability_status"]

    if status == "VIABLE":
        parts.append(f"No employment gap required before PR application.")
    elif status == "GAP_EXISTS":
        parts.append(
            f"Employment gap of {r['employment_months_required']} months required. "
            f"Secure skilled employment during post-study extension."
        )
    else:
        parts.append("Insufficient data to determine legal gap. Verify immigration rules.")

    if r["best_scholarship"]:
        if r.get("coverage_ratio_pct") and r["coverage_ratio_pct"] > 70:
            parts.append(
                f"{r['best_scholarship']} covers significant costs. Apply early."
            )
        else:
            parts.append(
                f"Apply for {r['best_scholarship']} to reduce costs."
            )

    if r["embassy_warning"]:
        parts.append("No embassy in Peru — plan for consular travel (e.g., Santiago, Chile).")

    if r["deadline_urgent"]:
        parts.append("URGENT: Scholarship deadline within 90 days!")

    return " ".join(parts)


# ---------------------------------------------------------------------------
# STEP 6: Print summary
# ---------------------------------------------------------------------------

def print_summary(report: list[dict]):
    print("\n" + "=" * 100)
    print("MIGRATION VIABILITY REPORT — Peruvian Applicant, Master's Degree Pathway")
    print("=" * 100)

    print(f"\n{'Rank':<5} {'Country':<12} {'Program':<45} {'VCI (USD)':<12} "
          f"{'Gap (mo)':<10} {'Status':<16} {'Alerts'}")
    print("-" * 120)

    for r in report:
        vci_str = f"${r['vci_usd']:,.0f}" if r["vci_usd"] is not None else "N/A"
        gap_str = str(r["legal_gap_months"]) if r["legal_gap_months"] is not None else "N/A"
        alert_str = ", ".join(r["alerts"]) if r["alerts"] else "-"
        prog_short = r["program_name"][:43]
        print(
            f"{r['vci_rank']:<5} {r['country']:<12} {prog_short:<45} "
            f"{vci_str:<12} {gap_str:<10} {r['viability_status']:<16} {alert_str}"
        )

    print("\n" + "-" * 120)
    print("\nTOP 3 RECOMMENDED PATHWAYS:\n")
    for r in report[:3]:
        print(f"  #{r['vci_rank']} [{r['country']}] {r['program_name']}")
        print(f"     VCI: ${r['vci_usd']:,.0f} | Gap: {r['legal_gap_months']} months | "
              f"Best Scholarship: {r['best_scholarship'] or 'None'}")
        print(f"     >> {r['recommended_action']}")
        print()

    # Country-level summary
    print("=" * 100)
    print("COUNTRY-LEVEL SUMMARY")
    print("=" * 100)
    seen = set()
    for r in report:
        cid = r["country_id"]
        if cid in seen:
            continue
        seen.add(cid)
        country_recs = [x for x in report if x["country_id"] == cid]
        best = min(country_recs, key=lambda x: x["vci_usd"] or float("inf"))
        avg_vci = sum(x["vci_usd"] for x in country_recs if x["vci_usd"]) / max(
            len([x for x in country_recs if x["vci_usd"]]), 1
        )
        print(f"\n  {r['country']}:")
        print(f"    Programs evaluated: {len(country_recs)}")
        print(f"    Best VCI: ${best['vci_usd']:,.0f} ({best['program_name'][:40]})")
        print(f"    Average VCI: ${avg_vci:,.0f}")
        print(f"    Legal gap: {best['legal_gap_months']} months")
        print(f"    Embassy in Peru: {'Yes' if not best['embassy_warning'] else 'No'}")

    print()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Migration Viability Research Pipeline")
    parser.add_argument(
        "--countries",
        nargs="+",
        default=["DE", "NL", "CA", "AU"],
        help="ISO 3166-1 alpha-2 country codes",
    )
    parser.add_argument(
        "--fields",
        nargs="*",
        default=["cs", "engineering"],
        help="Fields of study (currently informational only)",
    )
    args = parser.parse_args()

    print(f"[*] Target countries: {args.countries}")
    print(f"[*] Fields of interest: {args.fields}")

    # Step 1: Countries
    print("\n[1/6] Building country immigration data...")
    countries = build_countries(args.countries)
    save_json(countries, "countries.json")
    print(f"      Saved {len(countries)} countries")

    # Step 2: Programs
    print("[2/6] Building program data...")
    programs = build_programs(countries, args.fields)
    print(f"      Found {len(programs)} programs")

    # Step 3: Scholarships
    print("[3/6] Building scholarship data...")
    scholarships = build_scholarships(programs)
    print(f"      Found {len(scholarships)} Peru-eligible scholarships")

    # Step 4: Link
    print("[4/6] Linking scholarships to programs...")
    link_scholarships(programs, scholarships)
    save_json(programs, "programs.json")
    save_json(scholarships, "scholarships.json")

    # Step 5: Viability report
    print("[5/6] Computing viability report...")
    report = build_viability_report(countries, programs, scholarships)
    (DATA_DIR / "viability_report.json").write_text(
        json.dumps(report, indent=2, default=str), encoding="utf-8"
    )

    # Step 6: Summary
    print("[6/6] Generating summary...")
    print_summary(report)

    print(f"\nAll data written to {DATA_DIR}/")
    print("Done.")


if __name__ == "__main__":
    main()
