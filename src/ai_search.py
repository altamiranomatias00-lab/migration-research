"""
AI-powered search using Google Gemini API (google-genai SDK).
Researches real university programs, immigration data, and scholarships on demand.
Returns data matching Notion schema: Países, Maestrias, Becas.
"""
import json
import os
import time
from pathlib import Path
from google import genai
from google.genai import types

# Load from .env file if not in environment
def _load_api_key():
    key = os.environ.get("GEMINI_API_KEY", "")
    if not key:
        env_file = Path(__file__).resolve().parent.parent / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("GEMINI_API_KEY="):
                    key = line.split("=", 1)[1].strip()
                    break
    return key

GEMINI_API_KEY = _load_api_key()
ANTHROPIC_API_KEY = GEMINI_API_KEY  # backward compat

# Initialize client
_client = None
if GEMINI_API_KEY:
    _client = genai.Client(api_key=GEMINI_API_KEY)

# Ordered list of models to try; first success wins.
MODEL_CANDIDATES = [
    "gemini-2.5-flash",
    "gemini-2.0-flash",
]

# Maximum retries per model attempt.
_MAX_RETRIES = 2
_BACKOFF_BASE = 2  # seconds


def _repair_json(text: str) -> dict:
    """Try to repair truncated JSON from AI response."""
    import re
    for trim in range(min(len(text), 2000)):
        candidate = text[:len(text) - trim]
        opens = candidate.count('{') - candidate.count('}')
        open_brackets = candidate.count('[') - candidate.count(']')
        suffix = ']' * open_brackets + '}' * opens
        try:
            result = json.loads(candidate + suffix)
            if isinstance(result, dict):
                print(f"[AI] Repaired JSON by trimming {trim} chars", flush=True)
                return result
        except json.JSONDecodeError:
            continue
    result = {"countries": [], "programs": [], "scholarships": []}
    for key in result:
        pattern = f'"{key}"\\s*:\\s*\\['
        m = re.search(pattern, text)
        if m:
            start = m.end() - 1
            depth = 0
            for i in range(start, len(text)):
                if text[i] == '[': depth += 1
                elif text[i] == ']': depth -= 1
                if depth == 0:
                    try:
                        result[key] = json.loads(text[start:i+1])
                    except json.JSONDecodeError:
                        pass
                    break
    print(f"[AI] Extracted arrays: {[k for k,v in result.items() if v]}", flush=True)
    return result


# ---------------------------------------------------------------------------
# Validation & post-processing
# ---------------------------------------------------------------------------

_COUNTRY_REQUIRED = {
    "country_id": "",
    "country_name": "Desconocido",
    "region": "Desconocido",
    "months_to_pr": None,
    "study_visa_months": None,
    "post_study_extension_months": None,
    "solvency_buffer_usd": None,
    "work_permit_allowed": False,
    "max_hours_per_week": None,
    "embassy_in_peru": False,
    "link_pr": "",
    "link_study_visa": "",
    "link_visa_extension": "",
    "link_work_permit": "",
    "source_urls": {},
    "unverified_fields": [],
}

_PROGRAM_REQUIRED = {
    "program_id": "",
    "program_name": "Unknown Program",
    "university": "Unknown University",
    "city": "",
    "country_id": "",
    "faculty_or_department": "Desconocido",
    "degree_level": "masters",
    "duration_months": 24,
    "language_of_instruction": "English",
    "full_tuition_usd": None,
    "program_url": "",
    "coverage_pct": None,
    "stipend_monthly_usd": None,
    "university_scholarship": "",
    "unverified_fields": [],
}

_SCHOLARSHIP_REQUIRED = {
    "scholarship_id": "",
    "scholarship_name": "Unknown Scholarship",
    "provider_organization": "Universidad Pública",
    "candidate_type": ["País en desarrollo"],
    "coverage_pct": None,
    "monthly_stipend_usd": None,
    "covers_mobility_expenses": None,
    "covers_medical_insurance": None,
    "application_deadline": None,
    "application_status": "No Iniciada",
    "scholarship_url": "",
    "applicable_program_ids": [],
    "eligible_country_ids": [],
    "peru_eligible": None,
    "unverified_fields": [],
}


def _fill_defaults(item: dict, defaults: dict) -> dict:
    for key, default in defaults.items():
        if key not in item:
            item[key] = default
        elif item[key] is None and default is not None:
            item[key] = default
    return item


def _validate_and_postprocess(result: dict, degree_level: str) -> dict | None:
    required_keys = ("countries", "programs", "scholarships")
    for key in required_keys:
        if key not in result or not isinstance(result[key], list):
            result[key] = []

    if not result["countries"] and not result["programs"]:
        print("[AI] Validation failed: no countries and no programs returned", flush=True)
        return None

    # Filter out countries with suspicious/missing immigration data
    valid_countries = []
    for country in result["countries"]:
        _fill_defaults(country, _COUNTRY_REQUIRED)
        mpr = country.get("months_to_pr")
        svm = country.get("study_visa_months")
        # Reject if months_to_pr is 0, negative, or missing — it means no real PR pathway
        if mpr is not None and mpr <= 0:
            print(f"[AI] Rejected country {country.get('country_id')}: months_to_pr={mpr}", flush=True)
            continue
        if mpr is None and svm is None:
            print(f"[AI] Rejected country {country.get('country_id')}: no immigration data", flush=True)
            continue
        # Replace immigration links with Google search fallbacks
        cname = country.get("country_name", "")
        for link_field, search_term in [
            ("link_pr", f"{cname} permanent residency requirements"),
            ("link_study_visa", f"{cname} student visa application"),
            ("link_visa_extension", f"{cname} post study work visa"),
            ("link_work_permit", f"{cname} work permit international students"),
        ]:
            country[f"{link_field}_original"] = country.get(link_field, "")
            country[link_field] = f"https://www.google.com/search?q={search_term.replace(' ', '+')}"

        valid_countries.append(country)
    result["countries"] = valid_countries
    valid_country_ids = {c["country_id"] for c in valid_countries}

    valid_programs = []
    for program in result["programs"]:
        defaults = {**_PROGRAM_REQUIRED, "degree_level": degree_level}
        _fill_defaults(program, defaults)
        # Skip programs that are clearly incomplete (truncated JSON)
        if not program.get("program_name") or program["program_name"] == "Unknown Program":
            print(f"[AI] Rejected incomplete program: {program.get('program_id', '?')}", flush=True)
            continue
        if not program.get("university") or program["university"] == "Unknown University":
            print(f"[AI] Rejected program without university: {program.get('program_id', '?')}", flush=True)
            continue
        if not program.get("country_id") or program["country_id"] not in valid_country_ids:
            print(f"[AI] Rejected program from invalid country: {program.get('country_id', '?')}", flush=True)
            continue
        if not program.get("program_id"):
            uni_slug = program.get("university", "unknown").lower().replace(" ", "-")[:30]
            prog_slug = program.get("program_name", "unknown").lower().replace(" ", "-")[:30]
            program["program_id"] = f"{uni_slug}-{prog_slug}"
        # Build a reliable Google search URL as primary link
        # Gemini-generated URLs are often hallucinated 404s
        uni = program.get("university", "")
        pname = program.get("program_name", "")
        search_query = f"{uni} {pname} admissions".replace(" ", "+")
        program["program_url_search"] = f"https://www.google.com/search?q={search_query}"

        # Keep Gemini URL as secondary only if it starts with http
        url = program.get("program_url", "")
        if url and not url.startswith("http"):
            url = f"https://{url}"
        program["program_url_original"] = url if url else ""

        # Use Google search as the reliable primary URL
        program["program_url"] = program["program_url_search"]

        valid_programs.append(program)

    result["programs"] = valid_programs

    for scholarship in result["scholarships"]:
        _fill_defaults(scholarship, _SCHOLARSHIP_REQUIRED)
        if not scholarship.get("scholarship_id"):
            org_slug = scholarship.get("provider_organization", "unknown").lower().replace(" ", "-")[:30]
            name_slug = scholarship.get("scholarship_name", "unknown").lower().replace(" ", "-")[:30]
            scholarship["scholarship_id"] = f"{org_slug}-{name_slug}"
        # Build reliable Google search URL for scholarship
        sname = scholarship.get("scholarship_name", "")
        org = scholarship.get("provider_organization", "")
        schol_query = f"{sname} {org} apply".replace(" ", "+")
        scholarship["scholarship_url_search"] = f"https://www.google.com/search?q={schol_query}"

        url = scholarship.get("scholarship_url", "")
        if url and not url.startswith("http"):
            url = f"https://{url}" if url else ""
        scholarship["scholarship_url_original"] = url
        scholarship["scholarship_url"] = scholarship["scholarship_url_search"]

    print(f"[AI] Post-processed: {len(result['programs'])} programs, "
          f"{len(result['countries'])} countries, "
          f"{len(result['scholarships'])} scholarships", flush=True)
    return result


# ---------------------------------------------------------------------------
# API call with retry + model fallback
# ---------------------------------------------------------------------------

def _call_gemini_with_retry(model_name: str, prompt: str, system_prompt: str) -> str:
    last_exc = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            response = _client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    max_output_tokens=32000,
                    temperature=0.2,
                ),
            )
            return response.text.strip()
        except Exception as exc:
            exc_name = type(exc).__name__
            # Check if retryable (transient errors)
            if any(keyword in exc_name.lower() or keyword in str(exc).lower()
                   for keyword in ["resource", "unavailable", "deadline", "internal", "timeout", "connection"]):
                last_exc = exc
                if attempt < _MAX_RETRIES:
                    wait = _BACKOFF_BASE ** (attempt + 1)
                    print(f"[AI] Transient error ({exc_name}), retrying in {wait}s...", flush=True)
                    time.sleep(wait)
                else:
                    print(f"[AI] Exhausted retries for {model_name}: {exc}", flush=True)
            elif "permission" in exc_name.lower() or "permission" in str(exc).lower():
                print(f"[AI] Permission denied: {exc}", flush=True)
                raise
            elif "not_found" in exc_name.lower() or "404" in str(exc):
                print(f"[AI] Model not found ({model_name}): {exc}", flush=True)
                raise
            else:
                last_exc = exc
                print(f"[AI] Error ({exc_name}): {exc}", flush=True)
                if attempt < _MAX_RETRIES:
                    wait = _BACKOFF_BASE ** (attempt + 1)
                    time.sleep(wait)
                else:
                    raise

    raise last_exc


SYSTEM_PROMPT = """You are a migration research assistant for a Peruvian applicant seeking Permanent Residency abroad via a Master's (or Bachelor's/Doctorate) degree.

You return ONLY valid JSON — no markdown, no explanation, no preamble.
All amounts in USD. Dates in YYYY-MM-DD. Keep response compact (max 6 programs total).

RESEARCH METHODOLOGY — Follow these steps IN ORDER:

STEP 1 — COUNTRIES (immigration data):
- Select 3 countries with PROVEN pathways from student visa → PR for international graduates
- Source immigration data ONLY from official government websites (domains ending in .gov, .gc.ca, .gov.uk, .gov.au, or country-code TLDs for government portals like .gob.xx)
- Required data: months to PR, study visa duration, post-study work visa, solvency requirements, work permit rules
- REJECT any country where you cannot confirm a legal student→PR pathway from government sources

STEP 2 — SCHOLARSHIPS (per country):
- For each selected country, search for scholarships from: government agencies, non-profit organizations, and universities
- CRITICAL: Verify each scholarship actually covers the specific degree type ({raw_degree}) and field related to "{keyword}"
  — Many scholarships EXCLUDE professional degrees (MBA, JD, MD)
  — Many scholarships only cover STEM or only cover humanities
  — If the scholarship page says it does not cover the degree type, DO NOT include it
- Only include scholarships where Peru or "developing countries" are explicitly eligible
- Note which universities offer these scholarships — this informs Step 3

STEP 3 — PROGRAMS (per country, informed by Steps 1-2):
- For each country, search: "{keyword}" + "{raw_degree}" + [country name]
- Prioritize universities that appeared in Step 2 (those offering scholarships)
- Also include top-ranked universities in that country for this field
- Include EXACTLY 2 programs per country
- Link each program to applicable scholarships from Step 2 via applicable_program_ids

Be conservative: use highest known cost, fewest months. Mark uncertain values in unverified_fields."""

SEARCH_PROMPT_TEMPLATE = """Research {degree_level} programs matching "{keyword}"{country_clause}.

Follow the 3-step methodology (Countries → Scholarships → Programs) and return a JSON object:

{{
  "countries": [
    {{
      "country_id": "ISO alpha-2 (e.g. DE, CA, AU)",
      "country_name": "Spanish name (e.g. Alemania, Canadá, Australia)",
      "region": "One of: Europa, América del Norte, Oceanía, Asia, América del Sur, África",
      "months_to_pr": int_or_null,
      "study_visa_months": int_or_null,
      "post_study_extension_months": int_or_null,
      "solvency_buffer_usd": float_annual_or_null,
      "work_permit_allowed": true_or_false,
      "max_hours_per_week": int_or_null,
      "embassy_in_peru": true_or_false,
      "link_pr": "government URL (.gov or official) for PR info",
      "link_study_visa": "government URL for study visa",
      "link_visa_extension": "government URL for post-study extension",
      "link_work_permit": "government URL for work permits",
      "source_urls": {{}},
      "unverified_fields": []
    }}
  ],
  "scholarships": [
    {{
      "scholarship_id": "provider-slug-name-slug",
      "scholarship_name": "Full scholarship name",
      "provider_organization": "Gobierno Federal | Gobierno Estatal | Organización Internacional | Universidad Pública | Universidad Privada",
      "candidate_type": ["Excelencia académica | Liderazgo | País en desarrollo"],
      "coverage_pct": float_0_to_100_or_null,
      "monthly_stipend_usd": float_or_null,
      "covers_mobility_expenses": true_or_false_or_null,
      "covers_medical_insurance": true_or_false_or_null,
      "application_deadline": "YYYY-MM-DD or null",
      "application_status": "No Iniciada",
      "scholarship_url": "official scholarship provider URL",
      "applicable_program_ids": ["program_ids this scholarship applies to"],
      "eligible_country_ids": ["country_ids where this scholarship is available"],
      "peru_eligible": true_or_null,
      "covers_degree_types": ["{raw_degree}"],
      "unverified_fields": []
    }}
  ],
  "programs": [
    {{
      "program_id": "university-slug-program-slug",
      "program_name": "Official program name",
      "university": "Full university name",
      "city": "City name",
      "country_id": "ISO alpha-2",
      "faculty_or_department": "Spanish name (Escuela de Negocios, Facultad de Ciencias, etc.)",
      "degree_level": "{raw_degree}",
      "duration_months": int,
      "language_of_instruction": "English or native language",
      "full_tuition_usd": float_total_for_entire_program_or_null,
      "program_url": "university official domain URL",
      "coverage_pct": float_0_to_1_if_scholarship_covers_or_null,
      "stipend_monthly_usd": float_or_null,
      "university_scholarship": "scholarship name from this university or empty string",
      "unverified_fields": []
    }}
  ]
}}

CRITICAL RULES:
- EXACTLY 2 programs per country, EXACTLY 3 countries (6 programs total)
- Scholarships MUST genuinely cover {raw_degree} programs in the "{keyword}" field — verify degree type eligibility
- Countries: ONLY those with PROVEN student visa → PR pathways (Canada, Germany, Australia, Netherlands, Sweden, New Zealand, UK, etc.). NEVER include countries without established graduate immigration pathways.
- full_tuition_usd = TOTAL cost for entire program (not per year/semester)
- solvency_buffer_usd = annual amount to prove financial solvency
- country_name, region, faculty_or_department MUST be in SPANISH
- All applicable_program_ids MUST reference actual program_ids from the programs array
- All eligible_country_ids MUST reference actual country_ids from the countries array
- If uncertain about a value, use best estimate and add field name to unverified_fields"""


def ai_search(keyword: str, degree_levels: list[str], country_ids: list[str] | None = None, lang: str = "en") -> dict | None:
  try:
    if not GEMINI_API_KEY or not _client:
        return None

    degree_str = ", ".join(degree_levels) if degree_levels else "masters"
    raw_degree = degree_levels[0] if degree_levels else "masters"

    if country_ids:
        country_clause = f" in countries: {', '.join(country_ids)}"
    else:
        country_clause = " worldwide (suggest top 3 countries with best PR pathways)"

    prompt = SEARCH_PROMPT_TEMPLATE.format(
        degree_level=degree_str,
        keyword=keyword,
        country_clause=country_clause,
        raw_degree=raw_degree,
    )

    if lang == "es":
        prompt += "\n\nIMPORTANT — SPANISH MODE: The user is searching in Spanish. These fields MUST be in Spanish: country_name (Alemania, Canadá, Suecia...), region (Europa, América del Norte...), faculty_or_department (Facultad de Ingeniería, Escuela de Negocios...). Program names and university names should remain in their ORIGINAL language. provider_organization and candidate_type are already defined in Spanish above — use those exact values."
    else:
        prompt += "\n\nNOTE: Even in English mode, country_name, region, faculty_or_department, provider_organization, and candidate_type MUST still be in SPANISH as specified in the format rules above. Only the UI labels change, not the data language."

    system = SYSTEM_PROMPT.format(raw_degree=raw_degree, keyword=keyword)
    print(f"[AI] Calling Gemini API...", flush=True)

    for model_idx, model_name in enumerate(MODEL_CANDIDATES):
        try:
            print(f"[AI] Trying model: {model_name}", flush=True)
            text = _call_gemini_with_retry(model_name, prompt, system)
        except Exception as exc:
            exc_name = type(exc).__name__
            if "permission" in exc_name.lower() or "permission" in str(exc).lower():
                print(f"[AI] Permission denied (check API key): {exc}", flush=True)
                return None
            if model_idx < len(MODEL_CANDIDATES) - 1:
                print(f"[AI] Model {model_name} failed, trying next...", flush=True)
                continue
            print(f"[AI] All models exhausted", flush=True)
            return None

        # Strip markdown fences if present
        if text.startswith("```"):
            first_newline = text.index("\n")
            text = text[first_newline + 1:]
            if text.endswith("```"):
                text = text[:-3].strip()

        print(f"[AI] Got response from {model_name}, {len(text)} chars", flush=True)

        try:
            result = json.loads(text)
        except json.JSONDecodeError:
            result = _repair_json(text)

        if not isinstance(result, dict):
            print(f"[AI] Response was not a JSON object", flush=True)
            return None

        result = _validate_and_postprocess(result, raw_degree)
        return result

    return None
  except Exception as e:
    print(f"[AI] FATAL: {type(e).__name__}: {e}", flush=True)
    return None
