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

    for country in result["countries"]:
        _fill_defaults(country, _COUNTRY_REQUIRED)

    for program in result["programs"]:
        defaults = {**_PROGRAM_REQUIRED, "degree_level": degree_level}
        _fill_defaults(program, defaults)
        if not program.get("program_id"):
            uni_slug = program.get("university", "unknown").lower().replace(" ", "-")[:30]
            prog_slug = program.get("program_name", "unknown").lower().replace(" ", "-")[:30]
            program["program_id"] = f"{uni_slug}-{prog_slug}"

    for scholarship in result["scholarships"]:
        _fill_defaults(scholarship, _SCHOLARSHIP_REQUIRED)
        if not scholarship.get("scholarship_id"):
            org_slug = scholarship.get("provider_organization", "unknown").lower().replace(" ", "-")[:30]
            name_slug = scholarship.get("scholarship_name", "unknown").lower().replace(" ", "-")[:30]
            scholarship["scholarship_id"] = f"{org_slug}-{name_slug}"

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


SYSTEM_PROMPT = """You are a migration research assistant specializing in international education pathways for a Peruvian applicant seeking Permanent Residency abroad via a Master's (or Bachelor's or Doctorate) degree.

You return ONLY valid JSON — no markdown fences, no explanation, no preamble.

All financial amounts must be in USD. All dates in YYYY-MM-DD format.
Only include REAL programs from REAL universities with data you are confident about.
For any value you are uncertain about, add the field name to that object's "unverified_fields" array and use your best estimate or null.

Be conservative: use highest known cost, fewest months. Include 2-3 programs per country."""

SEARCH_PROMPT_TEMPLATE = """Research {degree_level} programs matching "{keyword}"{country_clause}.

Return a JSON object with THREE relational tables matching this exact structure:

{{
  "countries": [
    {{
      "country_id": "ISO alpha-2 code (e.g. SE, US, GB, CA)",
      "country_name": "Full name in Spanish (e.g. Suecia, Estados Unidos, Reino Unido)",
      "region": "One of: Europa, América del Norte, Oceanía, Asia, América del Sur, África",
      "months_to_pr": int_or_null,
      "study_visa_months": int_or_null,
      "post_study_extension_months": int_or_null,
      "solvency_buffer_usd": float_annual_or_null,
      "work_permit_allowed": true_or_false,
      "max_hours_per_week": int_or_null,
      "embassy_in_peru": true_or_false,
      "link_pr": "URL to permanent residency info page",
      "link_study_visa": "URL to study visa application page",
      "link_visa_extension": "URL to post-study visa extension page",
      "link_work_permit": "URL to work permit info page",
      "source_urls": {{}},
      "unverified_fields": []
    }}
  ],
  "programs": [
    {{
      "program_id": "university-slug-program-slug (lowercase, hyphenated)",
      "program_name": "Official program name",
      "university": "Full university name",
      "city": "City name",
      "country_id": "ISO alpha-2",
      "faculty_or_department": "Faculty or School name (e.g. Escuela de Negocios, Facultad de Ciencias)",
      "degree_level": "{raw_degree}",
      "duration_months": int,
      "language_of_instruction": "English or native language",
      "full_tuition_usd": float_total_cost_without_scholarship_or_null,
      "program_url": "Direct URL to the program page",
      "coverage_pct": float_0_to_1_if_org_scholarship_covers_or_null,
      "stipend_monthly_usd": float_monthly_living_stipend_or_null,
      "university_scholarship": "Name of university's own scholarship if available, or empty string",
      "unverified_fields": []
    }}
  ],
  "scholarships": [
    {{
      "scholarship_id": "provider-slug-name-slug (lowercase, hyphenated)",
      "scholarship_name": "Full scholarship name",
      "provider_organization": "One of: Gobierno Federal, Gobierno Estatal, Organización Internacional, Universidad Pública, Universidad Privada",
      "candidate_type": ["One or more of: Excelencia académica, Liderazgo, País en desarrollo"],
      "coverage_pct": float_0_to_100_or_null,
      "monthly_stipend_usd": float_or_null,
      "covers_mobility_expenses": true_or_false_or_null,
      "covers_medical_insurance": true_or_false_or_null,
      "application_deadline": "YYYY-MM-DD or date range string or null",
      "application_status": "No Iniciada",
      "scholarship_url": "Direct URL to scholarship application page",
      "applicable_program_ids": ["program_id references from programs array"],
      "eligible_country_ids": ["ISO alpha-2 codes of countries this scholarship applies to"],
      "peru_eligible": true_or_null,
      "unverified_fields": []
    }}
  ]
}}

MANDATORY RULES (NEVER SKIP):
- Include 2-3 programs per country
- For each program, include at least 1 applicable scholarship if one exists
- Only include scholarships where Peru is eligible (or likely eligible as a developing country)
- Focus on well-ranked, reputable universities

DATA QUALITY — ALL FIELDS MANDATORY:
- EVERY country MUST have: country_id, country_name, region, months_to_pr, study_visa_months, post_study_extension_months, solvency_buffer_usd, work_permit_allowed, max_hours_per_week, embassy_in_peru, link_pr, link_study_visa, link_visa_extension, link_work_permit
- EVERY program MUST have: program_id, program_name, university, city, country_id, faculty_or_department, duration_months, language_of_instruction, full_tuition_usd (total cost for full program, NOT per semester), program_url (real working URL)
- EVERY scholarship MUST have: scholarship_id, scholarship_name, provider_organization, candidate_type, coverage_pct, monthly_stipend_usd, covers_mobility_expenses, covers_medical_insurance, application_deadline, scholarship_url (real working URL), applicable_program_ids, eligible_country_ids, peru_eligible
- ALL URLs must be real, official, working URLs from the actual institution/organization websites
- ALL financial data must be in USD and reflect current/latest available figures
- full_tuition_usd = TOTAL cost for the entire program duration (not annual, not per semester)
- solvency_buffer_usd = annual amount required to prove financial solvency
- If you don't know a value with certainty, use your best estimate and add the field to unverified_fields. NEVER leave a field as null if you can estimate it.

FORMAT RULES:
- country_name must be in SPANISH (Suecia, not Sweden)
- provider_organization must be one of: Gobierno Federal, Gobierno Estatal, Organización Internacional, Universidad Pública, Universidad Privada
- candidate_type values must be from: Excelencia académica, Liderazgo, País en desarrollo
- faculty_or_department must be in SPANISH (Escuela de Negocios, Facultad de Ingeniería, etc.)
- All applicable_program_ids MUST reference actual program_ids from the programs array
- All eligible_country_ids MUST reference actual country_ids from the countries array"""


def ai_search(keyword: str, degree_levels: list[str], country_ids: list[str] | None = None, lang: str = "en") -> dict | None:
    if not GEMINI_API_KEY or not _client:
        return None

    degree_str = ", ".join(degree_levels) if degree_levels else "masters"
    raw_degree = degree_levels[0] if degree_levels else "masters"

    if country_ids:
        country_clause = f" in countries: {', '.join(country_ids)}"
    else:
        country_clause = " worldwide (suggest top 3-4 countries with best PR pathways)"

    prompt = SEARCH_PROMPT_TEMPLATE.format(
        degree_level=degree_str,
        keyword=keyword,
        country_clause=country_clause,
        raw_degree=raw_degree,
    )

    if lang == "es":
        prompt += "\n\nIMPORTANT: The user is searching in Spanish. Return country_name and faculty_or_department in Spanish. Program names should remain in their original language (usually English)."

    print(f"[AI] Calling Gemini API...", flush=True)

    for model_idx, model_name in enumerate(MODEL_CANDIDATES):
        try:
            print(f"[AI] Trying model: {model_name}", flush=True)
            text = _call_gemini_with_retry(model_name, prompt, SYSTEM_PROMPT)
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
