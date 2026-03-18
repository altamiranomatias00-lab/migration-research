"""
Viability formulas for migration pathway analysis.
Computes legal gaps, VCI scores, coverage ratios, and labor pressure indices.
"""
from __future__ import annotations
from datetime import datetime, date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scraper import Country, Program, Scholarship


def legal_gap(country: Country, program: Program | None) -> dict:
    """
    legal_gap_months = months_to_pr - (study_visa_months + post_study_extension_months)
    Positive = user must bridge with employment contract.
    Zero or negative = timeline naturally covers PR pathway.
    """
    if any(v is None for v in [
        country.months_to_pr,
        country.study_visa_months,
        country.post_study_extension_months
    ]):
        return {"legal_gap_months": None, "status": "INSUFFICIENT_DATA"}
    gap = country.months_to_pr - (
        country.study_visa_months + country.post_study_extension_months
    )
    return {
        "legal_gap_months": gap,
        "status": "VIABLE" if gap <= 0 else "GAP_EXISTS",
        "employment_months_required": max(0, gap)
    }


def viability_cost_index(
    country: Country,
    program: Program,
    best_scholarship: Scholarship | None,
    estimated_monthly_living_usd: float
) -> float | None:
    """
    VCI = net_program_cost + solvency_cost + gap_cost

    net_program_cost = full_tuition_usd * (1 - coverage_pct / 100)
    solvency_cost    = solvency_buffer_usd * study_visa_months
    gap_cost         = legal_gap_months * estimated_monthly_living_usd

    Lower VCI = more financially viable.
    """
    if program.full_tuition_usd is None:
        return None
    coverage = (best_scholarship.coverage_pct or 0) / 100 \
               if best_scholarship else 0
    net_program_cost = program.full_tuition_usd * (1 - coverage)

    # solvency_buffer_usd is an annual requirement; scale by study years
    study_years = (country.study_visa_months or 0) / 12
    solvency_cost = (country.solvency_buffer_usd or 0) * study_years

    gap = legal_gap(country, program)
    gap_months = max(0, gap.get("legal_gap_months") or 0)
    gap_cost = gap_months * estimated_monthly_living_usd

    return round(net_program_cost + solvency_cost + gap_cost, 2)


def coverage_ratio(vci: float, scholarship: Scholarship | None) -> float | None:
    """What % of VCI does the scholarship cover? >70% = self-funding alert."""
    if not scholarship or not scholarship.monthly_stipend_usd or not vci:
        return None
    stipend_total = scholarship.monthly_stipend_usd * 12
    return round((stipend_total / vci) * 100, 1)


def labor_pressure_index(country: Country) -> float | None:
    """
    gap / post_study_extension_months
    >0.7 = narrow employment window. Trigger HIGH_PRESSURE alert.
    """
    gap = legal_gap(country, None)
    gap_months = gap.get("legal_gap_months")
    if gap_months is None or not country.post_study_extension_months:
        return None
    return round(gap_months / country.post_study_extension_months, 2)


def is_within_days(deadline_str: str | None, days: int) -> bool:
    """Check if a deadline is within N days from today."""
    if not deadline_str:
        return False
    try:
        deadline = date.fromisoformat(deadline_str)
        return 0 <= (deadline - date.today()).days <= days
    except (ValueError, TypeError):
        return False


def compute_alerts(
    country: Country,
    program: Program,
    scholarship: Scholarship | None,
    lpi: float | None,
    cov_ratio: float | None
) -> list[str]:
    alerts = []
    if country.embassy_in_peru is False:
        alerts.append("NO_EMBASSY_IN_PERU")
    if scholarship and is_within_days(scholarship.application_deadline, 90):
        alerts.append("DEADLINE_URGENT")
    if lpi is not None and lpi > 0.7:
        alerts.append("HIGH_LABOR_PRESSURE")
    if cov_ratio is not None and cov_ratio > 70:
        alerts.append("SELF_FUNDING_VIABLE")
    return alerts
