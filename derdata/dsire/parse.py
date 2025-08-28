# derdata/dsire/parse.py
from __future__ import annotations

import gzip
import json
import re
from html import unescape
from pathlib import Path
from typing import Any, Iterable, List, Optional, Tuple

import pandas as pd
from dateutil import parser as dateparser
from pydantic import BaseModel, Field  # pip install pydantic>=2.7


# ----------------- pydantic models -----------------
class ProgramRow(BaseModel):
    program_id: int | str
    program_code: Optional[str] = None
    program_name: Optional[str] = None
    state: Optional[str] = None
    administrator: Optional[str] = None
    implementing_sector_name: Optional[str] = None
    category_name: Optional[str] = None
    type_name: Optional[str] = None
    website_url: Optional[str] = None
    funding_source: Optional[str] = None
    budget_text: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    last_updated: Optional[str] = None
    technologies: Optional[str] = None
    technology_categories: Optional[str] = None
    sectors: Optional[str] = None
    utilities: Optional[str] = None
    utilities_eia_ids: Optional[str] = None
    incentive_text: Optional[str] = None
    max_incentive_text: Optional[str] = None
    equipment_requirements: Optional[str] = None
    installation_requirements: Optional[str] = None
    eligibility_text: Optional[str] = None
    rec_ownership_text: Optional[str] = None


class ParameterRow(BaseModel):
    program_id: int | str
    source: str = Field(..., description="ProgramParameters or DerivedFromDetails")
    tech: Optional[str] = None
    sector: Optional[str] = None
    qualifier: Optional[str] = None  # e.g., min, max, base, cap
    amount: float
    units: str  # $/kWh, $/kW, %, USD, $/W, etc.
    notes: Optional[str] = None


# ----------------- helpers -----------------
_AMT_KW = re.compile(r"\$([\d,]+(?:\.\d+)?)\s*/\s*kW\b", re.I)
_AMT_KWH = re.compile(r"\$([\d,]+(?:\.\d+)?)\s*/\s*kWh\b", re.I)
_AMT_W = re.compile(r"\$([\d,]+(?:\.\d+)?)\s*/\s*W\b", re.I)
_AMT_PCT = re.compile(r"(\d{1,3}(?:\.\d+)?)\s*%\s*(?:of|towards|rebate|incentive|credit)?", re.I)
_AMT_CAP = re.compile(r"(?:up to|maximum(?: incentive)?|cap)\s*\$([\d,]+(?:\.\d+)?)", re.I)


def _strip_html(s: Optional[str]) -> Optional[str]:
    if not isinstance(s, str) or not s.strip():
        return None
    s = s.replace("<br />", "\n").replace("<br/>", "\n").replace("<br>", "\n")
    s = re.sub(r"<[^>]+>", "", s)
    return unescape(s).strip() or None


def _join_unique(items: Iterable[Optional[str]]) -> Optional[str]:
    seen: List[str] = []
    for x in items:
        if x and x not in seen:
            seen.append(x)
    return "; ".join(seen) if seen else None


def _parse_date(s: Optional[str]) -> Optional[str]:
    if not s or not isinstance(s, str):
        return None
    try:
        return dateparser.parse(s).date().isoformat()
    except Exception:
        return None


def _extract_amounts_any(text: Optional[str]) -> List[dict[str, Any]]:
    if not text:
        return []
    hits: List[dict[str, Any]] = []
    for pat, units in ((_AMT_KW, "$/kW"), (_AMT_KWH, "$/kWh"), (_AMT_W, "$/W")):
        for m in pat.finditer(text):
            hits.append(
                {"amount": float(m.group(1).replace(",", "")), "units": units, "source": "DerivedFromDetails", "notes": text}
            )
    for m in _AMT_PCT.finditer(text):
        hits.append({"amount": float(m.group(1)), "units": "%", "source": "DerivedFromDetails", "notes": text})
    for m in _AMT_CAP.finditer(text):
        hits.append(
            {
                "amount": float(m.group(1).replace(",", "")),
                "units": "USD",
                "qualifier": "cap",
                "source": "DerivedFromDetails",
                "notes": text,
            }
        )
    return hits


def _unwrap(obj: Any) -> List[dict[str, Any]]:
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for k in ("Programs", "results", "data", "items"):
            if k in obj and isinstance(obj[k], list):
                return obj[k]
        return [obj]
    return []


# ----------------- raw loading -----------------
def load_raw_dir(version_tag: str, project_root: Path) -> List[dict[str, Any]]:
    base = project_root / "data" / "raw" / "dsire" / version_tag
    recs: List[dict[str, Any]] = []
    if not base.exists():
        return recs
    for fp in sorted(base.glob("dsire_programs_*.json.gz")):
        with gzip.open(fp, "rt", encoding="utf-8") as f:
            obj = json.load(f)
        recs.extend(_unwrap(obj))
    return recs


# ----------------- build two tables -----------------
def build_tables(version_tag: str, project_root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    records = load_raw_dir(version_tag, project_root)

    prog_rows: List[ProgramRow] = []
    param_rows: List[ParameterRow] = []

    for r in records:
        pid = r.get("ProgramId") or r.get("ProgramID") or r.get("Id")
        if pid is None:
            continue

        techs = r.get("Technologies") or []
        sectors = r.get("Sectors") or []
        utils_ = r.get("Utilities") or []
        dets = r.get("Details") or []
        params = r.get("ProgramParameters") or []

        tech_names = _join_unique([t.get("name") for t in techs])
        tech_cats = _join_unique([t.get("category") for t in techs])
        sector_names = _join_unique([s.get("name") for s in sectors])
        util_names = _join_unique([u.get("name") for u in utils_])
        util_eia = _join_unique([str(u.get("EIA_id")) for u in utils_ if u.get("EIA_id")])

        det_map: dict[str, str] = {}
        for d in dets:
            label = (d.get("label") or "").strip()
            if not label:
                continue
            txt = _strip_html(d.get("value"))
            if txt:
                det_map[label] = txt

        incentive_text = det_map.get("Incentive Amount") or det_map.get("Incentive") or det_map.get("Benefit Details")
        max_incentive_tx = det_map.get("Maximum Incentive")
        equipment_req = det_map.get("Equipment Requirements")
        installation_req = det_map.get("Installation Requirements")
        eligibility_txt = det_map.get("Eligibility") or det_map.get("Eligibility Requirements")
        rec_ownership = det_map.get("Ownership of Renewable Energy Credits")

        prog_rows.append(
            ProgramRow(
                program_id=pid,
                program_code=r.get("Code"),
                program_name=r.get("Name"),
                state=r.get("State"),
                administrator=r.get("Administrator") or r.get("ImplementingSectorName"),
                implementing_sector_name=r.get("ImplementingSectorName"),
                category_name=r.get("CategoryName"),
                type_name=r.get("TypeName"),
                website_url=r.get("WebsiteUrl") or r.get("ProgramURL") or r.get("Website"),
                funding_source=r.get("FundingSource"),
                budget_text=r.get("Budget"),
                start_date=_parse_date(r.get("StartDate") or r.get("EffectiveDate")),
                end_date=_parse_date(r.get("EndDate") or r.get("ExpirationDate")),
                last_updated=_parse_date(r.get("LastUpdate") or r.get("LastUpdated")),
                technologies=tech_names,
                technology_categories=tech_cats,
                sectors=sector_names,
                utilities=util_names,
                utilities_eia_ids=util_eia,
                incentive_text=incentive_text,
                max_incentive_text=max_incentive_tx,
                equipment_requirements=equipment_req,
                installation_requirements=installation_req,
                eligibility_text=eligibility_txt,
                rec_ownership_text=rec_ownership,
            )
        )

        # Structured (preferred)
        for pp in params:
            tech_scope = [t.get("name") for t in (pp.get("technologies") or [])]
            sector_scope = [s.get("name") for s in (pp.get("sectors") or [])]
            for p in pp.get("parameters") or []:
                amount = p.get("amount")
                units = p.get("units")
                if amount is None or units is None:
                    continue
                try:
                    amt = float(amount)
                except Exception:
                    continue
                param_rows.append(
                    ParameterRow(
                        program_id=pid,
                        source="ProgramParameters",
                        tech=_join_unique(tech_scope),
                        sector=_join_unique(sector_scope),
                        qualifier=p.get("qualifier"),
                        amount=amt,
                        units=str(units),
                        notes=None,
                    )
                )

        # Derived (narrative)
        for hit in _extract_amounts_any(incentive_text):
            param_rows.append(
                ParameterRow(
                    program_id=pid,
                    source=str(hit.get("source")),
                    tech=None,
                    sector=None,
                    qualifier=hit.get("qualifier"),  # type: ignore[arg-type]
                    amount=float(hit["amount"]),
                    units=str(hit["units"]),
                    notes=str(hit.get("notes") or ""),
                )
            )
        for hit in _extract_amounts_any(max_incentive_tx):
            param_rows.append(
                ParameterRow(
                    program_id=pid,
                    source=str(hit.get("source")),
                    tech=None,
                    sector=None,
                    qualifier=str(hit.get("qualifier") or "cap"),
                    amount=float(hit["amount"]),
                    units=str(hit.get("units") or "USD"),
                    notes=str(hit.get("notes") or ""),
                )
            )

    programs_df = pd.DataFrame([r.model_dump() for r in prog_rows]).sort_values(
        ["state", "program_name"], na_position="last"
    ).reset_index(drop=True)
    parameters_df = pd.DataFrame([r.model_dump() for r in param_rows]).reset_index(drop=True)
    return programs_df, parameters_df


def write_processed(version_tag: str, project_root: Path, fmt: str = "parquet") -> tuple[Path, Path]:
    programs_df, parameters_df = build_tables(version_tag, project_root)
    out_dir = project_root / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)

    if fmt.lower() == "csv":
        p1 = out_dir / f"programs_{version_tag}.csv"
        p2 = out_dir / f"parameters_{version_tag}.csv"
        programs_df.to_csv(p1, index=False)
        parameters_df.to_csv(p2, index=False)
    else:
        p1 = out_dir / f"programs_{version_tag}.parquet"
        p2 = out_dir / f"parameters_{version_tag}.parquet"
        programs_df.to_parquet(p1, index=False)
        parameters_df.to_parquet(p2, index=False)

    return p1, p2
