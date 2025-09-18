# derdata/markets/pjm/revenue.py
from __future__ import annotations

from dataclasses import dataclass
import pandas as pd

from derdata.utils.time import ensure_utc_dtindex


# ---------------- Energy -----------------------------------------------------
def energy_revenue_mwh(profile_mwh: pd.Series, price_per_mwh: pd.Series) -> float:
    """
    profile_mwh: time-indexed MWh delivered/consumed (+gen, -load) per interval
    price_per_mwh: time-indexed LMP ($/MWh) aligned to profile
    Returns total $ for the overlapping index.
    """
    pm = pd.to_numeric(profile_mwh.copy(), errors="coerce").dropna()
    pp = pd.to_numeric(price_per_mwh.copy(), errors="coerce").dropna()

    # Build explicit DatetimeIndex variables (avoid assigning to .index)
    pm_idx = ensure_utc_dtindex(pm.index)
    pp_idx = ensure_utc_dtindex(pp.index)

    idx = pm_idx.intersection(pp_idx)
    if len(idx) == 0:
        return 0.0
    return float((pm.loc[idx] * pp.loc[idx]).sum())


# ---------------- Regulation -------------------------------------------------
@dataclass
class RegulationParams:
    cleared_mw: pd.Series              # MW cleared per interval (time-indexed)
    rmccp: pd.Series                   # $/MW-h capability price per interval
    rmpcp: pd.Series | None = None     # $/ΔMW performance price per interval
    mileage_ratio: pd.Series | None = None  # unitless ratio/index
    performance_score: float = 1.0     # 0..1 score
    hours_per_interval: float = 1.0    # e.g., 1 for hourly, 0.25 for 15-min


def regulation_revenue(params: RegulationParams) -> float:
    # Align by cleared index (refine as needed)
    idx = params.cleared_mw.index
    cap = (params.cleared_mw * params.rmccp).reindex(idx, fill_value=0.0) * params.hours_per_interval

    if params.rmpcp is not None and params.mileage_ratio is not None:
        mil = (
            params.cleared_mw.reindex(idx, fill_value=0.0)
            * params.mileage_ratio.reindex(idx, fill_value=0.0)
            * params.rmpcp.reindex(idx, fill_value=0.0)
            * params.performance_score
            * params.hours_per_interval
        )
        return float(cap.sum() + mil.sum())
    return float(cap.sum())


# ---------------- Reserves (Sync/Non-sync/Primary) --------------------------
def reserve_revenue(
    cleared_mw: pd.Series,   # MW cleared per interval
    mcp_per_mw_h: pd.Series, # $/MW-h per interval
    hours_per_interval: float = 1.0,
) -> float:
    cmw = pd.to_numeric(cleared_mw.copy(), errors="coerce").dropna()
    mcp = pd.to_numeric(mcp_per_mw_h.copy(), errors="coerce").dropna()

    # Explicit DatetimeIndex variables
    cmw_idx = ensure_utc_dtindex(cmw.index)
    mcp_idx = ensure_utc_dtindex(mcp.index)

    idx = cmw_idx.intersection(mcp_idx)
    if len(idx) == 0:
        return 0.0
    return float((cmw.loc[idx] * mcp.loc[idx] * hours_per_interval).sum())


# ---------------- Capacity (RPM) --------------------------------------------
def capacity_revenue_ucap(
    ucap_mw: float,
    price_per_mw_day: float,
    days_in_month: int | float,
) -> float:
    return float(ucap_mw * price_per_mw_day * days_in_month)


# ---------------- Emergency Events ------------------------------------------
def emergency_energy_revenue(
    event_energy_mwh: pd.Series,
    rt_lmp: pd.Series,
) -> float:
    return energy_revenue_mwh(event_energy_mwh, rt_lmp)
