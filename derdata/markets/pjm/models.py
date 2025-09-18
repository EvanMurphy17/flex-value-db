# derdata/markets/pjm/models.py
from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field


# ---- Energy ----------------------------------------------------------------
class EnergyInterval(BaseModel):
    ts: str                       # UTC timestamp or interval start (ISO 8601)
    market: str                   # "DA" or "RT"
    node: str                     # pricing node/hub
    lmp: float                    # $/MWh
    congestion: Optional[float] = None
    loss: Optional[float] = None


# ---- Regulation -------------------------------------------------------------
class RegulationPrice(BaseModel):
    ts: str                       # interval start UTC
    rmccp: float = Field(..., description="Regulation Market Capability Clearing Price $/MW-h")
    rmpcp: Optional[float] = Field(None, description="Regulation Market Performance (mileage) Clearing Price $/ΔMW")
    mileage_ratio: Optional[float] = Field(None, description="Mileage ratio / index used in settlements")
    product: Optional[str] = Field(None, description="REG-A/REG-D/Unified if provided")


# ---- Reserves ---------------------------------------------------------------
class ReservePrice(BaseModel):
    ts: str                       # interval start UTC
    product: str                  # "sync", "non-sync", "primary", etc.
    mcp: float                    # clearing price $/MW-h


# ---- Capacity (RPM) ---------------------------------------------------------
class CapacityPrice(BaseModel):
    delivery_year: int            # e.g., 2025/26 DY
    zone: str                     # PJM zone
    price_per_mw_day: float       # $/MW-day
    product: str = "Capacity Performance"


# ---- Emergency Events -------------------------------------------------------
class EmergencyEvent(BaseModel):
    start_ts: str
    end_ts: str
    zone: Optional[str] = None
    reason: Optional[str] = None
