"""Gamma (Polymarket) service helpers.

Builds Gamma API requests and normalizes market fields.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx

try:
    from ..config import settings  # type: ignore
except Exception:  # pragma: no cover
    settings = None  # Fallback to raw env vars


def _env(name: str, default: Any) -> Any:
    v = os.getenv(name)
    if v is not None:
        return v
    if settings is not None:
        # Map to existing settings when applicable
        if name == "GAMMA_API_URL":
            return getattr(settings, "poly_base_url", default)
        if name == "HTTP_TIMEOUT_SECONDS":
            return getattr(settings, "poly_timeout_s", default)
    return default


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _norm_price(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    # If Gamma returns cents, convert to 0-1
    if v > 1.0:
        v = v / 100.0
    # Clamp to [0,1]
    v = max(0.0, min(1.0, v))
    return round(v, 6)


def _mid_from(bid: Optional[float], ask: Optional[float], last: Optional[float]) -> Optional[float]:
    b = _norm_price(bid) if bid is not None else None
    a = _norm_price(ask) if ask is not None else None
    if b is not None and a is not None and a >= b:
        return _norm_price((b + a) / 2.0)
    if last is not None:
        return _norm_price(last)
    if b is not None:
        # Use bid + small tick as a fallback mid
        return _norm_price(min(1.0, b + 0.01))
    if a is not None:
        return _norm_price(max(0.0, a - 0.01))
    return None


async def fetch_upcoming_markets(
    days: int = 30,
    liquidity_min: float = 0.0,
    limit: int = 250,
) -> List[Dict[str, Any]]:
    """Fetch open markets resolving within the specified window.

    - Filters: closed=false, end_date_min <= endDate <= end_date_max
    - Sorted ascending by endDate
    - Normalizes price fields to [0,1]
    """
    base_url = _env("GAMMA_API_URL", "https://gamma-api.polymarket.com")
    timeout_s = float(_env("HTTP_TIMEOUT_SECONDS", 20))

    start = now_utc()
    end = start + timedelta(days=max(0, int(days)))

    params = {
        "closed": "false",
        "end_date_min": iso_z(start),
        "end_date_max": iso_z(end),
        "order": "endDate",
        "ascending": "true",
        "limit": str(int(limit)),
    }
    # Gamma supports liquidity_num_min
    if liquidity_min is not None:
        try:
            params["liquidity_num_min"] = str(float(liquidity_min))
        except Exception:
            pass

    async with httpx.AsyncClient(base_url=base_url, timeout=timeout_s) as client:
        resp = await client.get("/markets", params=params)
        resp.raise_for_status()
        data = resp.json()

    # Gamma may return a list or an object with data
    raw_markets: List[Dict[str, Any]]
    if isinstance(data, list):
        raw_markets = data
    else:
        raw_markets = data.get("data", [])

    out: List[Dict[str, Any]] = []
    for m in raw_markets:
        try:
            end_iso = m.get("endDate") or m.get("end_date")
            if not end_iso:
                continue
            # Build fields
            bid = m.get("bestBid")
            ask = m.get("bestAsk")
            last = m.get("lastTradePrice")
            mid = _mid_from(bid, ask, last)

            out.append(
                {
                    "id": m.get("id"),
                    "title": m.get("question") or m.get("title") or "",
                    "deadline_utc": end_iso,
                    "liquidity_num": float(m.get("liquidityNum") or 0.0),
                    "price_mid": mid,
                    "best_bid": _norm_price(bid),
                    "best_ask": _norm_price(ask),
                    "last_trade_price": _norm_price(last),
                    "tags": m.get("categoryTags") or m.get("tags") or [],
                    "category": m.get("category") or None,
                }
            )
        except Exception:
            # Skip malformed rows
            continue

    # Filter None mids and sort by deadline
    out = [r for r in out if r.get("price_mid") is not None]
    out.sort(key=lambda r: r.get("deadline_utc") or "")
    return out

