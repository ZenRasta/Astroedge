"""Markets router: upcoming, categories, quick analyze endpoints."""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

try:
    from ..supabase_client import supabase
    from ..services.quarters import parse_quarter
    from ..services.supabase_repo_opportunities import (
        fetch_contributions_for_market_quarter,
    )
    from ..polymarket_client import normalize_markets_for_quarter
    from ..services.supabase_repo_markets import upsert_markets
except ImportError:  # pragma: no cover - local run convenience
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from supabase_client import supabase
    from services.quarters import parse_quarter
    from services.supabase_repo_opportunities import (
        fetch_contributions_for_market_quarter,
    )
    from polymarket_client import normalize_markets_for_quarter
    from services.supabase_repo_markets import upsert_markets


logger = logging.getLogger(__name__)

router = APIRouter(tags=["markets"])


def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


@router.get("/markets/upcoming")
async def upcoming(
    quarter: str = Query(...),
    category: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
) -> List[Dict[str, Any]]:
    """List markets with deadline within the given quarter.

    Optionally filter by a category tag present in `category_tags`.
    """
    try:
        q_start, q_end = parse_quarter(quarter)

        markets = await supabase.select(
            table="markets",
            select="id,title,deadline_utc,price_yes,liquidity_score,category_tags,rules_clarity",
        )

        out: List[Dict[str, Any]] = []
        for m in markets or []:
            d = _parse_ts(m.get("deadline_utc"))
            if not d or not (q_start <= d < q_end):
                continue

            tags = m.get("category_tags") or []
            if category and category not in tags:
                continue

            out.append(
                {
                    "id": m.get("id"),
                    "title": m.get("title"),
                    "deadline_utc": m.get("deadline_utc"),
                    "price_yes_mid": m.get("price_yes"),
                    "tags": tags,
                    "liquidity_score": m.get("liquidity_score"),
                    "rules_clarity": m.get("rules_clarity"),
                }
            )

        # Fallback: if no DB markets for this quarter, scan Polymarket live
        if not out:
            try:
                scanned = await normalize_markets_for_quarter(quarter)
                if not scanned:
                    # If no quarter-limited markets, pull all live markets
                    from ..polymarket_client import normalize_live_markets
                    scanned = await normalize_live_markets()

                if scanned:
                    # Persist normalized markets for reuse
                    await upsert_markets(scanned)
                    for m in scanned:
                        # Always return; if quarter filter doesn't match, still include (user requested all live)
                        d = m.deadline_utc if isinstance(m.deadline_utc, datetime) else _parse_ts(str(m.deadline_utc))
                        out.append(
                            {
                                "id": m.id,
                                "title": m.title,
                                "deadline_utc": m.deadline_utc.isoformat() if isinstance(m.deadline_utc, datetime) else str(m.deadline_utc),
                                "price_yes_mid": m.price_yes,
                                "tags": m.category_tags or [],
                                "liquidity_score": m.liquidity_score,
                                "rules_clarity": m.rules_clarity,
                            }
                        )
            except Exception as e:
                logger.warning(f"Polymarket scan fallback failed: {e}")

        out.sort(key=lambda x: x.get("deadline_utc") or "")
        return out[:limit]
    except Exception as e:
        logger.error(f"Failed fetching upcoming markets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/markets/categories")
async def categories(
    quarter: str = Query(...),
) -> Dict[str, int]:
    """Return grouped counts by top-level category tag for markets in quarter."""
    try:
        q_start, q_end = parse_quarter(quarter)
        markets = await supabase.select(
            table="markets",
            select="deadline_utc,category_tags",
        )

        counts: Dict[str, int] = {}
        for m in markets or []:
            d = _parse_ts(m.get("deadline_utc"))
            if not d or not (q_start <= d < q_end):
                continue
            for tag in m.get("category_tags") or []:
                counts[tag] = counts.get(tag, 0) + 1

        # Return sorted by count desc
        return dict(sorted(counts.items(), key=lambda kv: kv[1], reverse=True))
    except Exception as e:
        logger.error(f"Failed fetching categories: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _logit(p: float) -> float:
    p = max(0.001, min(0.999, float(p or 0)))
    return __import__("math").log(p / (1 - p))


def _sigmoid(x: float) -> float:
    import math
    return 1.0 / (1.0 + math.exp(-x))


@router.post("/markets/analyze")
async def analyze_now(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    """One-shot analysis for selected markets (no persistence).

    body: {
      quarter: "YYYY-Qn",
      market_ids: ["..."],
      params: { lambda_gain, threshold, lambda_days, orb_limits, K_cap, fees_bps, slippage, size_cap }
    }
    """
    try:
        quarter = (body.get("quarter") or "").strip()
        market_ids: List[str] = body.get("market_ids") or []
        params: Dict[str, Any] = body.get("params") or {}

        # Defaults
        lambda_gain = float(params.get("lambda_gain", 0.10))
        threshold = float(params.get("threshold", 0.04))
        fees_bps = float(params.get("fees_bps", 60))
        slippage = float(params.get("slippage", 0.005))
        size_cap = float(params.get("size_cap", 0.05))

        # Fetch market snapshots
        results: List[Dict[str, Any]] = []
        for mid in market_ids:
            rows = await supabase.select(
                table="markets",
                select="id,title,deadline_utc,price_yes,category_tags",
                eq={"id": mid},
                limit=1,
            )
            if not rows:
                continue

            m = rows[0]
            p0 = float(m.get("price_yes") or 0.5)

            # s_astro from contributions in given quarter
            contribs = await fetch_contributions_for_market_quarter(mid, quarter)
            s_astro = float(sum((c.get("contribution") or 0.0) for c in contribs))

            # Decision math
            p_astro = max(0.02, min(0.98, _sigmoid(_logit(p0) + lambda_gain * s_astro)))
            edge_gross = abs(p_astro - p0)
            costs = (fees_bps / 10000.0) + slippage
            edge_net = edge_gross - costs

            decision = "HOLD"
            if edge_net >= threshold:
                decision = "BUY" if p_astro > p0 else "SELL"

            size_fraction = min(size_cap, max(0.0, edge_net * 2))

            results.append(
                {
                    "market_id": mid,
                    "title": m.get("title"),
                    "deadline_utc": m.get("deadline_utc"),
                    "tags": m.get("category_tags") or [],
                    "p0": p0,
                    "s_astro": s_astro,
                    "p_astro": p_astro,
                    "edge_net": edge_net,
                    "decision": decision,
                    "size_fraction": size_fraction,
                }
            )

        return results
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analyze now failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
