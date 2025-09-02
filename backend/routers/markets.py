"""Markets router: upcoming, categories, quick analyze endpoints."""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
import json

from fastapi import APIRouter, HTTPException, Query

try:
    from ..supabase_client import supabase
    from ..services.quarters import parse_quarter
    from ..services.supabase_repo_opportunities import (
        fetch_contributions_for_market_quarter,
    )
    from ..polymarket_client import normalize_markets_for_quarter
    from ..services.supabase_repo_markets import upsert_markets
    from ..services.gamma import fetch_upcoming_markets, iso_z, now_utc
    from ..db.supa import upsert_markets_cache
    from ..config import settings
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
    from services.gamma import fetch_upcoming_markets, iso_z, now_utc
    from db.supa import upsert_markets_cache
    from config import settings


logger = logging.getLogger(__name__)

router = APIRouter(tags=["markets"])
api_router = APIRouter(tags=["markets"])  # to be mounted at prefix /api in main


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
    """List markets with deadline within the given quarter - LEGACY ENDPOINT.

    Optionally filter by a category tag present in `category_tags`.
    Only works if MARKETS_CACHE_MODE=supabase, otherwise returns empty list.
    """
    try:
        if settings.markets_cache_mode != "supabase":
            logger.info("markets/upcoming: MARKETS_CACHE_MODE not set to supabase, returning empty")
            return []
            
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


@api_router.get("/markets/upcoming")
async def api_upcoming(
    days: int = Query(30, ge=1, le=60),
    liquidity_min: float = Query(0.0, ge=0.0),
    limit: int = Query(250, ge=1, le=500),
):
    """Fetch open Polymarket markets resolving within `days` - STATELESS by default.
    
    Hits Gamma API directly, no Supabase calls unless MARKETS_CACHE_MODE=supabase.
    """
    import httpx
    
    def _norm_mid(m: dict):
        bb, ba, last = m.get("bestBid"), m.get("bestAsk"), m.get("lastTradePrice")
        def _fix(x):
            if x is None: return None
            x = float(x)
            return x/100.0 if x > 1.0 else x
        bb, ba, last = _fix(bb), _fix(ba), _fix(last)
        if bb is not None and ba is not None: return (bb+ba)/2.0
        return last

    now = datetime.now(timezone.utc)
    end_max = now + timedelta(days=days)
    params = {
        "closed": "false",
        "end_date_min": now.isoformat(),
        "end_date_max": end_max.isoformat(),
        "order": "endDate",
        "ascending": "true",
        "limit": str(limit),
    }
    
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(f"{settings.gamma_api_url}/markets", params=params)
            r.raise_for_status()
            items = r.json()

        out = []
        for m in items:
            liq = m.get("liquidityNum")
            if isinstance(liq, (int, float)) and liq < liquidity_min:
                continue
            mid = _norm_mid(m)
            out.append({
                "id": m["id"],
                "question": m.get("question") or m.get("title"),
                "endDate": m.get("endDate") or m.get("endDateIso"),
                "liquidityNum": liq,
                "bestBid": m.get("bestBid"),
                "bestAsk": m.get("bestAsk"),
                "lastTradePrice": m.get("lastTradePrice"),
                "p_market_mid": mid,
                "tags": [t.get("label") for t in (m.get("tags") or []) if isinstance(t, dict)],
                "category": m.get("category") or None,
                "rules_clarity": "unknown"
            })

        # OPTIONAL cache path (disabled by default; never fatal)
        if settings.markets_cache_mode == "supabase":
            try:
                cache_rows = [
                    {
                        "id": m["id"],
                        "title": m["question"],
                        "deadline_utc": m["endDate"],
                        "liquidity_num": m["liquidityNum"],
                        "price_mid": m["p_market_mid"],
                        "best_bid": m["bestBid"],
                        "best_ask": m["bestAsk"],
                        "last_trade_price": m["lastTradePrice"],
                        "tags": json.dumps(m["tags"]),
                        "category": m["category"],
                        "rules_clarity": m["rules_clarity"],
                        "fetched_at": now.isoformat(),
                    }
                    for m in out
                ]
                await upsert_markets_cache(cache_rows)
            except Exception as e:
                logger.warning("markets_cache_upsert_failed: %s", e)

        return {"now_utc": now.isoformat(), "count": len(out), "limit": limit, "markets": out}
        
    except Exception as e:
        logger.error(f"Failed /api/markets/upcoming: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/markets/categories")
async def categories(
    quarter: str = Query(...),
) -> Dict[str, int]:
    """Return grouped counts by top-level category tag for markets in quarter.
    
    Only works if MARKETS_CACHE_MODE=supabase, otherwise returns empty dict.
    """
    try:
        if settings.markets_cache_mode != "supabase":
            logger.info("markets/categories: MARKETS_CACHE_MODE not set to supabase, returning empty")
            return {}
            
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
async def analyze_now(body: Dict[str, Any]) -> Dict[str, Any]:
    """One-shot analysis for selected markets (no persistence unless CURATION_STORE_MODE=supabase).

    body: {
      markets: [{ id, title, deadline_utc, p_market, tags? }],
      params?: { lambda_gain, threshold, lambda_days, orb_limits, K_cap, fees_bps, slippage, size_cap }
    }
    """
    try:
        markets: List[Dict[str, Any]] = body.get("markets") or []
        params: Dict[str, Any] = body.get("params") or {}

        # Defaults
        lambda_gain = float(params.get("lambda_gain", 0.10))
        threshold = float(params.get("threshold", 0.04))
        fees_bps = float(params.get("fees_bps", 60))
        slippage = float(params.get("slippage", 0.005))
        size_cap = float(params.get("size_cap", 0.05))

        # Analyze market snapshots
        analyses: List[Dict[str, Any]] = []
        
        for market in markets:
            market_id = market.get("id")
            title = market.get("title") or f"Market {market_id}"
            deadline_utc = market.get("deadline_utc")
            p_market = float(market.get("p_market") or 0.5)
            tags = market.get("tags") or []
            
            if settings.curation_store_mode != "supabase":
                # Stateless mode: return minimal analysis without database lookups
                s_astro = 0.0  # No astro contributions in stateless mode
            else:
                # Database mode: fetch astro contributions for this quarter
                try:
                    quarter = "2025-Q3"  # Default quarter for now
                    contribs = await fetch_contributions_for_market_quarter(market_id, quarter)
                    s_astro = float(sum((c.get("contribution") or 0.0) for c in contribs))
                except Exception as e:
                    logger.warning(f"Failed to fetch contributions for {market_id}: {e}")
                    s_astro = 0.0
                
            # Decision math
            p_model = max(0.02, min(0.98, _sigmoid(_logit(p_market) + lambda_gain * s_astro)))
            edge_gross = abs(p_model - p_market)
            costs = (fees_bps / 10000.0) + slippage
            edge_net = edge_gross - costs
            
            # Decision logic
            decision_side = "SKIP"
            if edge_net >= threshold:
                decision_side = "YES" if p_model > p_market else "NO"
            
            # Confidence based on edge strength
            confidence = min(1.0, max(0.0, edge_net * 5))  # Scale edge to confidence
            
            size_fraction = min(size_cap, max(0.0, edge_net * 2))
            
            # Calculate LLR_ast and edge_best
            llr_ast = s_astro * 2.0 if s_astro > 0 else None  # Simple LLR approximation
            edge_best = max(abs(edge_net), 0) if edge_net is not None else None
            
            analyses.append({
                "market_id": market_id,
                "title": title,
                "deadline_utc": deadline_utc,
                "prior": {
                    "p_market": p_market
                },
                "posterior": {
                    "p_model": p_model
                },
                "decision": {
                    "side": decision_side
                },
                "confidence": confidence,
                "edges": {
                    "yes": edge_net if decision_side == "YES" else -edge_net,
                    "no": edge_net if decision_side == "NO" else -edge_net,
                    "best": edge_best
                },
                "astro": {
                    "eligible": s_astro > 0,
                    "included": s_astro > 0,
                    "S_astro": s_astro,
                    "LLR_ast": llr_ast
                },
                "flags": [
                    "astro_eligible" if s_astro > 0 else None,
                    "astro_included" if s_astro > 0 else None
                ],
                "reasons": [
                    f"Edge: {edge_net:.3f}",
                    f"Astro score: {s_astro:.3f}" if s_astro > 0 else "No astro data"
                ],
                "evidence_cards": [],
                "tags": tags,
                "size_fraction": size_fraction
            })
        
        if False:  # Legacy database mode kept for reference
            # Database mode: full analysis with market data and contributions
            for mid in market_ids:
                # Prefer canonical markets; fallback to markets_cache (from /api scan)
                rows = await supabase.select(
                    table="markets",
                    select="id,title,deadline_utc,price_yes,category_tags",
                    eq={"id": mid},
                    limit=1,
                )
                market_from_cache = False
                if not rows:
                    rows = await supabase.select(
                        table="markets_cache",
                        select="id,title,deadline_utc,price_mid as price_yes,tags as category_tags",
                        eq={"id": mid},
                        limit=1,
                    )
                    market_from_cache = True if rows else False
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
                        "tags": (m.get("category_tags") or []),
                        "p0": p0,
                        "s_astro": s_astro,
                        "p_astro": p_astro,
                        "edge_net": edge_net,
                        "decision": decision,
                        "size_fraction": size_fraction,
                    }
                )

        return {"analyses": analyses}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analyze now failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
