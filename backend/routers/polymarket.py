"""Polymarket API routes for market scanning and tagging."""

import logging
from typing import List

from fastapi import APIRouter, HTTPException, Query

try:
    from ..config import settings
    from ..schemas import MarketNormalized, TaggerIn, TaggerOut
    from ..polymarket_client import normalize_markets_for_quarter, normalize_live_markets
    from ..services.llm_tagger import tag_markets_batch
    from ..services.supabase_repo_markets import upsert_markets
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from config import settings
    from schemas import MarketNormalized, TaggerIn, TaggerOut
    from polymarket_client import normalize_markets_for_quarter
    from services.llm_tagger import tag_markets_batch
    from services.supabase_repo_markets import upsert_markets

logger = logging.getLogger(__name__)

router = APIRouter(tags=["polymarket"])


@router.get("/polymarket/markets", response_model=List[MarketNormalized])
async def get_markets(quarter: str = Query(..., description="Quarter in format YYYY-Qn")):
    """
    Scan Polymarket for markets in the specified quarter.
    
    This endpoint:
    1. Fetches all markets from Gamma API with pagination
    2. Filters by quarter deadline
    3. Retrieves order book data from CLOB API
    4. Computes pricing and liquidity metrics
    5. Stores markets in database
    6. Returns markets filtered by minimum liquidity score
    
    The returned markets still need tagging via POST /markets/tag.
    """
    try:
        logger.info(f"Starting market scan for quarter {quarter}")
        
        # Normalize markets with pricing and liquidity
        markets = await normalize_markets_for_quarter(quarter)
        
        if not markets:
            logger.info(f"No markets found for quarter {quarter}")
            return []
        
        # Upsert to database
        await upsert_markets(markets)
        
        # Apply liquidity filter
        liquidity_threshold = float(settings.liquidity_min_score)
        filtered_markets = [
            market for market in markets 
            if market.liquidity_score >= liquidity_threshold
        ]
        
        logger.info(
            f"Market scan complete: {len(markets)} total, "
            f"{len(filtered_markets)} above liquidity threshold {liquidity_threshold}"
        )
        
        return filtered_markets
        
    except Exception as e:
        logger.error(f"Market scan failed for quarter {quarter}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/polymarket/markets/live", response_model=List[MarketNormalized])
async def get_live_markets():
    """Scan Polymarket for all live markets (deadline in the future)."""
    try:
        markets = await normalize_live_markets()
        if markets:
            await upsert_markets(markets)
        # Apply liquidity filter
        from ..config import settings as _settings
        liq = float(_settings.liquidity_min_score)
        return [m for m in markets if m.liquidity_score >= liq]
    except Exception as e:
        logger.error(f"Live market scan failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/markets/tag", response_model=List[TaggerOut])
async def post_markets_tag(markets: List[TaggerIn]):
    """
    Tag markets using LLM classification.
    
    This endpoint:
    1. Takes a list of markets to tag
    2. Uses LLM to classify rules clarity and categories
    3. Caches results in Redis and Supabase
    4. Returns tagging results
    5. Updates market records with tag information
    
    After tagging, you can filter out markets with rules_clarity == 'ambiguous'
    for your analysis workflow.
    """
    try:
        logger.info(f"Starting batch tagging for {len(markets)} markets")
        
        if not markets:
            return []
        
        # Tag markets with LLM
        results = await tag_markets_batch(markets)
        
        # Log summary statistics
        clarity_counts = {}
        category_counts = {}
        
        for result in results:
            clarity_counts[result.rules_clarity] = clarity_counts.get(result.rules_clarity, 0) + 1
            for category in result.category_tags:
                category_counts[category] = category_counts.get(category, 0) + 1
        
        high_conf_count = sum(1 for r in results if r.confidence >= 0.8)
        
        logger.info(
            f"Tagging complete: {len(results)} markets tagged, "
            f"{high_conf_count} high-confidence results. "
            f"Clarity: {clarity_counts}, Top categories: {dict(list(category_counts.items())[:5])}"
        )
        
        return results
        
    except Exception as e:
        logger.error(f"Market tagging failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/polymarket/health")
async def polymarket_health():
    """Health check for Polymarket integration."""
    try:
        # Test basic connectivity to Gamma API
        import httpx
        
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{settings.poly_base_url}/markets?limit=1")
            gamma_healthy = response.status_code == 200
            
            # Test CLOB API
            clob_response = await client.get(f"{settings.clob_base_url}")
            clob_healthy = clob_response.status_code in [200, 404]  # 404 is normal for root path
        
        return {
            "status": "healthy" if gamma_healthy and clob_healthy else "degraded",
            "gamma_api": "up" if gamma_healthy else "down",
            "clob_api": "up" if clob_healthy else "down",
            "redis_url": settings.redis_url,
            "llm_model": settings.llm_model
        }
        
    except Exception as e:
        logger.error(f"Polymarket health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "gamma_api": "unknown",
            "clob_api": "unknown"
        }
