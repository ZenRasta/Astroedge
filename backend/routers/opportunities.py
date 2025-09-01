"""Opportunities router for read-only access to scan results."""

import logging
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional, Dict, Any

try:
    from ..services.supabase_repo_opportunities import (
        fetch_opportunities_for_quarter,
        fetch_opportunity_with_market,
        fetch_contributions_for_market_quarter,
    )
    from ..schemas import OpportunityOut
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from services.supabase_repo_opportunities import (
        fetch_opportunities_for_quarter,
        fetch_opportunity_with_market,
        fetch_contributions_for_market_quarter,
    )
    from schemas import OpportunityOut

logger = logging.getLogger(__name__)

router = APIRouter(tags=["opportunities"])


@router.get("/opportunities/quarter", response_model=List[Dict[str, Any]])
async def get_opportunities_for_quarter(quarter: str, limit: int = Query(50, ge=1, le=200)):
    """Get opportunities for a specific quarter with market details."""
    try:
        opportunities = await fetch_opportunities_for_quarter(quarter, limit=limit)
        logger.info(f"Retrieved {len(opportunities)} opportunities for quarter {quarter}")
        return opportunities
    except Exception as e:
        logger.error(f"Error fetching opportunities for quarter {quarter}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/opportunities/{opp_id}")
async def get_opportunity_detail(opp_id: str, quarter: Optional[str] = None):
    """
    Returns opportunity row (joined with markets info) and contributions table
    filtered to the given quarter (if provided).
    """
    try:
        opp = await fetch_opportunity_with_market(opp_id)
        if not opp:
            raise HTTPException(status_code=404, detail="Opportunity not found")
        
        market_id = opp["market_id"]
        contribs = await fetch_contributions_for_market_quarter(market_id, quarter)
        
        logger.info(f"Retrieved opportunity {opp_id} with {len(contribs)} contributions")
        return {"opportunity": opp, "contributions": contribs}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching opportunity detail {opp_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))