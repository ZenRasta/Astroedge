"""Supabase repository for opportunities data access."""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

try:
    from ..supabase_client import supabase
    from .quarters import parse_quarter
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from supabase_client import supabase
    from services.quarters import parse_quarter

logger = logging.getLogger(__name__)


async def fetch_opportunities_for_quarter(quarter: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Fetch opportunities for a quarter with market details."""
    try:
        # Fetch opportunities first
        opportunities = await supabase.select(
            table="opportunities",
            select="id,market_id,p0,s_astro,p_astro,edge_net,size_fraction,decision,created_at",
            filters={"quarter": quarter}
        )
        
        if not opportunities:
            logger.info(f"No opportunities found for quarter {quarter}")
            return []
        
        # Sort by edge_net descending and limit
        sorted_opps = sorted(opportunities, key=lambda x: x.get("edge_net", 0), reverse=True)[:limit]
        
        # Fetch market details for each opportunity
        flattened = []
        for opp in sorted_opps:
            market_id = opp.get("market_id")
            if market_id:
                try:
                    markets = await supabase.select(
                        table="markets",
                        select="id,title,deadline_utc,rules_clarity,liquidity_score",
                        filters={"id": market_id}
                    )
                    market = markets[0] if markets else {}
                except Exception as e:
                    logger.warning(f"Failed to fetch market {market_id}: {e}")
                    market = {}
            else:
                market = {}
            
            flattened.append({
                **opp,
                "title": market.get("title", "Unknown Market"),
                "deadline_utc": market.get("deadline_utc"),
                "market_rules_clarity": market.get("rules_clarity"),
                "market_liquidity_score": market.get("liquidity_score")
            })
        
        logger.info(f"Fetched {len(flattened)} opportunities for quarter {quarter}")
        return flattened
        
    except Exception as e:
        logger.error(f"Error fetching opportunities for quarter {quarter}: {e}")
        raise


async def fetch_opportunity_with_market(opp_id: str) -> Optional[Dict[str, Any]]:
    """Fetch a single opportunity with market details."""
    try:
        # Fetch the opportunity
        opportunities = await supabase.select(
            table="opportunities",
            select="*",
            filters={"id": opp_id}
        )
        
        if not opportunities:
            return None
            
        opportunity = opportunities[0]
        market_id = opportunity.get("market_id")
        
        # Fetch the associated market
        if market_id:
            try:
                markets = await supabase.select(
                    table="markets",
                    select="id,title,deadline_utc,rules_clarity,liquidity_score,description",
                    filters={"id": market_id}
                )
                market = markets[0] if markets else {}
                opportunity["markets"] = market
            except Exception as e:
                logger.warning(f"Failed to fetch market {market_id} for opportunity {opp_id}: {e}")
                opportunity["markets"] = {}
        else:
            opportunity["markets"] = {}
            
        logger.info(f"Fetched opportunity {opp_id} with market details")
        return opportunity
        
    except Exception as e:
        logger.error(f"Error fetching opportunity {opp_id}: {e}")
        return None


async def fetch_contributions_for_market_quarter(
    market_id: str, 
    quarter: Optional[str]
) -> List[Dict[str, Any]]:
    """Fetch aspect contributions for a market, optionally filtered by quarter."""
    try:
        # Fetch aspect contributions for the market
        contributions = await supabase.select(
            table="aspect_contributions",
            select="*",
            filters={"market_id": market_id}
        )
        
        if not contributions:
            logger.info(f"No contributions found for market {market_id}")
            return []
        
        # Fetch associated aspect events and filter by quarter if needed
        enriched_contributions = []
        for contrib in contributions:
            aspect_event_id = contrib.get("aspect_event_id")
            if aspect_event_id:
                try:
                    aspect_events = await supabase.select(
                        table="aspect_events",
                        select="id,planet1,planet2,aspect,peak_utc,orb_deg,severity,is_eclipse",
                        filters={"id": aspect_event_id}
                    )
                    if aspect_events:
                        aspect_event = aspect_events[0]
                        
                        # Filter by quarter if provided
                        if quarter:
                            q_start, q_end = parse_quarter(quarter)
                            peak_utc = datetime.fromisoformat(aspect_event["peak_utc"].replace('Z', '+00:00'))
                            if not (q_start <= peak_utc < q_end):
                                continue  # Skip this contribution
                        
                        contrib["aspect_events"] = aspect_event
                        enriched_contributions.append(contrib)
                except Exception as e:
                    logger.warning(f"Failed to fetch aspect event {aspect_event_id}: {e}")
                    continue
        
        # Sort by peak_utc
        enriched_contributions.sort(key=lambda c: c.get("aspect_events", {}).get("peak_utc", ""))
        
        logger.info(f"Fetched {len(enriched_contributions)} contributions for market {market_id}")
        return enriched_contributions
        
    except Exception as e:
        logger.error(f"Error fetching contributions for market {market_id}: {e}")
        return []