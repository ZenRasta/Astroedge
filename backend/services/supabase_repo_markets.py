"""Supabase repository for market operations."""

import logging
from typing import Dict, Any, List, Optional
from uuid import uuid4
from datetime import datetime

import httpx

try:
    from ..config import settings
    from ..schemas import MarketNormalized, TaggerOut
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from config import settings
    from schemas import MarketNormalized, TaggerOut

logger = logging.getLogger(__name__)


class SupabaseMarketsRepository:
    """Repository for market-related database operations."""
    
    def __init__(self):
        self.base_url = f"{settings.supabase_url}/rest/v1"
        self.headers = {
            "apikey": settings.supabase_service_role,
            "Authorization": f"Bearer {settings.supabase_service_role}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
    
    async def upsert_markets(self, markets: List[MarketNormalized]) -> int:
        """Upsert markets into the database."""
        if not markets:
            return 0
        
        logger.info(f"Upserting {len(markets)} markets to database")
        
        # Convert to database format
        db_records = []
        for market in markets:
            record = {
                "id": market.id,
                "title": market.title,
                "description": market.description,
                "rules": market.rules,
                "deadline_utc": market.deadline_utc.isoformat(),
                "price_yes": market.price_yes,
                "spread": market.spread,
                "top_depth_usdc": market.top_depth_usdc,
                "liquidity_score": market.liquidity_score,
                "rules_clarity": market.rules_clarity,
                "category_tags": market.category_tags,
                "updated_at": datetime.utcnow().isoformat()
            }
            db_records.append(record)
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/markets",
                json=db_records,
                headers={
                    **self.headers,
                    "Prefer": "resolution=merge-duplicates,return=minimal"
                },
                timeout=30.0
            )
            
            if response.status_code not in [200, 201]:
                logger.error(f"Market upsert failed: {response.status_code} {response.text}")
                response.raise_for_status()
        
        logger.info(f"Successfully upserted {len(markets)} markets")
        return len(markets)
    
    async def update_market_tags(self, market_id: str, tags: TaggerOut) -> None:
        """Update market with LLM-generated tags."""
        logger.info(f"Updating tags for market {market_id}")
        
        payload = {
            "rules_clarity": tags.rules_clarity,
            "category_tags": tags.category_tags,
            "tag_confidence": tags.confidence,
            "tag_explanation": tags.explanation,
            "updated_at": datetime.utcnow().isoformat()
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.patch(
                f"{self.base_url}/markets?id=eq.{market_id}",
                json=payload,
                headers=self.headers,
                timeout=30.0
            )
            response.raise_for_status()
        
        logger.info(f"Updated tags for market {market_id}")
    
    async def cache_market_tag_json(self, market_id: str, model: str, response: Dict[str, Any]) -> None:
        """Cache LLM tagging response in database."""
        logger.debug(f"Caching tag response for market {market_id} with model {model}")
        
        payload = {
            "id": str(uuid4()),
            "market_id": market_id,
            "model": model,
            "response_json": response,
            "cached_at": datetime.utcnow().isoformat()
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/cached_market_tags",
                json=payload,
                headers=self.headers,
                timeout=30.0
            )
            
            if response.status_code not in [200, 201]:
                logger.warning(f"Failed to cache tag response: {response.status_code}")
                # Don't raise - caching is not critical
    
    async def fetch_markets(
        self,
        quarter: Optional[str] = None,
        min_liquidity_score: Optional[float] = None,
        rules_clarity: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Fetch markets with optional filters."""
        url = f"{self.base_url}/markets"
        params = []
        
        if quarter:
            # Filter by quarter would require parsing the deadline_utc
            # For now, we'll handle this in the application layer
            pass
        
        if min_liquidity_score is not None:
            params.append(f"liquidity_score=gte.{min_liquidity_score}")
        
        if rules_clarity:
            params.append(f"rules_clarity=eq.{rules_clarity}")
        
        if limit:
            params.append(f"limit={limit}")
        
        params.append("order=liquidity_score.desc")
        
        if params:
            url += "?" + "&".join(params)
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers, timeout=30.0)
            response.raise_for_status()
            
            return response.json()
    
    async def get_market_by_id(self, market_id: str) -> Optional[Dict[str, Any]]:
        """Get a single market by ID."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/markets?id=eq.{market_id}&limit=1",
                headers=self.headers,
                timeout=30.0
            )
            response.raise_for_status()
            
            data = response.json()
            return data[0] if data else None
    
    async def get_cached_tag(self, market_id: str, model: str) -> Optional[Dict[str, Any]]:
        """Get cached LLM tag response."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/cached_market_tags?market_id=eq.{market_id}&model=eq.{model}&order=cached_at.desc&limit=1",
                headers=self.headers,
                timeout=30.0
            )
            response.raise_for_status()
            
            data = response.json()
            return data[0] if data else None


# Global repository instance
_repo: Optional[SupabaseMarketsRepository] = None


def get_repo() -> SupabaseMarketsRepository:
    """Get global repository instance."""
    global _repo
    if _repo is None:
        _repo = SupabaseMarketsRepository()
    return _repo


# Convenience functions for backward compatibility
async def upsert_markets(markets: List[MarketNormalized]) -> int:
    """Upsert markets using global repository."""
    repo = get_repo()
    return await repo.upsert_markets(markets)


async def update_market_tags(market_id: str, tags: TaggerOut) -> None:
    """Update market tags using global repository."""
    repo = get_repo()
    return await repo.update_market_tags(market_id, tags)


async def cache_market_tag_json(market_id: str, model: str, response: Dict[str, Any]) -> None:
    """Cache market tag JSON using global repository."""
    repo = get_repo()
    return await repo.cache_market_tag_json(market_id, model, response)