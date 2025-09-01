"""LLM-based market tagging service with caching."""

import json
import logging
from typing import Dict, Any, Optional

import httpx
import redis.asyncio as redis

try:
    from ..config import settings
    from ..schemas import TaggerIn, TaggerOut, Category, RulesClarity
    from ..prompts import build_market_tagger_prompt
    from .supabase_repo_markets import cache_market_tag_json, update_market_tags
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from config import settings
    from schemas import TaggerIn, TaggerOut, Category, RulesClarity
    from prompts import build_market_tagger_prompt
    from services.supabase_repo_markets import cache_market_tag_json, update_market_tags

logger = logging.getLogger(__name__)


async def _get_redis():
    """Get Redis connection."""
    return await redis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)


async def _call_llm(prompt: str) -> Dict[str, Any]:
    """Call OpenRouter API for LLM inference."""
    headers = {
        "Authorization": f"Bearer {settings.openrouter_api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://astroedge.com",
        "X-Title": "AstroEdge Market Tagger"
    }
    
    payload = {
        "model": settings.llm_model,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
        "max_tokens": 500
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        
        result = response.json()
        content = result["choices"][0]["message"]["content"].strip()
        
        # Parse JSON from response
        # Handle potential markdown code blocks
        if content.startswith("```json"):
            content = content[7:]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()
        
        return json.loads(content)


def _heuristic_tagger(market: TaggerIn) -> TaggerOut:
    """Fallback heuristic tagger when LLM fails."""
    logger.info(f"Using heuristic tagger for market {market.id}")
    
    title_lower = market.title.lower()
    description_lower = (market.description or "").lower()
    rules_lower = (market.rules or "").lower()
    
    all_text = f"{title_lower} {description_lower} {rules_lower}"
    
    # Simple keyword-based categorization
    categories = []
    
    # Check for financial/market terms
    if any(term in all_text for term in ["stock", "price", "$", "market", "trading", "shares", "bitcoin", "crypto"]):
        categories.append("markets_finance")
    
    # Check for political terms
    elif any(term in all_text for term in ["election", "president", "vote", "political", "government", "congress", "senate"]):
        categories.append("geopolitics")
    
    # Check for sports terms
    elif any(term in all_text for term in ["game", "championship", "tournament", "team", "score", "match", "season"]):
        categories.append("sports")
    
    # Check for tech terms
    elif any(term in all_text for term in ["tech", "ai", "software", "app", "platform", "launch", "release"]):
        categories.append("communications_tech")
    
    # Default to public sentiment if no other category fits
    if not categories:
        categories.append("public_sentiment")
    
    # Simple rules clarity assessment
    clarity = "unclear"  # Conservative default
    if market.rules and len(market.rules) > 50:
        if any(term in rules_lower for term in ["specific", "exactly", "precisely", "by", "before", "after", "above", "below"]):
            clarity = "clear"
    
    return TaggerOut(
        market_id=market.id,
        rules_clarity=clarity,
        category_tags=categories[:2],  # Limit to 2 categories
        confidence=0.6,  # Lower confidence for heuristic
        explanation="Heuristic classification due to LLM failure"
    )


async def tag_market(market: TaggerIn) -> TaggerOut:
    """Tag a market using LLM with caching."""
    cache_key = f"tag:{market.id}:{settings.llm_model}"
    
    # Check Redis cache first
    try:
        redis = await _get_redis()
        cached = await redis.get(cache_key)
        if cached:
            logger.info(f"Using cached tag for market {market.id}")
            cached_data = json.loads(cached)
            return TaggerOut(**cached_data)
    except Exception as e:
        logger.warning(f"Redis cache error for market {market.id}: {e}")
    
    # Build prompt and call LLM
    prompt = build_market_tagger_prompt(
        title=market.title,
        description=market.description,
        rules=market.rules
    )
    
    max_retries = 2
    last_error = None
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Tagging market {market.id} with LLM (attempt {attempt + 1})")
            
            llm_response = await _call_llm(prompt)
            
            # Validate and create TaggerOut
            tagger_out = TaggerOut(
                market_id=market.id,
                rules_clarity=llm_response["rules_clarity"],
                category_tags=llm_response.get("category_tags", []),
                confidence=float(llm_response.get("confidence", 0.8)),
                explanation=llm_response.get("explanation")
            )
            
            # Cache in Redis
            try:
                await redis.setex(
                    cache_key,
                    int(settings.tag_cache_ttl_sec),
                    json.dumps(tagger_out.dict())
                )
            except Exception as e:
                logger.warning(f"Failed to cache tag for market {market.id}: {e}")
            
            # Cache in Supabase
            try:
                await cache_market_tag_json(market.id, settings.llm_model, llm_response)
                await update_market_tags(market.id, tagger_out)
            except Exception as e:
                logger.warning(f"Failed to persist tag for market {market.id}: {e}")
            
            logger.info(f"Successfully tagged market {market.id}: {tagger_out.rules_clarity}, {len(tagger_out.category_tags)} categories")
            return tagger_out
            
        except Exception as e:
            last_error = e
            logger.warning(f"LLM tagging attempt {attempt + 1} failed for market {market.id}: {e}")
            
            if attempt < max_retries - 1:
                continue
    
    # All LLM attempts failed, use heuristic fallback
    logger.error(f"All LLM attempts failed for market {market.id}: {last_error}")
    
    fallback_result = _heuristic_tagger(market)
    
    # Still try to cache and persist the heuristic result
    try:
        await redis.setex(
            cache_key,
            int(settings.tag_cache_ttl_sec),
            json.dumps(fallback_result.dict())
        )
        await update_market_tags(market.id, fallback_result)
    except Exception as e:
        logger.warning(f"Failed to persist heuristic tag for market {market.id}: {e}")
    
    return fallback_result


async def tag_markets_batch(markets: list[TaggerIn]) -> list[TaggerOut]:
    """Tag multiple markets with concurrency control."""
    import asyncio
    
    logger.info(f"Starting batch tagging for {len(markets)} markets")
    
    # Use semaphore to limit concurrency
    semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent LLM calls
    
    async def tag_with_semaphore(market: TaggerIn) -> TaggerOut:
        async with semaphore:
            return await tag_market(market)
    
    results = await asyncio.gather(
        *[tag_with_semaphore(market) for market in markets],
        return_exceptions=True
    )
    
    # Handle any exceptions
    tagged_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Failed to tag market {markets[i].id}: {result}")
            # Use heuristic fallback
            tagged_results.append(_heuristic_tagger(markets[i]))
        else:
            tagged_results.append(result)
    
    success_count = sum(1 for r in tagged_results if r.confidence > 0.7)
    logger.info(f"Batch tagging completed: {success_count}/{len(markets)} high confidence tags")
    
    return tagged_results