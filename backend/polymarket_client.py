"""Polymarket API client for Gamma and CLOB integration."""

import json
import logging
from typing import AsyncIterator, Dict, Any, List, Optional
from datetime import datetime

import httpx
import redis.asyncio as redis

try:
    from .config import settings
    from .schemas import MarketRaw, OrderbookL1, MarketNormalized
    from .services.quarters import parse_quarter
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from config import settings
    from schemas import MarketRaw, OrderbookL1, MarketNormalized
    from services.quarters import parse_quarter

logger = logging.getLogger(__name__)


async def _get_redis():
    """Get Redis connection."""
    return await redis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)


async def _fetch_markets_page(next_cursor: Optional[str] = None, limit: int = 200) -> Dict[str, Any]:
    """Fetch a single page of markets from Gamma API."""
    params = {"limit": limit}
    if next_cursor:
        params["next_cursor"] = next_cursor
    
    async with httpx.AsyncClient(base_url=settings.poly_base_url, timeout=float(settings.poly_timeout_s)) as client:
        response = await client.get("/markets", params=params)
        response.raise_for_status()
        return response.json()


async def iter_gamma_markets(limit_per_page: int = 200) -> AsyncIterator[Dict[str, Any]]:
    """Iterate through all markets from Gamma API with pagination."""
    cursor = None
    page_count = 0
    total_markets = 0
    
    while True:
        try:
            response = await _fetch_markets_page(cursor, limit_per_page)
            page_count += 1
            
            # Gamma API returns direct array of markets, not {data: [], next_cursor: ""}
            if isinstance(response, list):
                markets = response
                cursor = None  # No pagination info in array response
            else:
                # Handle object response with data/cursor fields
                markets = response.get("data", [])
                cursor = response.get("next_cursor")
            
            total_markets += len(markets)
            
            logger.info(f"Fetched page {page_count} with {len(markets)} markets (total: {total_markets})")
            
            for market in markets:
                yield market
            
            # Stop if we got fewer markets than requested (last page) or no cursor
            if len(markets) < limit_per_page or not cursor or cursor == "LTE=":
                logger.info(f"Completed market iteration: {page_count} pages, {total_markets} total markets")
                break
                
        except Exception as e:
            logger.error(f"Error fetching markets page {page_count + 1}: {e}")
            raise


def _normalize_market(market_data: Dict[str, Any]) -> MarketRaw:
    """Convert raw Gamma market data to MarketRaw model."""
    # Field names from actual Gamma API: id, question, description, endDate, clobTokenIds
    end_date = market_data.get("endDate") or market_data.get("end_date_iso") or market_data.get("end_date")
    if not end_date:
        raise ValueError(f"Market {market_data.get('id')} missing end_date")
    
    # Handle timezone info
    if end_date.endswith('Z'):
        end_date = end_date.replace('Z', '+00:00')
    
    # Extract tokens from clobTokenIds - these are the actual token IDs we need
    tokens = []
    if market_data.get("clobTokenIds"):
        try:
            import json
            token_ids = json.loads(market_data["clobTokenIds"])
            # Create tokens with outcomes based on typical binary market structure
            if len(token_ids) == 2:
                tokens = [
                    {"outcome": "Yes", "token_id": token_ids[0]},
                    {"outcome": "No", "token_id": token_ids[1]}
                ]
        except (json.JSONDecodeError, IndexError):
            logger.warning(f"Failed to parse clobTokenIds for market {market_data.get('id')}")
    
    return MarketRaw(
        id=market_data["id"],
        title=market_data.get("question", ""),
        description=market_data.get("description"),
        rules=market_data.get("description"),  # Use description as rules if rules not available
        deadline_utc=datetime.fromisoformat(end_date),
        tokens=tokens
    )


def yes_token_id(market: MarketRaw) -> Optional[str]:
    """Extract YES token ID from market tokens."""
    if not market.tokens:
        logger.warning(f"Market {market.id} has no tokens")
        return None
    
    # Look for "Yes" outcome (case-insensitive)
    for token in market.tokens:
        outcome = str(token.get("outcome", "")).lower()
        if outcome == "yes":
            return token.get("token_id")
    
    # Fallback to first token if no "Yes" found
    logger.warning(f"Market {market.id} has no YES token, using first token")
    return market.tokens[0].get("token_id") if market.tokens else None


def _l1_from_book(book: Dict[str, Any]) -> OrderbookL1:
    """Convert CLOB book response to OrderbookL1 model."""
    if not book:
        return OrderbookL1()
    
    bids = sorted(book.get("bids", []), key=lambda x: float(x["price"]), reverse=True)
    asks = sorted(book.get("asks", []), key=lambda x: float(x["price"]))
    
    bid = bids[0] if bids else None
    ask = asks[0] if asks else None
    
    return OrderbookL1(
        bid_yes=float(bid["price"]) if bid else None,
        ask_yes=float(ask["price"]) if ask else None,
        bid_sz_usdc=float(bid["size"]) if bid else 0.0,
        ask_sz_usdc=float(ask["size"]) if ask else 0.0,
    )


async def get_books_batch(token_ids: List[str]) -> Dict[str, OrderbookL1]:
    """Fetch order books for multiple token IDs with Redis caching."""
    if not token_ids:
        return {}
    
    redis = await _get_redis()
    fresh: Dict[str, OrderbookL1] = {}
    pending: List[str] = []
    
    # Check Redis cache first
    for token_id in token_ids:
        cache_key = f"pm:ob:{token_id}"
        cached = await redis.get(cache_key)
        if cached:
            try:
                fresh[token_id] = OrderbookL1(**json.loads(cached))
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"Invalid cached data for {token_id}: {e}")
                pending.append(token_id)
        else:
            pending.append(token_id)
    
    logger.info(f"Order books: {len(fresh)} from cache, {len(pending)} to fetch")
    
    # Fetch missing books from CLOB API
    if pending:
        try:
            payload = {"params": [{"token_id": tid} for tid in pending]}
            
            async with httpx.AsyncClient(base_url=settings.clob_base_url, timeout=float(settings.poly_timeout_s)) as client:
                response = await client.post("/books", json=payload)
                response.raise_for_status()
                books = response.json()  # list aligned with params order
            
            logger.info(f"Fetched {len(books)} order books from CLOB API")
            
            # Process and cache responses
            for token_id, book in zip(pending, books):
                l1 = _l1_from_book(book or {})
                
                # Cache in Redis
                await redis.setex(
                    f"pm:ob:{token_id}", 
                    int(settings.orderbook_cache_ttl_sec), 
                    json.dumps(l1.dict())
                )
                
                fresh[token_id] = l1
                
        except Exception as e:
            logger.error(f"Error fetching books batch: {e}")
            # Return cached results only
            
    return fresh


def mid_from_l1(l1: OrderbookL1) -> float:
    """Calculate mid price from Level 1 order book."""
    if l1.bid_yes is not None and l1.ask_yes is not None and l1.ask_yes >= l1.bid_yes:
        return (l1.bid_yes + l1.ask_yes) / 2
    if l1.bid_yes is not None:
        return min(1.0, l1.bid_yes + 0.01)
    if l1.ask_yes is not None:
        return max(0.0, l1.ask_yes - 0.01)
    return 0.5


def spread_from_l1(l1: OrderbookL1) -> float:
    """Calculate spread from Level 1 order book."""
    if l1.bid_yes is None or l1.ask_yes is None:
        return 0.02
    return max(0.0, l1.ask_yes - l1.bid_yes)


def liquidity_score(spread: float, depth_usdc: float) -> float:
    """Calculate liquidity score based on spread and depth."""
    spread_wide = float(settings.liquidity_spread_wide)
    depth_max = float(settings.liquidity_depth_max_usdc)
    
    # Normalize spread (0 = best, 1 = wide)
    spread_norm = min(spread / spread_wide, 1.0)
    
    # Normalize depth (0 = no depth, 1 = max depth)
    depth_norm = min(depth_usdc / depth_max, 1.0)
    
    # Weighted score: 60% spread, 40% depth
    score = 0.6 * (1.0 - spread_norm) + 0.4 * depth_norm
    
    return round(score, 3)


async def normalize_markets_for_quarter(quarter: str) -> List[MarketNormalized]:
    """Normalize markets for a specific quarter with pricing and liquidity metrics."""
    logger.info(f"Starting market normalization for quarter {quarter}")
    
    q_start, q_end = parse_quarter(quarter)
    
    # Step 1: Gather Gamma markets in quarter
    gamma_markets = []
    async for market_data in iter_gamma_markets():
        try:
            market = _normalize_market(market_data)
            if q_start <= market.deadline_utc < q_end:
                gamma_markets.append(market)
        except Exception as e:
            logger.warning(f"Failed to normalize market {market_data.get('id')}: {e}")
            continue
    
    logger.info(f"Found {len(gamma_markets)} markets in quarter {quarter}")
    
    if not gamma_markets:
        return []
    
    # Step 2: Batch YES token_ids → /books
    token_ids = []
    for market in gamma_markets:
        token_id = yes_token_id(market)
        if token_id:
            token_ids.append(token_id)
    
    logger.info(f"Fetching order books for {len(token_ids)} tokens")
    
    # Chunk token IDs for batching
    chunk_size = int(settings.books_batch)
    books: Dict[str, OrderbookL1] = {}
    
    for i in range(0, len(token_ids), chunk_size):
        chunk = token_ids[i:i + chunk_size]
        chunk_books = await get_books_batch(chunk)
        books.update(chunk_books)
    
    # Step 3: Compute metrics and create normalized markets
    normalized_markets: List[MarketNormalized] = []
    
    for market in gamma_markets:
        token_id = yes_token_id(market)
        l1 = books.get(token_id) if token_id else OrderbookL1()
        
        mid = mid_from_l1(l1)
        spread = spread_from_l1(l1)
        depth = min(l1.bid_sz_usdc, l1.ask_sz_usdc)
        liq_score = liquidity_score(spread, depth)
        
        normalized_market = MarketNormalized(
            id=market.id,
            title=market.title,
            description=market.description,
            rules=market.rules,
            deadline_utc=market.deadline_utc,
            price_yes=mid,
            spread=spread,
            top_depth_usdc=depth,
            liquidity_score=liq_score
        )
        
        normalized_markets.append(normalized_market)
    
    logger.info(f"Normalized {len(normalized_markets)} markets with liquidity metrics")
    
    return normalized_markets