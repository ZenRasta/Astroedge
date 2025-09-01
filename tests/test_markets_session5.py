"""Tests for Polymarket integration and market tagging (Session 5)."""

import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

from backend.polymarket_client import (
    mid_from_l1, spread_from_l1, liquidity_score, yes_token_id, 
    _l1_from_book, _normalize_market
)
from backend.schemas import OrderbookL1, MarketRaw, TaggerIn, TaggerOut
from backend.services.llm_tagger import _heuristic_tagger, tag_market
from backend.prompts import build_market_tagger_prompt


class TestOrderbookMath:
    """Test orderbook pricing and liquidity calculations."""
    
    def test_mid_spread_depth_liquidity(self):
        """Test mid, spread, depth and liquidity score calculations."""
        l1 = OrderbookL1(bid_yes=0.12, ask_yes=0.14, bid_sz_usdc=800, ask_sz_usdc=600)
        
        # Test mid price
        assert mid_from_l1(l1) == 0.13
        
        # Test spread
        assert spread_from_l1(l1) == 0.02
        
        # Test liquidity score
        depth = min(l1.bid_sz_usdc, l1.ask_sz_usdc)  # 600
        score = liquidity_score(0.02, depth)
        assert 0 <= score <= 1
        
        # With default settings: spread_wide=0.05, depth_max=5000
        # spread_norm = 0.02/0.05 = 0.4
        # depth_norm = 600/5000 = 0.12
        # score = 0.6 * (1-0.4) + 0.4 * 0.12 = 0.36 + 0.048 = 0.408
        assert abs(score - 0.408) < 0.01
    
    def test_mid_edge_cases(self):
        """Test mid price calculation edge cases."""
        # Only bid available
        l1_bid_only = OrderbookL1(bid_yes=0.75, ask_yes=None)
        assert mid_from_l1(l1_bid_only) == min(1.0, 0.75 + 0.01)
        
        # Only ask available
        l1_ask_only = OrderbookL1(bid_yes=None, ask_yes=0.25)
        assert mid_from_l1(l1_ask_only) == max(0.0, 0.25 - 0.01)
        
        # No prices available
        l1_empty = OrderbookL1()
        assert mid_from_l1(l1_empty) == 0.5
        
        # Invalid spread (ask < bid)
        l1_inverted = OrderbookL1(bid_yes=0.60, ask_yes=0.55)
        # Should still calculate mid
        mid = mid_from_l1(l1_inverted)
        assert mid == (0.60 + 0.55) / 2
    
    def test_spread_edge_cases(self):
        """Test spread calculation edge cases."""
        # Missing prices
        l1_no_bid = OrderbookL1(bid_yes=None, ask_yes=0.50)
        assert spread_from_l1(l1_no_bid) == 0.02  # default
        
        l1_no_ask = OrderbookL1(bid_yes=0.50, ask_yes=None)
        assert spread_from_l1(l1_no_ask) == 0.02  # default
        
        # Zero spread
        l1_zero = OrderbookL1(bid_yes=0.50, ask_yes=0.50)
        assert spread_from_l1(l1_zero) == 0.0


class TestYesTokenSelection:
    """Test YES token ID extraction from market data."""
    
    def test_yes_token_found(self):
        """Test successful YES token extraction."""
        market = MarketRaw(
            id="test_market",
            title="Test Market",
            deadline_utc=datetime.now(timezone.utc),
            tokens=[
                {"outcome": "No", "token_id": "token_no"},
                {"outcome": "Yes", "token_id": "token_yes"}
            ]
        )
        
        token_id = yes_token_id(market)
        assert token_id == "token_yes"
    
    def test_yes_token_case_insensitive(self):
        """Test YES token extraction is case insensitive."""
        market = MarketRaw(
            id="test_market",
            title="Test Market", 
            deadline_utc=datetime.now(timezone.utc),
            tokens=[
                {"outcome": "NO", "token_id": "token_no"},
                {"outcome": "yes", "token_id": "token_yes_lower"}
            ]
        )
        
        token_id = yes_token_id(market)
        assert token_id == "token_yes_lower"
    
    def test_yes_token_fallback(self):
        """Test fallback to first token when no YES found."""
        market = MarketRaw(
            id="test_market",
            title="Test Market",
            deadline_utc=datetime.now(timezone.utc),
            tokens=[
                {"outcome": "Option A", "token_id": "token_a"},
                {"outcome": "Option B", "token_id": "token_b"}
            ]
        )
        
        token_id = yes_token_id(market)
        assert token_id == "token_a"  # First token
    
    def test_yes_token_no_tokens(self):
        """Test handling of markets with no tokens."""
        market = MarketRaw(
            id="test_market",
            title="Test Market",
            deadline_utc=datetime.now(timezone.utc),
            tokens=[]
        )
        
        token_id = yes_token_id(market)
        assert token_id is None


class TestOrderbookConversion:
    """Test CLOB book response to L1 conversion."""
    
    def test_l1_from_book_normal(self):
        """Test normal order book conversion."""
        book = {
            "bids": [
                {"price": "0.12", "size": "800.0"},
                {"price": "0.11", "size": "500.0"}
            ],
            "asks": [
                {"price": "0.14", "size": "600.0"},
                {"price": "0.15", "size": "400.0"}
            ]
        }
        
        l1 = _l1_from_book(book)
        
        assert l1.bid_yes == 0.12  # Best (highest) bid
        assert l1.ask_yes == 0.14  # Best (lowest) ask
        assert l1.bid_sz_usdc == 800.0
        assert l1.ask_sz_usdc == 600.0
    
    def test_l1_from_book_empty(self):
        """Test empty order book conversion."""
        l1 = _l1_from_book({})
        
        assert l1.bid_yes is None
        assert l1.ask_yes is None
        assert l1.bid_sz_usdc == 0.0
        assert l1.ask_sz_usdc == 0.0
    
    def test_l1_from_book_missing_sides(self):
        """Test order book with missing bid or ask side."""
        book_no_bids = {
            "bids": [],
            "asks": [{"price": "0.14", "size": "600.0"}]
        }
        
        l1 = _l1_from_book(book_no_bids)
        assert l1.bid_yes is None
        assert l1.ask_yes == 0.14


class TestMarketNormalization:
    """Test Gamma market data normalization."""
    
    def test_normalize_market_complete(self):
        """Test complete market normalization."""
        market_data = {
            "id": "market_123",
            "question": "Will it rain tomorrow?",
            "description": "Weather prediction market",
            "rules": "Resolves YES if precipitation > 0.1 inches",
            "end_date_iso": "2024-12-31T23:59:59Z",
            "tokens": [
                {"outcome": "Yes", "token_id": "yes_token"},
                {"outcome": "No", "token_id": "no_token"}
            ]
        }
        
        market = _normalize_market(market_data)
        
        assert market.id == "market_123"
        assert market.title == "Will it rain tomorrow?"
        assert market.description == "Weather prediction market"
        assert market.rules == "Resolves YES if precipitation > 0.1 inches"
        assert market.deadline_utc.year == 2024
        assert len(market.tokens) == 2
    
    def test_normalize_market_minimal(self):
        """Test market normalization with minimal data."""
        market_data = {
            "id": "minimal_market",
            "end_date": "2024-06-15T12:00:00+00:00",
        }
        
        market = _normalize_market(market_data)
        
        assert market.id == "minimal_market"
        assert market.title == ""  # Empty fallback
        assert market.description is None
        assert market.rules is None
        assert market.tokens == []
    
    def test_normalize_market_missing_date(self):
        """Test error handling for missing end date."""
        market_data = {
            "id": "no_date_market"
        }
        
        with pytest.raises(ValueError, match="missing end_date"):
            _normalize_market(market_data)


class TestGammaPagination:
    """Test Gamma API pagination handling."""
    
    @pytest.mark.asyncio
    async def test_gamma_pagination_flow(self, monkeypatch):
        """Test pagination through multiple pages."""
        from backend.polymarket_client import iter_gamma_markets
        
        # Mock responses for two pages
        page1 = {
            "data": [
                {"id": "market_1", "end_date_iso": "2024-12-31T23:59:59Z"},
                {"id": "market_2", "end_date_iso": "2024-11-30T23:59:59Z"}
            ],
            "next_cursor": "cursor_page_2"
        }
        
        page2 = {
            "data": [
                {"id": "market_3", "end_date_iso": "2024-10-31T23:59:59Z"}
            ],
            "next_cursor": "LTE="  # End sentinel
        }
        
        call_count = 0
        
        async def mock_fetch_page(cursor, limit):
            nonlocal call_count
            call_count += 1
            
            if call_count == 1:
                assert cursor is None  # First call
                return page1
            elif call_count == 2:
                assert cursor == "cursor_page_2"
                return page2
            else:
                raise AssertionError("Too many calls")
        
        monkeypatch.setattr("backend.polymarket_client._fetch_markets_page", mock_fetch_page)
        
        # Collect all markets
        markets = []
        async for market in iter_gamma_markets():
            markets.append(market)
        
        assert len(markets) == 3
        assert markets[0]["id"] == "market_1"
        assert markets[1]["id"] == "market_2"
        assert markets[2]["id"] == "market_3"
        assert call_count == 2


class TestCLOBBooksBatch:
    """Test CLOB API batch book fetching."""
    
    @pytest.mark.asyncio
    async def test_books_batch_with_cache(self, monkeypatch):
        """Test batch book fetching with Redis cache."""
        from backend.polymarket_client import get_books_batch
        
        # Mock Redis
        mock_redis = AsyncMock()
        
        # First token cached, second not cached
        mock_redis.get.side_effect = [
            json.dumps({"bid_yes": 0.50, "ask_yes": 0.52, "bid_sz_usdc": 1000, "ask_sz_usdc": 800}),
            None  # Not cached
        ]
        
        monkeypatch.setattr("backend.polymarket_client._get_redis", AsyncMock(return_value=mock_redis))
        
        # Mock CLOB API response for uncached token
        mock_clob_response = [
            # Response for second token only (first was cached)
            {
                "bids": [{"price": "0.60", "size": "1200"}],
                "asks": [{"price": "0.62", "size": "900"}]
            }
        ]
        
        mock_post = AsyncMock()
        mock_post.json.return_value = mock_clob_response
        mock_post.raise_for_status = MagicMock()
        
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.post.return_value = mock_post
        
        with patch('httpx.AsyncClient', return_value=mock_client):
            books = await get_books_batch(["token_1", "token_2"])
        
        assert len(books) == 2
        
        # First token from cache
        assert books["token_1"].bid_yes == 0.50
        assert books["token_1"].ask_yes == 0.52
        
        # Second token from API
        assert books["token_2"].bid_yes == 0.60
        assert books["token_2"].ask_yes == 0.62
        
        # Verify cache set was called for uncached token
        mock_redis.setex.assert_called_once()


class TestLLMTagger:
    """Test LLM market tagging functionality."""
    
    def test_heuristic_tagger_financial(self):
        """Test heuristic tagger for financial markets."""
        market = TaggerIn(
            id="fin_market",
            title="Will Tesla stock close above $200 by year end?",
            description="Stock price prediction market",
            rules="Resolves YES if TSLA closes above $200 on last trading day"
        )
        
        result = _heuristic_tagger(market)
        
        assert result.market_id == "fin_market"
        assert "markets_finance" in result.category_tags
        assert result.confidence == 0.6  # Heuristic confidence
        assert result.rules_clarity == "clear"  # Has detailed rules
    
    def test_heuristic_tagger_political(self):
        """Test heuristic tagger for political markets."""
        market = TaggerIn(
            id="pol_market", 
            title="Who will win the 2024 presidential election?",
            description="Election prediction market",
            rules="Short rules"
        )
        
        result = _heuristic_tagger(market)
        
        assert "geopolitics" in result.category_tags
        assert result.rules_clarity == "unclear"  # Short rules
    
    def test_heuristic_tagger_default(self):
        """Test heuristic tagger fallback to default category."""
        market = TaggerIn(
            id="unknown_market",
            title="Will something happen?",
            description="Generic market",
            rules=""
        )
        
        result = _heuristic_tagger(market)
        
        assert "public_sentiment" in result.category_tags  # Default
        assert len(result.category_tags) <= 2
    
    @pytest.mark.asyncio
    async def test_tag_market_with_cache(self, monkeypatch):
        """Test market tagging with Redis cache hit."""
        from backend.services.llm_tagger import tag_market
        
        cached_result = {
            "market_id": "cached_market",
            "rules_clarity": "clear",
            "category_tags": ["markets_finance"],
            "confidence": 0.95,
            "explanation": "Cached result"
        }
        
        # Mock Redis cache hit
        mock_redis = AsyncMock()
        mock_redis.get.return_value = json.dumps(cached_result)
        
        monkeypatch.setattr("backend.services.llm_tagger._get_redis", AsyncMock(return_value=mock_redis))
        
        market = TaggerIn(
            id="cached_market",
            title="Test market",
            description="Test description"
        )
        
        result = await tag_market(market)
        
        assert result.market_id == "cached_market"
        assert result.rules_clarity == "clear"
        assert result.category_tags == ["markets_finance"]
        assert result.confidence == 0.95
    
    @pytest.mark.asyncio
    async def test_tag_market_llm_failure_fallback(self, monkeypatch):
        """Test LLM failure with heuristic fallback."""
        from backend.services.llm_tagger import tag_market
        
        # Mock Redis miss
        mock_redis = AsyncMock()
        mock_redis.get.return_value = None
        monkeypatch.setattr("backend.services.llm_tagger._get_redis", AsyncMock(return_value=mock_redis))
        
        # Mock LLM failure
        async def mock_call_llm(prompt):
            raise Exception("LLM API error")
        
        monkeypatch.setattr("backend.services.llm_tagger._call_llm", mock_call_llm)
        
        # Mock Supabase functions to avoid actual DB calls
        monkeypatch.setattr("backend.services.llm_tagger.cache_market_tag_json", AsyncMock())
        monkeypatch.setattr("backend.services.llm_tagger.update_market_tags", AsyncMock())
        
        market = TaggerIn(
            id="failed_market",
            title="Will Bitcoin reach $100k?",
            description="Crypto market prediction"
        )
        
        result = await tag_market(market)
        
        # Should use heuristic fallback
        assert result.market_id == "failed_market"
        assert "markets_finance" in result.category_tags  # From heuristic
        assert result.confidence == 0.6  # Heuristic confidence
        assert "Heuristic classification" in result.explanation


class TestPrompts:
    """Test LLM prompt generation."""
    
    def test_market_tagger_prompt_complete(self):
        """Test prompt building with complete market data."""
        prompt = build_market_tagger_prompt(
            title="Will Tesla stock close above $200 on Dec 31?",
            description="Tesla stock price prediction market",
            rules="Resolves YES if TSLA closes above $200 on last trading day of 2024"
        )
        
        assert "Will Tesla stock close above $200 on Dec 31?" in prompt
        assert "Tesla stock price prediction market" in prompt
        assert "Resolves YES if TSLA closes above $200" in prompt
        assert "return only the JSON response" in prompt.lower()
    
    def test_market_tagger_prompt_minimal(self):
        """Test prompt building with minimal data."""
        prompt = build_market_tagger_prompt(
            title="Simple market",
            description=None,
            rules=None
        )
        
        assert "Simple market" in prompt
        assert "N/A" in prompt  # For missing fields


if __name__ == "__main__":
    pytest.main([__file__, "-v"])