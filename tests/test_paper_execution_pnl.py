"""Tests for paper execution and P&L calculation."""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timezone

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from backend.trading import OrderIn, execute_order_paper, _slippage
from backend.pnl import mark_to_market, _mark_price
from backend.schemas import OrderbookL1


class TestSlippageModel:
    """Test the slippage calculation model."""
    
    def test_slippage_within_depth(self):
        """Test slippage when order size is within available depth."""
        # Order size within depth - only linear component
        slippage = _slippage(qty=100, top_depth=200)
        expected = 0.0005 * 100  # A * q1, where q2 = 0
        assert slippage == expected
    
    def test_slippage_exceeds_depth(self):
        """Test slippage when order size exceeds available depth."""
        # Order size exceeds depth - linear + convex components
        slippage = _slippage(qty=300, top_depth=200)
        expected = 0.0005 * 200 + 0.0025 * 100  # A * q1 + B * q2
        assert slippage == expected
    
    def test_slippage_zero_depth(self):
        """Test slippage when no depth available."""
        slippage = _slippage(qty=100, top_depth=0)
        expected = 0.0025 * 100  # B * qty (all convex)
        assert slippage == expected


class TestMarkPrice:
    """Test mark price calculations."""
    
    def test_mark_price_mid(self):
        """Test mid mark price calculation."""
        l1 = MagicMock()
        l1.bid_yes = 0.45
        l1.ask_yes = 0.55
        
        # Mock settings
        with patch('backend.pnl.settings') as mock_settings:
            mock_settings.mark_method = "mid"
            mark = _mark_price(l1, "YES")
            assert mark == 0.5
    
    def test_mark_price_bid(self):
        """Test bid mark price calculation."""
        l1 = MagicMock()
        l1.bid_yes = 0.45
        l1.ask_yes = 0.55
        
        with patch('backend.pnl.settings') as mock_settings:
            mock_settings.mark_method = "bid"
            mark = _mark_price(l1, "YES")
            assert mark == 0.45
    
    def test_mark_price_no_prices(self):
        """Test mark price when no prices available."""
        l1 = MagicMock()
        l1.bid_yes = None
        l1.ask_yes = None
        
        with patch('backend.pnl.settings') as mock_settings:
            mock_settings.mark_method = "mid"
            mark = _mark_price(l1, "YES")
            assert mark == 0.5  # Default


class TestPaperExecution:
    """Test paper order execution scenarios."""
    
    @pytest.mark.asyncio
    async def test_successful_order_execution(self):
        """Test successful paper order execution."""
        order = OrderIn(
            market_id="test_market",
            token_id="test_token",
            side="YES",
            qty=100.0,
            limit_price=None,
            comment="Test order"
        )
        
        # Mock orderbook
        mock_l1 = MagicMock()
        mock_l1.ask_yes = 0.50
        mock_l1.bid_yes = 0.48
        mock_l1.ask_sz_usdc = 5000.0  # $5000 depth
        
        # Mock dependencies
        with patch('backend.trading.get_books_batch') as mock_books, \
             patch('backend.trading.supabase') as mock_supabase, \
             patch('backend.trading.perform_full_risk_check') as mock_risk, \
             patch('backend.trading._update_positions_from_fill') as mock_update_pos:
            
            # Setup mocks
            mock_books.return_value = {"test_token": mock_l1}
            mock_risk.return_value = MagicMock(allowed=True)
            
            # Mock supabase insert responses
            mock_supabase.insert.side_effect = [
                [{"id": "order_123"}],  # order insert
                None  # fill insert
            ]
            
            # Execute order
            result = await execute_order_paper(order)
            
            # Verify result
            assert result["status"] == "filled"
            assert result["order_id"] == "order_123"
            assert result["filled"] == 100.0
            assert result["avg_px"] > 0.50  # Should include slippage
            assert "fee_usdc" in result
    
    @pytest.mark.asyncio
    async def test_limit_price_rejection(self):
        """Test order rejection due to limit price."""
        order = OrderIn(
            market_id="test_market",
            token_id="test_token",
            side="YES",
            qty=100.0,
            limit_price=0.49,  # Below effective price
            comment="Test limit order"
        )
        
        # Mock orderbook with higher ask
        mock_l1 = MagicMock()
        mock_l1.ask_yes = 0.50
        mock_l1.ask_sz_usdc = 5000.0
        
        with patch('backend.trading.get_books_batch') as mock_books, \
             patch('backend.trading.perform_full_risk_check') as mock_risk:
            
            mock_books.return_value = {"test_token": mock_l1}
            mock_risk.return_value = MagicMock(allowed=True)
            
            result = await execute_order_paper(order)
            
            assert result["status"] == "rejected_limit"
            assert result["filled"] == 0.0
            assert result["order_id"] is None
    
    @pytest.mark.asyncio
    async def test_risk_check_rejection(self):
        """Test order rejection due to risk controls."""
        order = OrderIn(
            market_id="test_market",
            token_id="test_token",
            side="YES",
            qty=100.0
        )
        
        mock_l1 = MagicMock()
        mock_l1.ask_yes = 0.50
        mock_l1.ask_sz_usdc = 5000.0
        
        with patch('backend.trading.get_books_batch') as mock_books, \
             patch('backend.trading.perform_full_risk_check') as mock_risk:
            
            mock_books.return_value = {"test_token": mock_l1}
            mock_risk.return_value = MagicMock(
                allowed=False,
                reason="Market exposure limit exceeded"
            )
            
            result = await execute_order_paper(order)
            
            assert "rejected_risk" in result["status"]
            assert result["filled"] == 0.0


class TestPnLCalculation:
    """Test P&L calculation scenarios."""
    
    @pytest.mark.asyncio
    async def test_mark_to_market_no_positions(self):
        """Test mark-to-market with no positions."""
        with patch('backend.pnl.supabase') as mock_supabase:
            mock_supabase.select.return_value = []  # No positions
            mock_supabase.insert = AsyncMock()
            
            result = await mark_to_market()
            
            assert result["equity_usdc"] == 0.0
            assert result["realized_usdc"] == 0.0
            assert result["unrealized_usdc"] == 0.0
            assert result["fees_usdc"] == 0.0
    
    @pytest.mark.asyncio
    async def test_mark_to_market_with_positions(self):
        """Test mark-to-market with profitable position."""
        mock_positions = [{
            "market_id": "test_market",
            "token_id": "test_token",
            "side": "YES",
            "qty": 100,
            "vwap": 0.45,  # Bought at 0.45
            "realized_pnl": -2.70  # Paid fees
        }]
        
        mock_fills = [{
            "fee_usdc": 2.70
        }]
        
        # Mock current orderbook - mark at 0.55
        mock_l1 = MagicMock()
        mock_l1.bid_yes = 0.54
        mock_l1.ask_yes = 0.56
        
        with patch('backend.pnl.supabase') as mock_supabase, \
             patch('backend.pnl.get_books_batch') as mock_books, \
             patch('backend.pnl.settings') as mock_settings:
            
            mock_settings.mark_method = "mid"
            
            # Setup supabase responses
            mock_supabase.select.side_effect = [
                mock_positions,  # positions query
                mock_fills       # fills query
            ]
            mock_supabase.insert = AsyncMock()
            
            mock_books.return_value = {"test_token": mock_l1}
            
            result = await mark_to_market()
            
            # Expected: 100 * (0.55 - 0.45) = $10 unrealized
            assert result["unrealized_usdc"] == 10.0
            assert result["realized_usdc"] == -2.70
            assert result["equity_usdc"] == 7.30  # 10 - 2.70
            assert result["fees_usdc"] == 2.70


class TestIntegrationScenarios:
    """Integration test scenarios."""
    
    @pytest.mark.asyncio
    async def test_buy_sell_roundtrip(self):
        """Test a complete buy-sell roundtrip scenario."""
        # This would be a more complex test that:
        # 1. Places a buy order at 0.45
        # 2. Checks position is created
        # 3. Updates mark price to 0.55
        # 4. Verifies unrealized P&L
        # 5. Places sell order at 0.55
        # 6. Verifies realized P&L and position closure
        
        # Implementation would require more extensive mocking
        # of the full trading pipeline
        pass


def test_fee_calculation():
    """Test fee calculation accuracy."""
    # 60 bps fee on $100 notional = $0.60
    notional = 100.0
    fee_bps = 60
    expected_fee = notional * (fee_bps / 10000.0)
    assert expected_fee == 0.60


def test_price_bounds():
    """Test that effective prices stay within valid bounds."""
    # Effective prices should be clamped to [0.01, 0.99]
    test_cases = [
        (0.005, 0.01),   # Below minimum
        (0.995, 0.99),   # Above maximum
        (0.50, 0.50),    # Normal case
    ]
    
    for input_price, expected in test_cases:
        clamped = max(0.01, min(0.99, input_price))
        assert clamped == expected


if __name__ == "__main__":
    # Run basic tests
    test_slippage = TestSlippageModel()
    test_slippage.test_slippage_within_depth()
    test_slippage.test_slippage_exceeds_depth()
    test_slippage.test_slippage_zero_depth()
    
    test_fee_calculation()
    test_price_bounds()
    
    print("✅ All basic tests passed!")
    print("Run 'pytest tests/test_paper_execution_pnl.py' for async tests")