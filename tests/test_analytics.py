"""Tests for analytics and backtesting functionality."""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timezone
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from backend.services.analytics import calculate_portfolio_kpis, get_trade_scatter_data, get_daily_pnl_series
from backend.services.backtest import BacktestConfig, BacktestEngine
from backend.services.calculation_breakdown import get_opportunity_calculation_breakdown


class TestAnalyticsService:
    """Test analytics service functionality."""
    
    @pytest.mark.asyncio
    async def test_portfolio_kpis_no_data(self):
        """Test KPI calculation with no trading data."""
        with patch('backend.services.analytics._get_live_fills', return_value=[]):
            with patch('backend.services.analytics.get_equity_curve', return_value=[]):
                with patch('backend.services.analytics._get_live_positions_count', return_value=0):
                    kpis = await calculate_portfolio_kpis()
                    
                    assert kpis.total_trades == 0
                    assert kpis.total_return == 0.0
                    assert kpis.sharpe_ratio == 0.0
    
    @pytest.mark.asyncio
    async def test_portfolio_kpis_with_data(self):
        """Test KPI calculation with sample trading data."""
        mock_trades = [
            {
                "pnl": 10.0,
                "fees": 1.0,
                "qty": 100,
                "price": 0.50,
                "entry_time": "2024-01-01T10:00:00Z",
                "exit_time": "2024-01-02T10:00:00Z"
            },
            {
                "pnl": -5.0,
                "fees": 1.0,
                "qty": 200,
                "price": 0.60,
                "entry_time": "2024-01-03T10:00:00Z",
                "exit_time": "2024-01-04T10:00:00Z"
            }
        ]
        
        mock_equity = [
            {"ts": "2024-01-01T00:00:00Z", "equity_usdc": 1000.0},
            {"ts": "2024-01-02T00:00:00Z", "equity_usdc": 1010.0},
            {"ts": "2024-01-03T00:00:00Z", "equity_usdc": 1005.0}
        ]
        
        with patch('backend.services.analytics._get_live_fills', return_value=mock_trades):
            with patch('backend.services.analytics.get_equity_curve', return_value=mock_equity):
                with patch('backend.services.analytics._get_live_positions_count', return_value=1):
                    kpis = await calculate_portfolio_kpis()
                    
                    assert kpis.total_trades == 2
                    assert kpis.win_rate == 0.5  # 1 win out of 2 trades
                    assert kpis.total_fees == 2.0
                    assert kpis.current_positions == 1
    
    @pytest.mark.asyncio
    async def test_trade_scatter_data(self):
        """Test trade scatter data generation."""
        mock_trades = [
            {
                "pnl": 10.0,
                "market_id": "test_market",
                "side": "YES",
                "entry_time": "2024-01-01T10:00:00Z",
                "exit_time": "2024-01-01T12:00:00Z",
                "price": 0.50,
                "qty": 100
            }
        ]
        
        with patch('backend.services.analytics._get_live_fills', return_value=mock_trades):
            scatter_data = await get_trade_scatter_data()
            
            assert len(scatter_data) == 1
            assert scatter_data[0]["hold_time_hours"] == 2.0
            assert scatter_data[0]["pnl"] == 10.0
            assert scatter_data[0]["market_id"] == "test_market"


class TestBacktestEngine:
    """Test backtesting functionality."""
    
    def test_backtest_config_creation(self):
        """Test backtest configuration."""
        config_data = {
            "start_date": "2024-01-01T00:00:00Z",
            "end_date": "2024-12-31T23:59:59Z",
            "initial_capital": 1000.0,
            "lambda_gain": 0.10,
            "threshold": 0.04
        }
        
        config = BacktestConfig(config_data)
        
        assert config.initial_capital == 1000.0
        assert config.lambda_gain == 0.10
        assert config.threshold == 0.04
        assert config.scan_frequency == "daily"  # default
    
    def test_scan_schedule_generation(self):
        """Test scan schedule generation."""
        config = BacktestConfig({
            "start_date": "2024-01-01T00:00:00Z",
            "end_date": "2024-01-03T00:00:00Z",
            "scan_frequency": "daily"
        })
        
        engine = BacktestEngine(config)
        dates = engine._generate_scan_schedule()
        
        assert len(dates) == 3  # Jan 1, 2, 3
        assert dates[0].day == 1
        assert dates[2].day == 3
    
    def test_quarter_for_date(self):
        """Test quarter calculation for dates."""
        config = BacktestConfig({"start_date": "2024-01-01", "end_date": "2024-12-31"})
        engine = BacktestEngine(config)
        
        jan_date = datetime(2024, 1, 15)
        apr_date = datetime(2024, 4, 15)
        jul_date = datetime(2024, 7, 15)
        oct_date = datetime(2024, 10, 15)
        
        assert engine._get_quarter_for_date(jan_date) == "2024-Q1"
        assert engine._get_quarter_for_date(apr_date) == "2024-Q2"
        assert engine._get_quarter_for_date(jul_date) == "2024-Q3"
        assert engine._get_quarter_for_date(oct_date) == "2024-Q4"


class TestCalculationBreakdown:
    """Test calculation breakdown functionality."""
    
    @pytest.mark.asyncio
    async def test_opportunity_breakdown_not_found(self):
        """Test breakdown for non-existent opportunity."""
        with patch('backend.services.calculation_breakdown.supabase') as mock_supabase:
            mock_supabase.select = AsyncMock(return_value=[])
            
            with pytest.raises(ValueError, match="not found"):
                await get_opportunity_calculation_breakdown("invalid_id")
    
    @pytest.mark.asyncio
    async def test_opportunity_breakdown_success(self):
        """Test successful breakdown generation."""
        mock_opportunity = {
            "id": "test_opp_id",
            "market_id": "test_market",
            "p0": 0.45,
            "s_astro": 0.15,
            "p_astro": 0.55,
            "edge_net": 0.08,
            "decision": "BUY",
            "size_fraction": 0.04,
            "costs": {"fee": 0.006, "spread": 0.01, "slippage": 0.004},
            "config_snapshot": {"lambda_gain": 0.10}
        }
        
        mock_market = {
            "id": "test_market",
            "title": "Test Market",
            "price_yes": 0.45,
            "deadline_utc": "2024-12-31T23:59:59Z"
        }
        
        mock_contributions = [
            {
                "aspect_id": "aspect_1",
                "contribution": 0.08,
                "temporal_w": 1.0,
                "angular_w": 0.9,
                "severity_w": 1.0,
                "category_w": 0.8,
                "aspect_events": {
                    "planet1": "MARS",
                    "planet2": "JUPITER", 
                    "aspect": "square",
                    "peak_utc": "2024-06-15T12:00:00Z",
                    "orb_deg": 2.5,
                    "is_eclipse": False,
                    "severity": "major"
                }
            }
        ]
        
        with patch('backend.services.calculation_breakdown.supabase') as mock_supabase:
            # Mock database calls
            mock_supabase.select = AsyncMock(side_effect=[
                [mock_opportunity],  # opportunities query
                [mock_market],       # markets query  
                mock_contributions   # contributions query
            ])
            
            breakdown = await get_opportunity_calculation_breakdown("test_opp_id")
            
            assert breakdown["opportunity_id"] == "test_opp_id"
            assert len(breakdown["calculation_steps"]) > 0
            assert len(breakdown["aspects_analysis"]) == 1
            assert breakdown["final_summary"]["decision"] == "BUY"
            
            # Check specific calculation steps
            step_names = [step["name"] for step in breakdown["calculation_steps"]]
            assert "Base Probability" in step_names
            assert "Astrology Score" in step_names
            assert "Net Edge" in step_names
            assert "Position Sizing" in step_names


class TestKPICalculations:
    """Test individual KPI calculation functions."""
    
    def test_returns_calculation(self):
        """Test return calculation logic."""
        from backend.services.analytics import _calculate_returns
        
        mock_equity_curve = [
            {"ts": "2024-01-01T00:00:00Z", "equity_usdc": 1000.0},
            {"ts": "2024-06-01T00:00:00Z", "equity_usdc": 1100.0},
            {"ts": "2024-12-31T23:59:59Z", "equity_usdc": 1200.0}
        ]
        
        total_return, annualized_return = _calculate_returns(mock_equity_curve)
        
        assert total_return == 0.2  # 20% total return
        assert annualized_return > 0  # Should be positive
    
    def test_win_metrics_calculation(self):
        """Test win rate and profit factor calculation."""
        from backend.services.analytics import _calculate_win_metrics
        
        mock_trades = [
            {"pnl": 10.0},   # Win
            {"pnl": -5.0},   # Loss
            {"pnl": 15.0},   # Win
            {"pnl": -3.0}    # Loss
        ]
        
        win_rate, profit_factor = _calculate_win_metrics(mock_trades)
        
        assert win_rate == 0.5  # 2 wins out of 4 trades
        assert profit_factor == 25.0 / 8.0  # Total profit / Total loss
    
    def test_max_drawdown_calculation(self):
        """Test maximum drawdown calculation."""
        from backend.services.analytics import _calculate_max_drawdown
        
        mock_equity_curve = [
            {"equity_usdc": 1000.0},
            {"equity_usdc": 1100.0},  # Peak
            {"equity_usdc": 950.0},   # Drawdown 
            {"equity_usdc": 1000.0},
            {"equity_usdc": 1200.0}   # New peak
        ]
        
        max_dd = _calculate_max_drawdown(mock_equity_curve)
        
        # Max drawdown should be (1100 - 950) / 1100 = ~0.136
        assert abs(max_dd - 0.13636363636363635) < 0.001


if __name__ == "__main__":
    # Run basic tests
    test_analytics = TestAnalyticsService()
    test_backtest = TestBacktestEngine()
    test_breakdown = TestCalculationBreakdown()
    test_kpis = TestKPICalculations()
    
    # Test config creation
    test_backtest.test_backtest_config_creation()
    test_backtest.test_scan_schedule_generation()
    test_backtest.test_quarter_for_date()
    
    # Test KPI calculations
    test_kpis.test_returns_calculation()
    test_kpis.test_win_metrics_calculation()
    test_kpis.test_max_drawdown_calculation()
    
    print("✅ All basic analytics tests passed!")
    print("Run 'pytest tests/test_analytics.py' for async tests")