"""Analytics service for calculating performance KPIs and metrics."""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import math
import numpy as np

try:
    from ..supabase_client import supabase
    from ..pnl import get_equity_curve
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from supabase_client import supabase
    from pnl import get_equity_curve

logger = logging.getLogger(__name__)


class PerformanceKPIs:
    """Container for performance KPIs."""
    
    def __init__(self, data: Dict[str, Any]):
        self.total_return = data.get("total_return", 0.0)
        self.annualized_return = data.get("annualized_return", 0.0)
        self.sharpe_ratio = data.get("sharpe_ratio", 0.0)
        self.max_drawdown = data.get("max_drawdown", 0.0)
        self.win_rate = data.get("win_rate", 0.0)
        self.profit_factor = data.get("profit_factor", 0.0)
        self.total_trades = data.get("total_trades", 0)
        self.avg_trade_pnl = data.get("avg_trade_pnl", 0.0)
        self.best_trade = data.get("best_trade", 0.0)
        self.worst_trade = data.get("worst_trade", 0.0)
        self.avg_hold_time_hours = data.get("avg_hold_time_hours", 0.0)
        self.total_fees = data.get("total_fees", 0.0)
        self.current_positions = data.get("current_positions", 0)
        self.total_volume = data.get("total_volume", 0.0)
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_return": self.total_return,
            "annualized_return": self.annualized_return,  
            "sharpe_ratio": self.sharpe_ratio,
            "max_drawdown": self.max_drawdown,
            "win_rate": self.win_rate,
            "profit_factor": self.profit_factor,
            "total_trades": self.total_trades,
            "avg_trade_pnl": self.avg_trade_pnl,
            "best_trade": self.best_trade,
            "worst_trade": self.worst_trade,
            "avg_hold_time_hours": self.avg_hold_time_hours,
            "total_fees": self.total_fees,
            "current_positions": self.current_positions,
            "total_volume": self.total_volume
        }


async def calculate_portfolio_kpis(test_run_id: Optional[str] = None) -> PerformanceKPIs:
    """
    Calculate comprehensive portfolio KPIs.
    
    Args:
        test_run_id: If provided, calculate for specific test run. Otherwise use live data.
    """
    try:
        if test_run_id:
            # Backtest data
            trades = await _get_test_trades(test_run_id)
            equity_curve = await _get_test_equity_curve(test_run_id)
            positions_count = await _get_test_positions_count(test_run_id)
        else:
            # Live data
            trades = await _get_live_fills()
            equity_curve = await get_equity_curve(limit=1000)
            positions_count = await _get_live_positions_count()
        
        if not trades:
            return PerformanceKPIs({})
        
        # Calculate metrics
        total_return, annualized_return = _calculate_returns(equity_curve)
        sharpe_ratio = _calculate_sharpe_ratio(equity_curve)
        max_drawdown = _calculate_max_drawdown(equity_curve)
        win_rate, profit_factor = _calculate_win_metrics(trades)
        trade_stats = _calculate_trade_stats(trades)
        
        total_fees = sum(trade.get("fees", 0) or 0 for trade in trades)
        total_volume = sum(trade.get("qty", 0) * trade.get("price", 0) for trade in trades)
        
        return PerformanceKPIs({
            "total_return": total_return,
            "annualized_return": annualized_return,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "total_trades": len(trades),
            "avg_trade_pnl": trade_stats.get("avg_pnl", 0.0),
            "best_trade": trade_stats.get("best_trade", 0.0),
            "worst_trade": trade_stats.get("worst_trade", 0.0),
            "avg_hold_time_hours": trade_stats.get("avg_hold_hours", 0.0),
            "total_fees": total_fees,
            "current_positions": positions_count,
            "total_volume": total_volume
        })
        
    except Exception as e:
        logger.error(f"Error calculating portfolio KPIs: {e}")
        return PerformanceKPIs({})


async def get_trade_scatter_data(test_run_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Get trade scatter plot data for P&L vs hold time analysis.
    
    Args:
        test_run_id: If provided, get backtest data. Otherwise use live data.
    """
    try:
        if test_run_id:
            trades = await _get_test_trades(test_run_id)
        else:
            trades = await _get_live_fills()
        
        scatter_data = []
        for trade in trades:
            # Calculate hold time in hours
            entry_time = _parse_timestamp(trade.get("entry_time") or trade.get("ts"))
            exit_time = _parse_timestamp(trade.get("exit_time"))
            
            if entry_time and exit_time:
                hold_hours = (exit_time - entry_time).total_seconds() / 3600
            else:
                hold_hours = 0
            
            # Calculate P&L
            pnl = trade.get("realized_pnl") or trade.get("pnl", 0)
            
            scatter_data.append({
                "hold_time_hours": hold_hours,
                "pnl": pnl,
                "market_id": trade.get("market_id", "unknown"),
                "side": trade.get("side", "unknown"),
                "entry_price": trade.get("entry_price") or trade.get("price", 0),
                "exit_price": trade.get("exit_price"),
                "qty": trade.get("qty", 0)
            })
        
        return scatter_data
        
    except Exception as e:
        logger.error(f"Error getting trade scatter data: {e}")
        return []


async def get_daily_pnl_series(test_run_id: Optional[str] = None, days: int = 30) -> List[Dict[str, Any]]:
    """
    Get daily P&L series for charting.
    
    Args:
        test_run_id: If provided, get backtest data. Otherwise use live data.
        days: Number of days to fetch
    """
    try:
        if test_run_id:
            equity_curve = await _get_test_equity_curve(test_run_id)
        else:
            equity_curve = await get_equity_curve(limit=days * 24)  # Approximate hourly data
        
        # Group by day and calculate daily P&L changes
        daily_data = {}
        prev_equity = 0
        
        for entry in equity_curve:
            date_str = _parse_timestamp(entry["ts"]).date().isoformat()
            equity = entry.get("equity_usdc", 0)
            
            if date_str not in daily_data:
                daily_pnl = equity - prev_equity if prev_equity > 0 else 0
                daily_data[date_str] = {
                    "date": date_str,
                    "daily_pnl": daily_pnl,
                    "cumulative_pnl": equity,
                    "realized_pnl": entry.get("realized_usdc", 0),
                    "unrealized_pnl": entry.get("unrealized_usdc", 0),
                    "fees": entry.get("fees_usdc", 0)
                }
            
            prev_equity = equity
        
        # Sort by date and return list
        return sorted(daily_data.values(), key=lambda x: x["date"])[-days:]
        
    except Exception as e:
        logger.error(f"Error getting daily P&L series: {e}")
        return []


def _calculate_returns(equity_curve: List[Dict[str, Any]]) -> Tuple[float, float]:
    """Calculate total and annualized returns."""
    if not equity_curve or len(equity_curve) < 2:
        return 0.0, 0.0
    
    # Sort by timestamp
    sorted_curve = sorted(equity_curve, key=lambda x: _parse_timestamp(x["ts"]))
    
    initial_equity = sorted_curve[0].get("equity_usdc", 0)
    final_equity = sorted_curve[-1].get("equity_usdc", 0)
    
    if initial_equity <= 0:
        return 0.0, 0.0
    
    total_return = (final_equity - initial_equity) / initial_equity
    
    # Calculate time period in years
    start_time = _parse_timestamp(sorted_curve[0]["ts"])
    end_time = _parse_timestamp(sorted_curve[-1]["ts"])
    years = (end_time - start_time).total_seconds() / (365.25 * 24 * 3600)
    
    if years <= 0:
        return total_return, 0.0
    
    # Annualized return
    annualized_return = ((final_equity / initial_equity) ** (1/years)) - 1
    
    return total_return, annualized_return


def _calculate_sharpe_ratio(equity_curve: List[Dict[str, Any]], risk_free_rate: float = 0.05) -> float:
    """Calculate Sharpe ratio."""
    if not equity_curve or len(equity_curve) < 2:
        return 0.0
    
    # Calculate daily returns
    returns = []
    sorted_curve = sorted(equity_curve, key=lambda x: _parse_timestamp(x["ts"]))
    
    for i in range(1, len(sorted_curve)):
        prev_equity = sorted_curve[i-1].get("equity_usdc", 0)
        curr_equity = sorted_curve[i].get("equity_usdc", 0)
        
        if prev_equity > 0:
            daily_return = (curr_equity - prev_equity) / prev_equity
            returns.append(daily_return)
    
    if not returns:
        return 0.0
    
    # Convert to numpy for easier calculation
    returns_array = np.array(returns)
    
    if len(returns_array) == 0 or np.std(returns_array) == 0:
        return 0.0
    
    # Annualize (assuming daily data points)
    daily_risk_free = risk_free_rate / 365.25
    excess_return = np.mean(returns_array) - daily_risk_free
    volatility = np.std(returns_array)
    
    sharpe = (excess_return / volatility) * math.sqrt(365.25)
    return sharpe


def _calculate_max_drawdown(equity_curve: List[Dict[str, Any]]) -> float:
    """Calculate maximum drawdown."""
    if not equity_curve:
        return 0.0
    
    sorted_curve = sorted(equity_curve, key=lambda x: _parse_timestamp(x["ts"]))
    peak = 0
    max_drawdown = 0
    
    for entry in sorted_curve:
        equity = entry.get("equity_usdc", 0)
        
        if equity > peak:
            peak = equity
        
        if peak > 0:
            drawdown = (peak - equity) / peak
            max_drawdown = max(max_drawdown, drawdown)
    
    return max_drawdown


def _calculate_win_metrics(trades: List[Dict[str, Any]]) -> Tuple[float, float]:
    """Calculate win rate and profit factor."""
    if not trades:
        return 0.0, 0.0
    
    wins = 0
    total_profit = 0.0
    total_loss = 0.0
    
    for trade in trades:
        pnl = trade.get("realized_pnl") or trade.get("pnl", 0)
        
        if pnl > 0:
            wins += 1
            total_profit += pnl
        elif pnl < 0:
            total_loss += abs(pnl)
    
    win_rate = wins / len(trades)
    profit_factor = total_profit / total_loss if total_loss > 0 else float('inf') if total_profit > 0 else 0.0
    
    return win_rate, profit_factor


def _calculate_trade_stats(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate trade statistics."""
    if not trades:
        return {}
    
    pnls = []
    hold_times = []
    
    for trade in trades:
        pnl = trade.get("realized_pnl") or trade.get("pnl", 0)
        pnls.append(pnl)
        
        # Calculate hold time
        entry_time = _parse_timestamp(trade.get("entry_time") or trade.get("ts"))
        exit_time = _parse_timestamp(trade.get("exit_time"))
        
        if entry_time and exit_time:
            hold_hours = (exit_time - entry_time).total_seconds() / 3600
            hold_times.append(hold_hours)
    
    return {
        "avg_pnl": np.mean(pnls) if pnls else 0.0,
        "best_trade": max(pnls) if pnls else 0.0,
        "worst_trade": min(pnls) if pnls else 0.0,
        "avg_hold_hours": np.mean(hold_times) if hold_times else 0.0
    }


async def _get_test_trades(test_run_id: str) -> List[Dict[str, Any]]:
    """Get trades for a test run."""
    return await supabase.select(
        table="test_trades",
        select="*",
        eq={"test_run_id": test_run_id}
    ) or []


async def _get_test_equity_curve(test_run_id: str) -> List[Dict[str, Any]]:
    """Get equity curve for a test run."""
    return await supabase.select(
        table="test_equity", 
        select="*",
        eq={"test_run_id": test_run_id}
    ) or []


async def _get_test_positions_count(test_run_id: str) -> int:
    """Get current positions count for a test run."""
    trades = await supabase.select(
        table="test_trades",
        select="id",
        eq={"test_run_id": test_run_id},
        is_null={"exit_time": True}
    )
    return len(trades or [])


async def _get_live_fills() -> List[Dict[str, Any]]:
    """Get live fills."""
    return await supabase.select(
        table="fills",
        select="*"
    ) or []


async def _get_live_positions_count() -> int:
    """Get live positions count."""
    positions = await supabase.select(
        table="positions",
        select="id"
    )
    return len(positions or [])


def _parse_timestamp(ts_str: Optional[str]) -> Optional[datetime]:
    """Parse timestamp string."""
    if not ts_str:
        return None
        
    try:
        # Handle various timestamp formats
        if isinstance(ts_str, datetime):
            return ts_str
        return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None