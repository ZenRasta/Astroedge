"""Analytics REST endpoints for KPIs, backtesting, and performance metrics."""

import logging
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

try:
    from ..services.analytics import calculate_portfolio_kpis, get_trade_scatter_data, get_daily_pnl_series
    from ..services.backtest import run_backtest, stop_backtest, get_backtest_status, list_backtest_runs
    from ..services.calculation_breakdown import get_opportunity_calculation_breakdown, get_market_calculation_factors
    from ..supabase_client import supabase
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from services.analytics import calculate_portfolio_kpis, get_trade_scatter_data, get_daily_pnl_series
    from services.backtest import run_backtest, stop_backtest, get_backtest_status, list_backtest_runs
    from services.calculation_breakdown import get_opportunity_calculation_breakdown, get_market_calculation_factors
    from supabase_client import supabase

logger = logging.getLogger(__name__)

router = APIRouter(tags=["analytics"])


class KPIResponse(BaseModel):
    """Portfolio KPI response."""
    total_return: float
    annualized_return: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    profit_factor: float
    total_trades: int
    avg_trade_pnl: float
    best_trade: float
    worst_trade: float
    avg_hold_time_hours: float
    total_fees: float
    current_positions: int
    total_volume: float


class TradeScatterPoint(BaseModel):
    """Trade scatter plot data point."""
    hold_time_hours: float
    pnl: float
    market_id: str
    side: str
    entry_price: float
    exit_price: Optional[float]
    qty: float


class DailyPnLEntry(BaseModel):
    """Daily P&L series entry."""
    date: str
    daily_pnl: float
    cumulative_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    fees: float


class BacktestRequest(BaseModel):
    """Request model for starting a backtest."""
    name: str = Field(..., description="Name for the backtest run")
    start_date: str = Field(..., description="Start date (ISO format)")
    end_date: str = Field(..., description="End date (ISO format)")
    initial_capital: float = Field(1000.0, description="Initial capital in USD")
    scan_frequency: str = Field("daily", description="Scan frequency (daily/hourly)")
    
    # Strategy parameters
    lambda_gain: float = Field(0.10, description="Lambda gain parameter")
    threshold: float = Field(0.04, description="Edge threshold")
    lambda_days: float = Field(5.0, description="Lambda days parameter") 
    max_positions: int = Field(10, description="Maximum concurrent positions")
    max_position_size: float = Field(0.05, description="Maximum position size fraction")
    fee_bps: int = Field(60, description="Fee in basis points")


class BacktestResponse(BaseModel):
    """Response model for backtest operations."""
    test_run_id: str
    status: str
    message: str


class BacktestStatus(BaseModel):
    """Backtest run status."""
    id: str
    name: str
    type: str
    status: str
    start_date: str
    end_date: Optional[str]
    metrics: Optional[Dict[str, Any]]
    created_at: str


@router.get("/kpis", response_model=KPIResponse)
async def get_portfolio_kpis(test_run_id: Optional[str] = Query(None, description="Test run ID for backtest KPIs")):
    """
    Get portfolio KPIs.
    
    If test_run_id is provided, returns backtest KPIs.
    Otherwise returns live trading KPIs.
    """
    try:
        kpis = await calculate_portfolio_kpis(test_run_id)
        
        return KPIResponse(**kpis.to_dict())
        
    except Exception as e:
        logger.error(f"Error calculating KPIs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trades/scatter", response_model=List[TradeScatterPoint])
async def get_trade_scatter(test_run_id: Optional[str] = Query(None, description="Test run ID for backtest data")):
    """
    Get trade scatter plot data for P&L vs hold time analysis.
    """
    try:
        scatter_data = await get_trade_scatter_data(test_run_id)
        
        return [
            TradeScatterPoint(
                hold_time_hours=point["hold_time_hours"],
                pnl=point["pnl"],
                market_id=point["market_id"],
                side=point["side"],
                entry_price=point["entry_price"],
                exit_price=point.get("exit_price"),
                qty=point["qty"]
            )
            for point in scatter_data
        ]
        
    except Exception as e:
        logger.error(f"Error getting trade scatter data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pnl/daily", response_model=List[DailyPnLEntry])
async def get_daily_pnl(
    test_run_id: Optional[str] = Query(None, description="Test run ID for backtest data"),
    days: int = Query(30, ge=1, le=365, description="Number of days to fetch")
):
    """
    Get daily P&L series for charting.
    """
    try:
        daily_data = await get_daily_pnl_series(test_run_id, days)
        
        return [
            DailyPnLEntry(
                date=entry["date"],
                daily_pnl=entry["daily_pnl"],
                cumulative_pnl=entry["cumulative_pnl"],
                realized_pnl=entry["realized_pnl"],
                unrealized_pnl=entry["unrealized_pnl"],
                fees=entry["fees"]
            )
            for entry in daily_data
        ]
        
    except Exception as e:
        logger.error(f"Error getting daily P&L data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/backtest/start", response_model=BacktestResponse)
async def start_backtest(request: BacktestRequest):
    """
    Start a new backtest run.
    """
    try:
        # Validate dates
        try:
            start_date = datetime.fromisoformat(request.start_date.replace('Z', '+00:00'))
            end_date = datetime.fromisoformat(request.end_date.replace('Z', '+00:00'))
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use ISO format.")
        
        if end_date <= start_date:
            raise HTTPException(status_code=400, detail="End date must be after start date")
        
        # Build config
        config = {
            "start_date": request.start_date,
            "end_date": request.end_date,
            "initial_capital": request.initial_capital,
            "scan_frequency": request.scan_frequency,
            "lambda_gain": request.lambda_gain,
            "threshold": request.threshold,
            "lambda_days": request.lambda_days,
            "max_positions": request.max_positions,
            "max_position_size": request.max_position_size,
            "fee_bps": request.fee_bps
        }
        
        # Start backtest asynchronously
        test_run_id = await run_backtest(request.name, config)
        
        return BacktestResponse(
            test_run_id=test_run_id,
            status="running",
            message=f"Backtest '{request.name}' started successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting backtest: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/backtest/{test_run_id}/stop", response_model=BacktestResponse)
async def stop_backtest_endpoint(test_run_id: str):
    """
    Stop a running backtest.
    """
    try:
        success = await stop_backtest(test_run_id)
        
        if success:
            return BacktestResponse(
                test_run_id=test_run_id,
                status="stopped",
                message="Backtest stopped successfully"
            )
        else:
            raise HTTPException(status_code=500, detail="Failed to stop backtest")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping backtest: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backtest/{test_run_id}/status", response_model=BacktestStatus)
async def get_backtest_status_endpoint(test_run_id: str):
    """
    Get status of a backtest run.
    """
    try:
        status = await get_backtest_status(test_run_id)
        
        if not status:
            raise HTTPException(status_code=404, detail="Backtest run not found")
        
        return BacktestStatus(
            id=status["id"],
            name=status["name"],
            type=status["type"],
            status=status["status"],
            start_date=status["start_date"],
            end_date=status.get("end_date"),
            metrics=status.get("metrics"),
            created_at=status["created_at"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting backtest status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backtest/runs", response_model=List[BacktestStatus])
async def list_backtest_runs_endpoint(limit: int = Query(20, ge=1, le=100)):
    """
    List recent backtest runs.
    """
    try:
        runs = await list_backtest_runs(limit)
        
        return [
            BacktestStatus(
                id=run["id"],
                name=run["name"],
                type=run["type"],
                status=run["status"],
                start_date=run["start_date"],
                end_date=run.get("end_date"),
                metrics=run.get("metrics"),
                created_at=run["created_at"]
            )
            for run in runs
        ]
        
    except Exception as e:
        logger.error(f"Error listing backtest runs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backtest/{test_run_id}/equity")
async def get_backtest_equity_curve(test_run_id: str):
    """
    Get equity curve for a backtest run.
    """
    try:
        equity_data = await supabase.select(
            table="test_equity",
            select="ts,equity_usdc,realized_pnl,unrealized_pnl,fees_usdc,positions_count",
            eq={"test_run_id": test_run_id}
        )
        
        if not equity_data:
            raise HTTPException(status_code=404, detail="No equity data found for this backtest")
        
        # Sort by timestamp
        equity_data = sorted(equity_data, key=lambda x: x["ts"])
        
        return equity_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting backtest equity curve: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backtest/{test_run_id}/trades")
async def get_backtest_trades(test_run_id: str, limit: int = Query(100, ge=1, le=500)):
    """
    Get trades for a backtest run.
    """
    try:
        trades = await supabase.select(
            table="test_trades",
            select="*",
            eq={"test_run_id": test_run_id}
        )
        
        if not trades:
            return []
        
        # Sort by entry time
        trades = sorted(trades, key=lambda x: x["entry_time"], reverse=True)[:limit]
        
        return trades
        
    except Exception as e:
        logger.error(f"Error getting backtest trades: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backtest/{test_run_id}/opportunities")
async def get_backtest_opportunities(test_run_id: str, limit: int = Query(100, ge=1, le=500)):
    """
    Get opportunities scanned during a backtest run.
    """
    try:
        opportunities = await supabase.select(
            table="test_opportunities",
            select="*",
            eq={"test_run_id": test_run_id}
        )
        
        if not opportunities:
            return []
        
        # Sort by scan time
        opportunities = sorted(opportunities, key=lambda x: x["scan_time"], reverse=True)[:limit]
        
        return opportunities
        
    except Exception as e:
        logger.error(f"Error getting backtest opportunities: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/markets/top-performers")
async def get_top_performing_markets(limit: int = Query(10, ge=1, le=50)):
    """
    Get top performing markets by total P&L.
    """
    try:
        # Get markets with their total P&L from fills/trades
        markets_pnl = await supabase.select(
            table="fills",
            select="market_id"
        )
        
        # This is simplified - in real implementation would aggregate P&L by market
        # For now, return sample data
        return [
            {
                "market_id": "sample_market_1",
                "title": "Sample Market 1",
                "total_pnl": 125.50,
                "trade_count": 8,
                "win_rate": 0.75
            }
        ]
        
    except Exception as e:
        logger.error(f"Error getting top performing markets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard/summary")
async def get_dashboard_summary():
    """
    Get high-level dashboard summary.
    """
    try:
        # Get live KPIs
        kpis = await calculate_portfolio_kpis()
        
        # Get recent backtest count
        backtest_runs = await list_backtest_runs(limit=5)
        
        # Get active positions count
        positions = await supabase.select(
            table="positions",
            select="id"
        )
        
        return {
            "live_equity": kpis.total_return * 1000,  # Assuming $1000 base
            "live_pnl_today": 0,  # Would calculate from today's equity change
            "total_trades": kpis.total_trades,
            "active_positions": len(positions or []),
            "recent_backtests": len(backtest_runs),
            "running_backtests": len([r for r in backtest_runs if r["status"] == "running"])
        }
        
    except Exception as e:
        logger.error(f"Error getting dashboard summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/opportunities/{opportunity_id}/breakdown")
async def get_opportunity_breakdown(opportunity_id: str):
    """
    Get detailed calculation breakdown for an opportunity.
    """
    try:
        breakdown = await get_opportunity_calculation_breakdown(opportunity_id)
        return breakdown
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting opportunity breakdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/markets/{market_id}/factors")
async def get_market_factors(market_id: str):
    """
    Get calculation factors for a market.
    """
    try:
        factors = await get_market_calculation_factors(market_id)
        return factors
        
    except Exception as e:
        logger.error(f"Error getting market factors: {e}")
        raise HTTPException(status_code=500, detail=str(e))
