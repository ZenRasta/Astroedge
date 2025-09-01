"""Trading REST endpoints for order placement and P&L monitoring."""

import logging
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

try:
    from ..trading import execute_order, OrderIn
    from ..pnl import mark_to_market, get_equity_curve, get_positions_summary, get_performance_metrics
    from ..supabase_client import supabase
    from ..services.risk import enable_trading, disable_trading
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from trading import execute_order, OrderIn
    from pnl import mark_to_market, get_equity_curve, get_positions_summary, get_performance_metrics
    from supabase_client import supabase
    from services.risk import enable_trading, disable_trading

logger = logging.getLogger(__name__)

router = APIRouter(tags=["trading"])


class PlaceOrderRequest(BaseModel):
    """Request model for placing orders."""
    market_id: str = Field(..., description="Market ID")
    token_id: Optional[str] = Field(None, description="Token ID (optional, will lookup YES token)")
    side: str = Field("YES", description="Order side (YES only in MVP)")
    qty: float = Field(..., gt=0, description="Quantity in shares")
    limit_price: Optional[float] = Field(None, description="Limit price (optional for IOC)")
    tif: str = Field("IOC", description="Time in force")
    comment: Optional[str] = Field(None, description="Optional comment")


class OrderResponse(BaseModel):
    """Response model for order execution."""
    order_id: Optional[str]
    filled: float
    avg_px: Optional[float]
    status: str
    fee_usdc: Optional[float] = None


class PositionSummary(BaseModel):
    """Position summary with current mark-to-market."""
    market_id: str
    market_title: str
    token_id: Optional[str]
    side: str
    qty: float
    vwap: float
    mark_price: float
    realized_pnl: float
    unrealized_pnl: float
    current_value: float
    cost_basis: float


class PnLSnapshot(BaseModel):
    """Current P&L snapshot."""
    ts: str
    equity_usdc: float
    realized_usdc: float
    unrealized_usdc: float
    fees_usdc: float


class EquityCurveEntry(BaseModel):
    """Equity curve entry."""
    ts: str
    equity_usdc: float
    realized_usdc: float
    unrealized_usdc: float
    fees_usdc: float


@router.post("/orders/place", response_model=OrderResponse)
async def place_order(request: PlaceOrderRequest):
    """
    Place a paper trading order.
    
    Executes immediately with IOC semantics against current orderbook + slippage.
    """
    try:
        order = OrderIn(
            market_id=request.market_id,
            token_id=request.token_id,
            side=request.side,
            qty=request.qty,
            limit_price=request.limit_price,
            tif=request.tif,
            comment=request.comment
        )
        
        result = await execute_order(order)
        
        return OrderResponse(
            order_id=result.get("order_id"),
            filled=result.get("filled", 0.0),
            avg_px=result.get("avg_px"),
            status=result.get("status", "unknown"),
            fee_usdc=result.get("fee_usdc")
        )
        
    except Exception as e:
        logger.error(f"Error placing order: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/positions", response_model=List[PositionSummary])
async def get_positions():
    """
    Get current positions with mark-to-market values.
    """
    try:
        positions = await get_positions_summary()
        
        return [
            PositionSummary(
                market_id=pos["market_id"],
                market_title=pos["market_title"],
                token_id=pos.get("token_id"),
                side=pos["side"],
                qty=float(pos["qty"]),
                vwap=float(pos["vwap"]),
                mark_price=pos["mark_price"],
                realized_pnl=float(pos["realized_pnl"]),
                unrealized_pnl=pos["unrealized_pnl"],
                current_value=pos["current_value"],
                cost_basis=pos["cost_basis"]
            )
            for pos in positions
        ]
        
    except Exception as e:
        logger.error(f"Error fetching positions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pnl", response_model=PnLSnapshot)
async def get_pnl_snapshot():
    """
    Get current P&L snapshot with mark-to-market.
    
    This triggers a fresh mark-to-market calculation and updates the equity curve.
    """
    try:
        snapshot = await mark_to_market()
        
        return PnLSnapshot(
            ts=snapshot["ts"],
            equity_usdc=snapshot["equity_usdc"],
            realized_usdc=snapshot["realized_usdc"],
            unrealized_usdc=snapshot["unrealized_usdc"],
            fees_usdc=snapshot["fees_usdc"]
        )
        
    except Exception as e:
        logger.error(f"Error getting P&L snapshot: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pnl/equity_curve", response_model=List[EquityCurveEntry])
async def get_pnl_curve(limit: int = Query(200, ge=1, le=1000)):
    """
    Get historical equity curve.
    
    Args:
        limit: Maximum number of entries to return (default 200)
    """
    try:
        curve_data = await get_equity_curve(limit=limit)
        
        return [
            EquityCurveEntry(
                ts=entry["ts"],
                equity_usdc=float(entry["equity_usdc"]),
                realized_usdc=float(entry["realized_usdc"]),
                unrealized_usdc=float(entry["unrealized_usdc"]),
                fees_usdc=float(entry["fees_usdc"])
            )
            for entry in curve_data
        ]
        
    except Exception as e:
        logger.error(f"Error fetching equity curve: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/performance")
async def get_performance():
    """
    Get portfolio performance metrics.
    """
    try:
        metrics = await get_performance_metrics()
        return metrics
        
    except Exception as e:
        logger.error(f"Error calculating performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/orders")
async def get_orders(limit: int = Query(50, ge=1, le=200)):
    """
    Get recent orders.
    """
    try:
        orders = await supabase.select(
            table="orders",
            select="id,ts,market_id,side,qty,limit_price,mode,comment"
        )
        
        # Sort by timestamp (client-side)
        if orders:
            orders = sorted(orders, key=lambda x: x["ts"], reverse=True)[:limit]
        
        return orders or []
        
    except Exception as e:
        logger.error(f"Error fetching orders: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fills")
async def get_fills(limit: int = Query(50, ge=1, le=200)):
    """
    Get recent fills.
    """
    try:
        fills = await supabase.select(
            table="fills",
            select="id,ts,order_id,market_id,side,qty,price,fee_usdc"
        )
        
        # Sort by timestamp (client-side)
        if fills:
            fills = sorted(fills, key=lambda x: x["ts"], reverse=True)[:limit]
        
        return fills or []
        
    except Exception as e:
        logger.error(f"Error fetching fills: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/risk/enable")
async def enable_trading_endpoint():
    """
    Enable trading (admin function).
    """
    try:
        success = await enable_trading()
        if success:
            return {"status": "success", "message": "Trading enabled"}
        else:
            raise HTTPException(status_code=500, detail="Failed to enable trading")
        
    except Exception as e:
        logger.error(f"Error enabling trading: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/risk/disable")
async def disable_trading_endpoint(reason: str = "Manual disable"):
    """
    Disable trading (admin function).
    """
    try:
        success = await disable_trading(reason)
        if success:
            return {"status": "success", "message": f"Trading disabled: {reason}"}
        else:
            raise HTTPException(status_code=500, detail="Failed to disable trading")
        
    except Exception as e:
        logger.error(f"Error disabling trading: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/risk/status")
async def get_risk_status():
    """
    Get current risk management status.
    """
    try:
        risk_states = await supabase.select(
            table="risk_state",
            select="trading_enabled,daily_pnl_start_usdc,updated_at"
        )
        
        if not risk_states:
            return {"trading_enabled": True, "daily_pnl_start_usdc": 0.0}
        
        return risk_states[0]
        
    except Exception as e:
        logger.error(f"Error fetching risk status: {e}")
        raise HTTPException(status_code=500, detail=str(e))