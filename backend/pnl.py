"""P&L calculation and equity curve management."""

import logging
from typing import Dict, List, Any
from datetime import datetime, timezone

try:
    from .config import settings
    from .supabase_client import supabase
    from .polymarket_client import get_books_batch
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(__file__))
    from config import settings
    from supabase_client import supabase
    from polymarket_client import get_books_batch

logger = logging.getLogger(__name__)


def _mark_price(l1, side: str) -> float:
    """Calculate mark price based on configured method."""
    method = settings.mark_method.lower()
    
    if method == "mid":
        if l1.bid_yes is not None and l1.ask_yes is not None:
            return (l1.bid_yes + l1.ask_yes) / 2.0
        elif l1.bid_yes is not None:
            return l1.bid_yes
        elif l1.ask_yes is not None:
            return l1.ask_yes
        else:
            return 0.5  # Default if no prices available
            
    elif method == "bid":
        return l1.bid_yes or 0.0
        
    elif method == "ask":
        return l1.ask_yes or 1.0
        
    else:
        # Default to mid
        if l1.bid_yes is not None and l1.ask_yes is not None:
            return (l1.bid_yes + l1.ask_yes) / 2.0
        return l1.bid_yes or l1.ask_yes or 0.5


async def mark_to_market() -> Dict[str, Any]:
    """
    Calculate current mark-to-market P&L and update equity curve.
    
    Returns:
        Dict with equity_usdc, realized_usdc, unrealized_usdc, fees_usdc
    """
    try:
        timestamp = datetime.now(timezone.utc)
        
        # Load all positions
        positions = await supabase.select(
            table="positions",
            select="market_id,token_id,side,qty,vwap,realized_pnl"
        )
        
        if not positions:
            # No positions - initialize with zero equity
            snap = {
                "ts": timestamp.isoformat(),
                "equity_usdc": 0.0,
                "realized_usdc": 0.0,
                "unrealized_usdc": 0.0,
                "fees_usdc": 0.0
            }
            
            await supabase.insert(
                table="equity_curve",
                data={k: v for k, v in snap.items() if k != "ts"}
            )
            
            logger.info("Created initial equity curve entry with zero values")
            return snap
        
        # Get all unique token IDs for marking
        token_ids = list(set(p["token_id"] for p in positions if p.get("token_id")))
        
        if not token_ids:
            logger.warning("No token IDs found in positions")
            unrealized_pnl = 0.0
        else:
            # Fetch current orderbooks
            l1_books = await get_books_batch(token_ids)
            
            # Calculate unrealized P&L
            unrealized_pnl = 0.0
            for pos in positions:
                token_id = pos.get("token_id")
                if not token_id:
                    continue
                
                l1 = l1_books.get(token_id)
                if not l1:
                    logger.warning(f"No orderbook data for token {token_id}")
                    continue
                
                mark_px = _mark_price(l1, pos["side"])
                position_qty = float(pos["qty"])
                vwap = float(pos["vwap"])
                
                # Unrealized P&L = qty * (mark - vwap)
                position_unrealized = position_qty * (mark_px - vwap)
                unrealized_pnl += position_unrealized
                
                logger.debug(f"Position {pos['market_id']}: qty={position_qty}, vwap={vwap:.4f}, mark={mark_px:.4f}, unreal={position_unrealized:.2f}")
        
        # Calculate realized P&L (sum from positions)
        realized_pnl = sum(float(p.get("realized_pnl", 0)) for p in positions)
        
        # Calculate total fees (sum from all fills)
        fills = await supabase.select(table="fills", select="fee_usdc")
        total_fees = sum(float(f.get("fee_usdc", 0)) for f in fills)
        
        # Total equity (fees are already accounted for in realized P&L from fills)
        equity = realized_pnl + unrealized_pnl
        
        # Create equity snapshot
        snap = {
            "ts": timestamp.isoformat(),
            "equity_usdc": equity,
            "realized_usdc": realized_pnl,
            "unrealized_usdc": unrealized_pnl,
            "fees_usdc": total_fees
        }
        
        # Upsert to equity curve (using timestamp as primary key)
        try:
            await supabase.insert(
                table="equity_curve",
                data={
                    "ts": snap["ts"],
                    "equity_usdc": snap["equity_usdc"],
                    "realized_usdc": snap["realized_usdc"],
                    "unrealized_usdc": snap["unrealized_usdc"],
                    "fees_usdc": snap["fees_usdc"]
                }
            )
        except Exception as e:
            # If insert fails (duplicate), try update
            logger.debug(f"Insert failed, attempting upsert: {e}")
            await supabase.update(
                table="equity_curve",
                data={
                    "equity_usdc": snap["equity_usdc"],
                    "realized_usdc": snap["realized_usdc"],
                    "unrealized_usdc": snap["unrealized_usdc"],
                    "fees_usdc": snap["fees_usdc"]
                },
                filters={"ts": snap["ts"]}
            )
        
        logger.info(f"Mark-to-market: equity=${equity:.2f}, realized=${realized_pnl:.2f}, unrealized=${unrealized_pnl:.2f}")
        return snap
        
    except Exception as e:
        logger.error(f"Error in mark-to-market calculation: {e}")
        # Return default values on error
        return {
            "ts": datetime.now(timezone.utc).isoformat(),
            "equity_usdc": 0.0,
            "realized_usdc": 0.0,
            "unrealized_usdc": 0.0,
            "fees_usdc": 0.0
        }


async def get_equity_curve(limit: int = 200) -> List[Dict[str, Any]]:
    """
    Get historical equity curve data.
    
    Args:
        limit: Maximum number of entries to return
        
    Returns:
        List of equity curve entries sorted by timestamp
    """
    try:
        equity_data = await supabase.select(
            table="equity_curve",
            select="ts,equity_usdc,realized_usdc,unrealized_usdc,fees_usdc"
        )
        
        if not equity_data:
            return []
        
        # Sort by timestamp (client-side since our simple client doesn't support order by)
        sorted_data = sorted(equity_data, key=lambda x: x["ts"])
        
        # Apply limit
        return sorted_data[-limit:] if len(sorted_data) > limit else sorted_data
        
    except Exception as e:
        logger.error(f"Error fetching equity curve: {e}")
        return []


async def get_positions_summary() -> List[Dict[str, Any]]:
    """
    Get current positions with market-to-market values.
    
    Returns:
        List of positions with unrealized P&L
    """
    try:
        positions = await supabase.select(
            table="positions",
            select="market_id,token_id,side,qty,vwap,realized_pnl,updated_at"
        )
        
        if not positions:
            return []
        
        # Get market names for display
        market_ids = list(set(p["market_id"] for p in positions))
        markets_data = {}
        
        if market_ids:
            markets = await supabase.select(
                table="markets",
                select="id,title"
            )
            markets_data = {m["id"]: m.get("title", "Unknown") for m in markets}
        
        # Get current marks
        token_ids = list(set(p["token_id"] for p in positions if p.get("token_id")))
        l1_books = {}
        
        if token_ids:
            l1_books = await get_books_batch(token_ids)
        
        # Enrich positions with current values
        enriched_positions = []
        for pos in positions:
            token_id = pos.get("token_id")
            l1 = l1_books.get(token_id) if token_id else None
            
            mark_px = _mark_price(l1, pos["side"]) if l1 else float(pos["vwap"])
            qty = float(pos["qty"])
            vwap = float(pos["vwap"])
            
            unrealized = qty * (mark_px - vwap)
            current_value = qty * mark_px
            
            enriched_pos = {
                **pos,
                "market_title": markets_data.get(pos["market_id"], pos["market_id"]),
                "mark_price": mark_px,
                "unrealized_pnl": unrealized,
                "current_value": current_value,
                "cost_basis": qty * vwap
            }
            
            enriched_positions.append(enriched_pos)
        
        # Sort by absolute unrealized P&L descending
        enriched_positions.sort(key=lambda x: abs(x["unrealized_pnl"]), reverse=True)
        
        return enriched_positions
        
    except Exception as e:
        logger.error(f"Error getting positions summary: {e}")
        return []


async def get_performance_metrics() -> Dict[str, Any]:
    """
    Calculate portfolio performance metrics.
    
    Returns:
        Dict with various performance statistics
    """
    try:
        equity_data = await get_equity_curve(limit=1000)  # Get more history for metrics
        
        if len(equity_data) < 2:
            return {
                "total_trades": 0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "max_drawdown": 0.0,
                "sharpe_ratio": 0.0
            }
        
        # Calculate basic metrics
        initial_equity = equity_data[0]["equity_usdc"]
        current_equity = equity_data[-1]["equity_usdc"]
        total_return = float(current_equity) - float(initial_equity)
        
        # Calculate maximum drawdown
        peak = float(initial_equity)
        max_drawdown = 0.0
        
        for entry in equity_data:
            equity = float(entry["equity_usdc"])
            if equity > peak:
                peak = equity
            drawdown = (peak - equity) / peak if peak > 0 else 0.0
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
        # Get trade statistics
        fills = await supabase.select(table="fills", select="side,qty,price,fee_usdc")
        total_trades = len(fills) if fills else 0
        
        # Calculate win rate (simplified - need better trade matching logic)
        winning_trades = 0
        if fills:
            for fill in fills:
                if fill["side"] == "YES" and float(fill["price"]) < 0.6:  # Rough heuristic
                    winning_trades += 1
        
        win_rate = winning_trades / total_trades if total_trades > 0 else 0.0
        
        return {
            "total_return": total_return,
            "total_trades": total_trades,
            "win_rate": win_rate,
            "max_drawdown": max_drawdown,
            "current_equity": float(current_equity),
            "peak_equity": peak
        }
        
    except Exception as e:
        logger.error(f"Error calculating performance metrics: {e}")
        return {
            "total_trades": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "max_drawdown": 0.0,
            "sharpe_ratio": 0.0
        }