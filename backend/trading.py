"""Paper trading execution engine with slippage model."""

import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime, timezone

try:
    from .config import settings
    from .supabase_client import supabase
    from .polymarket_client import get_books_batch
    from .services.risk import perform_full_risk_check
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(__file__))
    from config import settings
    from supabase_client import supabase
    from polymarket_client import get_books_batch
    from services.risk import perform_full_risk_check

logger = logging.getLogger(__name__)


@dataclass
class OrderIn:
    """Input order specification."""
    market_id: str
    token_id: Optional[str]
    side: str          # 'YES' only for MVP
    qty: float
    limit_price: Optional[float] = None
    tif: str = "IOC"
    comment: Optional[str] = None


def _slippage(qty: float, top_depth: float) -> float:
    """Calculate price impact using convex slippage model."""
    A = float(settings.slippage_a)
    B = float(settings.slippage_b)
    
    # Linear impact up to available depth, convex beyond
    q1 = min(qty, top_depth) if top_depth > 0 else 0
    q2 = max(qty - top_depth, 0.0) if top_depth > 0 else qty
    
    slippage = A * q1 + B * q2
    logger.debug(f"Slippage calculation: qty={qty}, depth={top_depth}, q1={q1}, q2={q2}, slip={slippage}")
    
    return slippage


async def _get_yes_token_id(market_id: str) -> Optional[str]:
    """Get YES token ID for a market."""
    try:
        markets = await supabase.select(
            table="markets",
            select="id,tokens",
            filters={"id": market_id}
        )
        
        if not markets:
            logger.warning(f"Market {market_id} not found")
            return None
        
        market = markets[0]
        tokens = market.get("tokens", [])
        
        # Look for YES token
        for token in tokens:
            if isinstance(token, dict) and token.get("outcome", "").lower() == "yes":
                return token.get("token_id")
        
        # Fallback to first token if available
        if tokens and isinstance(tokens[0], dict):
            return tokens[0].get("token_id")
        
        logger.warning(f"No YES token found for market {market_id}")
        return None
        
    except Exception as e:
        logger.error(f"Error getting YES token for market {market_id}: {e}")
        return None


async def _update_positions_from_fill(
    market_id: str, 
    token_id: str, 
    side: str, 
    qty: float, 
    price: float, 
    fee_usdc: float
) -> None:
    """Update positions table from a fill."""
    try:
        # Get existing position
        positions = await supabase.select(
            table="positions",
            select="*",
            filters={"market_id": market_id}
        )
        
        if not positions:
            # New position
            position_data = {
                "market_id": market_id,
                "token_id": token_id,
                "side": side,
                "qty": qty,
                "vwap": price,
                "realized_pnl": -fee_usdc,  # Opening position pays fees
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            await supabase.insert(table="positions", data=position_data)
            logger.info(f"Created new position for {market_id}: {qty} @ {price}")
            
        else:
            # Update existing position
            pos = positions[0]
            current_qty = float(pos["qty"])
            current_vwap = float(pos["vwap"])
            current_realized = float(pos["realized_pnl"])
            
            if (side == "YES" and current_qty >= 0) or (side == "NO" and current_qty < 0):
                # Adding to position - update VWAP
                new_qty = current_qty + qty
                new_vwap = (current_qty * current_vwap + qty * price) / new_qty if new_qty != 0 else price
                new_realized = current_realized - fee_usdc  # Opening costs
                
            else:
                # Reducing position - realize P&L
                if side == "YES":  # Selling shares we're long
                    realized_gain = (price - current_vwap) * min(qty, abs(current_qty)) - fee_usdc
                else:  # Covering short (buying back NO)
                    realized_gain = (current_vwap - price) * min(qty, abs(current_qty)) - fee_usdc
                
                new_realized = current_realized + realized_gain
                new_qty = current_qty + (qty if side == "YES" else -qty)
                new_vwap = current_vwap  # Keep same VWAP for remaining position
            
            # Update position
            update_data = {
                "qty": new_qty,
                "vwap": new_vwap,
                "realized_pnl": new_realized,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            await supabase.update(
                table="positions",
                data=update_data,
                filters={"market_id": market_id}
            )
            
            logger.info(f"Updated position for {market_id}: {current_qty} -> {new_qty} @ {new_vwap}")
            
    except Exception as e:
        logger.error(f"Error updating position from fill: {e}")
        # Don't fail the fill if position update fails
        

async def execute_order_paper(order: OrderIn) -> Dict[str, Any]:
    """
    Execute paper order with IOC semantics against current orderbook + slippage.
    
    Returns:
        dict with order_id, filled, avg_px, status
    """
    logger.info(f"Executing paper order: {order}")
    
    try:
        # Get token ID if not provided
        token_id = order.token_id
        if not token_id:
            token_id = await _get_yes_token_id(order.market_id)
            if not token_id:
                return {
                    "order_id": None,
                    "filled": 0.0,
                    "avg_px": None,
                    "status": "rejected_no_token"
                }
        
        # Fetch current orderbook
        books = await get_books_batch([token_id])
        l1 = books.get(token_id)
        
        if not l1:
            return {
                "order_id": None,
                "filled": 0.0,
                "avg_px": None,
                "status": "rejected_no_book"
            }
        
        # Determine reference price and available depth
        if order.side == "YES":
            # Buying YES: start from ask price
            ref_price = l1.ask_yes if l1.ask_yes is not None else (
                l1.bid_yes + 0.02 if l1.bid_yes is not None else 0.51
            )
            depth = float(l1.ask_sz_usdc) / ref_price if ref_price > 0 else 0.0  # Convert USDC depth to shares
        else:
            # TODO: Implement NO side later
            return {
                "order_id": None,
                "filled": 0.0,
                "avg_px": None,
                "status": "rejected_side_not_supported"
            }
        
        # Calculate effective price with slippage
        slippage_amount = _slippage(order.qty, depth)
        effective_price = ref_price + slippage_amount
        
        # Clamp to valid price range [0.01, 0.99]
        effective_price = max(0.01, min(0.99, effective_price))
        
        logger.info(f"Price calculation: ref={ref_price:.4f}, slip={slippage_amount:.4f}, eff={effective_price:.4f}")
        
        # Check limit price (IOC semantics)
        if order.limit_price is not None:
            if effective_price > order.limit_price:
                return {
                    "order_id": None,
                    "filled": 0.0,
                    "avg_px": None,
                    "status": "rejected_limit"
                }
        
        # Risk checks
        risk_check = await perform_full_risk_check(order.market_id, order.qty, effective_price)
        if not risk_check.allowed:
            return {
                "order_id": None,
                "filled": 0.0,
                "avg_px": None,
                "status": f"rejected_risk: {risk_check.reason}"
            }
        
        # Calculate fees
        fee_bps = int(settings.taker_fee_bps)
        notional_value = effective_price * order.qty
        fee_usdc = notional_value * (fee_bps / 10000.0)
        
        # Create order record
        order_data = {
            "market_id": order.market_id,
            "token_id": token_id,
            "side": order.side,
            "qty": order.qty,
            "limit_price": order.limit_price,
            "tif": order.tif,
            "mode": "paper",
            "comment": order.comment,
            "config_snapshot": {
                "ref_price": ref_price,
                "slippage": slippage_amount,
                "depth": depth,
                "fee_bps": fee_bps
            },
            "ts": datetime.now(timezone.utc).isoformat()
        }
        
        order_result = await supabase.insert(table="orders", data=order_data)
        order_id = order_result[0]["id"] if order_result else None
        
        if not order_id:
            raise RuntimeError("Failed to create order record")
        
        # Create fill record
        fill_data = {
            "order_id": order_id,
            "market_id": order.market_id,
            "token_id": token_id,
            "side": order.side,
            "qty": order.qty,
            "price": effective_price,
            "fee_bps": fee_bps,
            "fee_usdc": fee_usdc,
            "ts": datetime.now(timezone.utc).isoformat()
        }
        
        await supabase.insert(table="fills", data=fill_data)
        
        # Update positions
        await _update_positions_from_fill(
            order.market_id, token_id, order.side, order.qty, effective_price, fee_usdc
        )
        
        logger.info(f"Paper order executed successfully: {order.qty} @ {effective_price:.4f}, fee: {fee_usdc:.2f}")
        
        return {
            "order_id": order_id,
            "filled": order.qty,
            "avg_px": effective_price,
            "status": "filled",
            "fee_usdc": fee_usdc
        }
        
    except Exception as e:
        logger.error(f"Error executing paper order: {e}")
        return {
            "order_id": None,
            "filled": 0.0,
            "avg_px": None,
            "status": f"error: {str(e)}"
        }


async def execute_order_live(order: OrderIn) -> Dict[str, Any]:
    """
    Execute live order (placeholder for real CLOB integration).
    Currently returns not implemented.
    """
    if not settings.live_clob_enabled:
        return {
            "order_id": None,
            "filled": 0.0,
            "avg_px": None,
            "status": "rejected_live_disabled"
        }
    
    # TODO: Implement live CLOB integration
    logger.warning("Live execution not yet implemented")
    return {
        "order_id": None,
        "filled": 0.0,
        "avg_px": None,
        "status": "not_implemented"
    }


async def execute_order(order: OrderIn) -> Dict[str, Any]:
    """
    Execute order using configured execution mode.
    """
    if settings.execution_mode == "live":
        return await execute_order_live(order)
    else:
        return await execute_order_paper(order)