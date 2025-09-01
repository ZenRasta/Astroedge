"""Risk management system for trading operations."""

import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass

try:
    from ..config import settings
    from ..supabase_client import supabase
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from config import settings
    from supabase_client import supabase

logger = logging.getLogger(__name__)


@dataclass
class RiskCheck:
    """Risk check result."""
    allowed: bool
    reason: Optional[str] = None
    current_exposure: Optional[float] = None
    limit: Optional[float] = None


async def apply_daily_breaker() -> bool:
    """Apply daily loss breaker if drawdown exceeds limit."""
    try:
        # Get today's start time
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Get equity curve entries since today start
        equity_data = await supabase.select(
            table="equity_curve",
            select="equity_usdc,ts",
            # Note: would need to add filter support to custom client
        )
        
        if not equity_data:
            logger.info("No equity data found for daily breaker check")
            return True
        
        # Filter to today's data (doing client-side filtering due to simple supabase client)
        today_equity = [
            e for e in equity_data 
            if datetime.fromisoformat(e["ts"].replace('Z', '+00:00')) >= today_start
        ]
        
        if len(today_equity) < 2:
            return True
        
        # Calculate daily drawdown
        day_start_equity = today_equity[0]["equity_usdc"]
        current_equity = today_equity[-1]["equity_usdc"]
        daily_pnl = float(current_equity) - float(day_start_equity)
        
        max_drawdown = float(settings.daily_max_drawdown_usdc)
        
        if daily_pnl < -max_drawdown:
            logger.warning(f"Daily drawdown breaker triggered: {daily_pnl:.2f} < -{max_drawdown:.2f}")
            
            # Disable trading
            await supabase.update(
                table="risk_state",
                data={"trading_enabled": False, "updated_at": datetime.now(timezone.utc).isoformat()},
                filters={"id": 1}
            )
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error in daily breaker check: {e}")
        # Fail safe - allow trading if check fails
        return True


async def check_trading_enabled() -> RiskCheck:
    """Check if trading is enabled."""
    try:
        risk_states = await supabase.select(
            table="risk_state",
            select="trading_enabled",
            filters={"id": 1}
        )
        
        if not risk_states:
            # Initialize risk state if missing
            await supabase.insert(
                table="risk_state",
                data={"id": 1, "trading_enabled": True}
            )
            return RiskCheck(allowed=True)
        
        enabled = risk_states[0].get("trading_enabled", True)
        
        if not enabled:
            return RiskCheck(
                allowed=False,
                reason="Trading disabled by risk controls"
            )
        
        return RiskCheck(allowed=True)
        
    except Exception as e:
        logger.error(f"Error checking trading enabled: {e}")
        # Fail safe - allow trading if check fails
        return RiskCheck(allowed=True)


async def check_market_exposure_limit(market_id: str, additional_qty: float, price: float) -> RiskCheck:
    """Check per-market exposure limit."""
    try:
        # Get current positions for this market
        positions = await supabase.select(
            table="positions",
            select="qty,vwap",
            filters={"market_id": market_id}
        )
        
        current_exposure = 0.0
        if positions:
            pos = positions[0]
            current_exposure = float(pos["qty"]) * float(pos["vwap"])
        
        additional_exposure = additional_qty * price
        total_exposure = current_exposure + additional_exposure
        
        limit = float(settings.max_per_market_usdc)
        
        if total_exposure > limit:
            return RiskCheck(
                allowed=False,
                reason=f"Market exposure limit exceeded",
                current_exposure=current_exposure,
                limit=limit
            )
        
        return RiskCheck(
            allowed=True,
            current_exposure=current_exposure,
            limit=limit
        )
        
    except Exception as e:
        logger.error(f"Error checking market exposure for {market_id}: {e}")
        # Fail safe - allow if check fails
        return RiskCheck(allowed=True)


async def check_theme_exposure_limit(market_id: str, additional_qty: float, price: float) -> RiskCheck:
    """Check per-theme (category) exposure limit."""
    try:
        # Get market categories
        markets = await supabase.select(
            table="markets",
            select="category_tags",
            filters={"id": market_id}
        )
        
        if not markets or not markets[0].get("category_tags"):
            # No categories, skip check
            return RiskCheck(allowed=True)
        
        market_categories = markets[0]["category_tags"]
        if isinstance(market_categories, str):
            import json
            try:
                market_categories = json.loads(market_categories)
            except:
                market_categories = []
        
        if not market_categories:
            return RiskCheck(allowed=True)
        
        # Get all markets with overlapping categories
        all_markets = await supabase.select(
            table="markets",
            select="id,category_tags"
        )
        
        # Find markets with shared categories
        related_market_ids = set()
        for market in all_markets:
            market_tags = market.get("category_tags", [])
            if isinstance(market_tags, str):
                import json
                try:
                    market_tags = json.loads(market_tags)
                except:
                    market_tags = []
            
            # Check for overlap
            if any(tag in market_categories for tag in market_tags):
                related_market_ids.add(market["id"])
        
        # Calculate total exposure across theme
        theme_exposure = 0.0
        if related_market_ids:
            positions = await supabase.select(
                table="positions",
                select="market_id,qty,vwap"
            )
            
            for pos in positions:
                if pos["market_id"] in related_market_ids:
                    theme_exposure += float(pos["qty"]) * float(pos["vwap"])
        
        additional_exposure = additional_qty * price
        total_theme_exposure = theme_exposure + additional_exposure
        
        limit = float(settings.max_per_theme_usdc)
        
        if total_theme_exposure > limit:
            return RiskCheck(
                allowed=False,
                reason=f"Theme exposure limit exceeded for categories: {market_categories}",
                current_exposure=theme_exposure,
                limit=limit
            )
        
        return RiskCheck(
            allowed=True,
            current_exposure=theme_exposure,
            limit=limit
        )
        
    except Exception as e:
        logger.error(f"Error checking theme exposure for {market_id}: {e}")
        # Fail safe - allow if check fails
        return RiskCheck(allowed=True)


async def check_size_fraction_limit(qty: float, price: float) -> RiskCheck:
    """Check position size as fraction of equity."""
    try:
        # Get latest equity
        equity_data = await supabase.select(
            table="equity_curve",
            select="equity_usdc"
            # Would need to add order by support to get latest
        )
        
        if not equity_data:
            # No equity data, use conservative default
            current_equity = 10000.0
        else:
            # Get the latest entry (assuming sorted by timestamp)
            current_equity = float(equity_data[-1]["equity_usdc"])
            if current_equity <= 0:
                current_equity = 10000.0  # Conservative default
        
        order_value = qty * price
        size_fraction = order_value / current_equity
        
        max_fraction = float(settings.max_size_fraction)
        
        if size_fraction > max_fraction:
            return RiskCheck(
                allowed=False,
                reason=f"Position size fraction exceeded: {size_fraction:.3f} > {max_fraction:.3f}",
                current_exposure=order_value,
                limit=current_equity * max_fraction
            )
        
        return RiskCheck(
            allowed=True,
            current_exposure=order_value,
            limit=current_equity * max_fraction
        )
        
    except Exception as e:
        logger.error(f"Error checking size fraction: {e}")
        # Fail safe - allow if check fails
        return RiskCheck(allowed=True)


async def perform_full_risk_check(market_id: str, qty: float, price: float) -> RiskCheck:
    """Perform all risk checks for an order."""
    
    # Apply daily breaker first
    if not await apply_daily_breaker():
        return RiskCheck(
            allowed=False,
            reason="Daily loss limit reached - trading suspended"
        )
    
    # Check if trading is enabled
    enabled_check = await check_trading_enabled()
    if not enabled_check.allowed:
        return enabled_check
    
    # Check market exposure limit
    market_check = await check_market_exposure_limit(market_id, qty, price)
    if not market_check.allowed:
        return market_check
    
    # Check theme exposure limit
    theme_check = await check_theme_exposure_limit(market_id, qty, price)
    if not theme_check.allowed:
        return theme_check
    
    # Check size fraction limit
    size_check = await check_size_fraction_limit(qty, price)
    if not size_check.allowed:
        return size_check
    
    logger.info(f"Risk checks passed for {market_id}: qty={qty}, price={price}")
    return RiskCheck(allowed=True)


async def enable_trading() -> bool:
    """Manually enable trading (admin function)."""
    try:
        await supabase.update(
            table="risk_state",
            data={"trading_enabled": True, "updated_at": datetime.now(timezone.utc).isoformat()},
            filters={"id": 1}
        )
        logger.info("Trading manually enabled")
        return True
    except Exception as e:
        logger.error(f"Error enabling trading: {e}")
        return False


async def disable_trading(reason: str = "Manual disable") -> bool:
    """Manually disable trading (admin function)."""
    try:
        await supabase.update(
            table="risk_state",
            data={"trading_enabled": False, "updated_at": datetime.now(timezone.utc).isoformat()},
            filters={"id": 1}
        )
        logger.warning(f"Trading disabled: {reason}")
        return True
    except Exception as e:
        logger.error(f"Error disabling trading: {e}")
        return False