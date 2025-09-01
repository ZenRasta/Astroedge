"""Backtesting engine and runner for strategy evaluation."""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import asyncio
import uuid

try:
    from ..supabase_client import supabase
    from ..config import settings
    from .analytics import calculate_portfolio_kpis
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from supabase_client import supabase
    from config import settings
    from services.analytics import calculate_portfolio_kpis

logger = logging.getLogger(__name__)


class BacktestConfig:
    """Backtesting configuration."""
    
    def __init__(self, data: Dict[str, Any]):
        self.start_date = data.get("start_date")
        self.end_date = data.get("end_date") 
        self.initial_capital = data.get("initial_capital", 1000.0)
        self.scan_frequency = data.get("scan_frequency", "daily")  # daily, hourly
        self.execution_mode = data.get("execution_mode", "paper")
        self.fee_bps = data.get("fee_bps", 60)
        self.slippage_model = data.get("slippage_model", "linear")
        self.max_positions = data.get("max_positions", 10)
        self.max_position_size = data.get("max_position_size", 0.05)
        
        # Strategy parameters
        self.lambda_gain = data.get("lambda_gain", 0.10)
        self.threshold = data.get("threshold", 0.04)
        self.lambda_days = data.get("lambda_days", 5)
        self.orb_limits = data.get("orb_limits", {"square": 8, "opposition": 8, "conjunction": 6})
        self.k_cap = data.get("k_cap", 5.0)
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "initial_capital": self.initial_capital,
            "scan_frequency": self.scan_frequency,
            "execution_mode": self.execution_mode,
            "fee_bps": self.fee_bps,
            "slippage_model": self.slippage_model,
            "max_positions": self.max_positions,
            "max_position_size": self.max_position_size,
            "lambda_gain": self.lambda_gain,
            "threshold": self.threshold,
            "lambda_days": self.lambda_days,
            "orb_limits": self.orb_limits,
            "k_cap": self.k_cap
        }


class BacktestEngine:
    """Main backtesting engine."""
    
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.test_run_id = None
        self.current_positions = {}
        self.equity = config.initial_capital
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.total_fees = 0.0
        
    async def run(self, name: str) -> str:
        """
        Run the backtest.
        
        Args:
            name: Name for the backtest run
            
        Returns:
            test_run_id: UUID of the backtest run
        """
        try:
            # Create test run record
            self.test_run_id = str(uuid.uuid4())
            
            await supabase.insert(
                table="test_runs",
                data={
                    "id": self.test_run_id,
                    "name": name,
                    "type": "backtest",
                    "config": self.config.to_dict(),
                    "start_date": self.config.start_date,
                    "end_date": self.config.end_date,
                    "status": "running"
                }
            )
            
            logger.info(f"Starting backtest '{name}' with run_id {self.test_run_id}")
            
            # Generate scan schedule
            scan_dates = self._generate_scan_schedule()
            
            # Run through each scan date
            for i, scan_date in enumerate(scan_dates):
                try:
                    await self._process_scan_date(scan_date)
                    
                    # Update equity curve
                    await self._record_equity_snapshot(scan_date)
                    
                    # Periodic progress update
                    if i % 10 == 0:
                        progress = (i / len(scan_dates)) * 100
                        logger.info(f"Backtest progress: {progress:.1f}% - Equity: ${self.equity:.2f}")
                        
                except Exception as e:
                    logger.error(f"Error processing scan date {scan_date}: {e}")
                    continue
            
            # Calculate final metrics
            final_metrics = await self._calculate_final_metrics()
            
            # Update test run with completion
            await supabase.update(
                table="test_runs",
                data={
                    "status": "completed",
                    "end_date": datetime.utcnow().isoformat(),
                    "metrics": final_metrics
                },
                eq={"id": self.test_run_id}
            )
            
            logger.info(f"Backtest '{name}' completed. Final equity: ${self.equity:.2f}")
            return self.test_run_id
            
        except Exception as e:
            logger.error(f"Backtest failed: {e}")
            
            if self.test_run_id:
                await supabase.update(
                    table="test_runs",
                    data={
                        "status": "failed",
                        "end_date": datetime.utcnow().isoformat()
                    },
                    eq={"id": self.test_run_id}
                )
            
            raise
    
    def _generate_scan_schedule(self) -> List[datetime]:
        """Generate list of scan dates based on frequency."""
        start = datetime.fromisoformat(self.config.start_date.replace('Z', '+00:00'))
        end = datetime.fromisoformat(self.config.end_date.replace('Z', '+00:00'))
        
        dates = []
        current = start
        
        if self.config.scan_frequency == "daily":
            delta = timedelta(days=1)
        elif self.config.scan_frequency == "hourly":
            delta = timedelta(hours=1)
        else:
            delta = timedelta(days=1)  # Default to daily
        
        while current <= end:
            dates.append(current)
            current += delta
            
        return dates
    
    async def _process_scan_date(self, scan_date: datetime):
        """Process a single scan date."""
        # 1. Close expired positions
        await self._close_expired_positions(scan_date)
        
        # 2. Scan for opportunities
        opportunities = await self._scan_opportunities(scan_date)
        
        # 3. Execute trades based on opportunities
        await self._execute_opportunities(opportunities, scan_date)
        
        # 4. Update mark-to-market
        await self._update_mark_to_market(scan_date)
    
    async def _scan_opportunities(self, scan_date: datetime) -> List[Dict[str, Any]]:
        """Scan for trading opportunities at a given date."""
        try:
            # This would typically call the main scanning engine
            # For now, simulate with a simplified approach
            
            # Get markets active at scan_date
            quarter = self._get_quarter_for_date(scan_date)
            markets = await self._get_active_markets(scan_date, quarter)
            
            opportunities = []
            
            for market in markets[:20]:  # Limit for backtest performance
                try:
                    # Simulate opportunity calculation
                    opp = await self._calculate_opportunity(market, scan_date, quarter)
                    if opp:
                        opportunities.append(opp)
                        
                        # Record opportunity in database
                        await supabase.insert(
                            table="test_opportunities",
                            data={
                                "test_run_id": self.test_run_id,
                                "market_id": market["id"],
                                "scan_time": scan_date.isoformat(),
                                "p0": opp["p0"],
                                "p_astro": opp["p_astro"], 
                                "edge_net": opp["edge_net"],
                                "decision": opp["decision"],
                                "size_fraction": opp["size_fraction"],
                                "executed": False,
                                "metadata": {"market_title": market.get("title", "")}
                            }
                        )
                        
                except Exception as e:
                    logger.warning(f"Error calculating opportunity for market {market['id']}: {e}")
                    continue
            
            return opportunities
            
        except Exception as e:
            logger.error(f"Error scanning opportunities for {scan_date}: {e}")
            return []
    
    async def _calculate_opportunity(self, market: Dict[str, Any], scan_date: datetime, quarter: str) -> Optional[Dict[str, Any]]:
        """Calculate opportunity for a market at a specific date."""
        try:
            # Simplified opportunity calculation for backtest
            # In real implementation, this would call the full astrology engine
            
            market_id = market["id"]
            
            # Get historical price at scan_date (simulate with current price + noise)
            base_price = market.get("price_yes", 0.5)
            
            # Simulate astrology effect (random for backtest)
            import random
            astro_effect = random.uniform(-0.1, 0.1)
            p_astro = max(0.01, min(0.99, base_price + astro_effect))
            
            edge_net = abs(p_astro - base_price) - 0.02  # Subtract costs
            
            if edge_net >= self.config.threshold:
                decision = "BUY" if p_astro > base_price else "SELL"
                size_fraction = min(self.config.max_position_size, edge_net * 2)
                
                return {
                    "market_id": market_id,
                    "p0": base_price,
                    "p_astro": p_astro,
                    "edge_net": edge_net,
                    "decision": decision,
                    "size_fraction": size_fraction
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error calculating opportunity: {e}")
            return None
    
    async def _execute_opportunities(self, opportunities: List[Dict[str, Any]], scan_date: datetime):
        """Execute trades based on opportunities."""
        # Sort by edge and take top opportunities
        sorted_opps = sorted(opportunities, key=lambda x: x["edge_net"], reverse=True)
        
        executed_count = 0
        
        for opp in sorted_opps:
            # Check position limits
            if len(self.current_positions) >= self.config.max_positions:
                break
                
            # Check if already have position in this market
            if opp["market_id"] in self.current_positions:
                continue
            
            # Execute trade
            success = await self._execute_trade(opp, scan_date)
            if success:
                executed_count += 1
                
                # Update opportunity record
                await supabase.update(
                    table="test_opportunities",
                    data={
                        "executed": True,
                        "execution_price": opp["p0"],  # Simplified
                        "execution_qty": opp["size_fraction"] * self.equity / opp["p0"]
                    },
                    eq={
                        "test_run_id": self.test_run_id,
                        "market_id": opp["market_id"],
                        "scan_time": scan_date.isoformat()
                    }
                )
        
        if executed_count > 0:
            logger.debug(f"Executed {executed_count} trades on {scan_date.date()}")
    
    async def _execute_trade(self, opportunity: Dict[str, Any], scan_date: datetime) -> bool:
        """Execute a single trade."""
        try:
            market_id = opportunity["market_id"]
            side = "YES" if opportunity["decision"] == "BUY" else "NO"
            entry_price = opportunity["p0"]
            
            # Calculate position size
            position_value = opportunity["size_fraction"] * self.equity
            qty = position_value / entry_price
            
            # Calculate fees
            fees = position_value * (self.config.fee_bps / 10000.0)
            
            # Create trade record
            trade_id = str(uuid.uuid4())
            
            await supabase.insert(
                table="test_trades",
                data={
                    "id": trade_id,
                    "test_run_id": self.test_run_id,
                    "market_id": market_id,
                    "side": side,
                    "qty": qty,
                    "entry_price": entry_price,
                    "entry_time": scan_date.isoformat(),
                    "fees": fees,
                    "metadata": {
                        "opportunity": opportunity,
                        "position_value": position_value
                    }
                }
            )
            
            # Update internal position tracking
            self.current_positions[market_id] = {
                "trade_id": trade_id,
                "side": side,
                "qty": qty,
                "entry_price": entry_price,
                "entry_time": scan_date,
                "fees": fees
            }
            
            # Update equity for fees
            self.equity -= fees
            self.total_fees += fees
            
            return True
            
        except Exception as e:
            logger.error(f"Error executing trade: {e}")
            return False
    
    async def _close_expired_positions(self, current_date: datetime):
        """Close positions for markets that have expired."""
        expired_positions = []
        
        for market_id, position in self.current_positions.items():
            # Check if market has expired (simplified check)
            # In real implementation, check against market deadline
            days_held = (current_date - position["entry_time"]).days
            
            if days_held >= 30:  # Force close after 30 days for backtest
                expired_positions.append(market_id)
        
        for market_id in expired_positions:
            await self._close_position(market_id, current_date, "expired")
    
    async def _close_position(self, market_id: str, exit_date: datetime, reason: str = "manual"):
        """Close a position."""
        if market_id not in self.current_positions:
            return
        
        position = self.current_positions[market_id]
        
        # Simulate exit price (random outcome for backtest)
        import random
        if reason == "expired":
            # Market resolved - simulate random outcome
            exit_price = 1.0 if random.random() > 0.5 else 0.0
        else:
            # Market exit at current price (simplified)
            exit_price = position["entry_price"] * random.uniform(0.8, 1.2)
        
        # Calculate P&L
        entry_value = position["qty"] * position["entry_price"]
        exit_value = position["qty"] * exit_price
        pnl = exit_value - entry_value - position["fees"]
        
        # Update trade record
        await supabase.update(
            table="test_trades",
            data={
                "exit_price": exit_price,
                "exit_time": exit_date.isoformat(),
                "realized_pnl": pnl,
                "outcome": 1 if exit_price > 0.5 else 0
            },
            eq={"id": position["trade_id"]}
        )
        
        # Update equity
        self.equity += exit_value
        self.realized_pnl += pnl
        
        # Remove from positions
        del self.current_positions[market_id]
        
        logger.debug(f"Closed position in {market_id}: P&L ${pnl:.2f}")
    
    async def _update_mark_to_market(self, current_date: datetime):
        """Update mark-to-market values for open positions."""
        total_unrealized = 0.0
        
        for market_id, position in self.current_positions.items():
            # Simulate current mark price
            import random
            mark_price = position["entry_price"] * random.uniform(0.9, 1.1)
            
            current_value = position["qty"] * mark_price
            entry_value = position["qty"] * position["entry_price"]
            unrealized_pnl = current_value - entry_value
            
            total_unrealized += unrealized_pnl
        
        self.unrealized_pnl = total_unrealized
        self.equity = self.config.initial_capital + self.realized_pnl + self.unrealized_pnl - self.total_fees
    
    async def _record_equity_snapshot(self, timestamp: datetime):
        """Record equity curve snapshot."""
        await supabase.insert(
            table="test_equity",
            data={
                "test_run_id": self.test_run_id,
                "ts": timestamp.isoformat(),
                "equity_usdc": self.equity,
                "realized_pnl": self.realized_pnl,
                "unrealized_pnl": self.unrealized_pnl,
                "fees_usdc": self.total_fees,
                "positions_count": len(self.current_positions)
            }
        )
    
    async def _calculate_final_metrics(self) -> Dict[str, Any]:
        """Calculate final backtest metrics."""
        kpis = await calculate_portfolio_kpis(self.test_run_id)
        return kpis.to_dict()
    
    async def _get_active_markets(self, scan_date: datetime, quarter: str) -> List[Dict[str, Any]]:
        """Get markets active at a given date."""
        # Simplified - get markets for quarter
        # In real implementation, filter by deadline
        markets = await supabase.select(
            table="markets",
            select="id,title,description,deadline_utc,price_yes,liquidity_score"
        )
        
        # Filter markets that haven't expired yet
        active_markets = []
        for market in markets or []:
            try:
                deadline = datetime.fromisoformat(market["deadline_utc"].replace('Z', '+00:00'))
                if deadline > scan_date:
                    active_markets.append(market)
            except:
                continue
        
        return active_markets[:50]  # Limit for performance
    
    def _get_quarter_for_date(self, date: datetime) -> str:
        """Get quarter string for a date."""
        quarter = (date.month - 1) // 3 + 1
        return f"{date.year}-Q{quarter}"


async def run_backtest(name: str, config: Dict[str, Any]) -> str:
    """
    Run a backtest with given configuration.
    
    Args:
        name: Name for the backtest
        config: Backtest configuration
        
    Returns:
        test_run_id: UUID of the backtest run
    """
    backtest_config = BacktestConfig(config)
    engine = BacktestEngine(backtest_config)
    
    return await engine.run(name)


async def stop_backtest(test_run_id: str) -> bool:
    """
    Stop a running backtest.
    
    Args:
        test_run_id: UUID of the test run to stop
        
    Returns:
        success: True if stopped successfully
    """
    try:
        await supabase.update(
            table="test_runs",
            data={
                "status": "stopped",
                "end_date": datetime.utcnow().isoformat()
            },
            eq={"id": test_run_id}
        )
        
        logger.info(f"Stopped backtest {test_run_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error stopping backtest: {e}")
        return False


async def get_backtest_status(test_run_id: str) -> Optional[Dict[str, Any]]:
    """Get status of a backtest run."""
    try:
        runs = await supabase.select(
            table="test_runs",
            select="*",
            eq={"id": test_run_id}
        )
        
        return runs[0] if runs else None
        
    except Exception as e:
        logger.error(f"Error getting backtest status: {e}")
        return None


async def list_backtest_runs(limit: int = 20) -> List[Dict[str, Any]]:
    """List recent backtest runs."""
    try:
        runs = await supabase.select(
            table="test_runs",
            select="id,name,type,status,start_date,end_date,metrics,created_at"
        )
        
        # Sort by created_at desc (client side)
        if runs:
            runs = sorted(runs, key=lambda x: x["created_at"], reverse=True)[:limit]
        
        return runs or []
        
    except Exception as e:
        logger.error(f"Error listing backtest runs: {e}")
        return []