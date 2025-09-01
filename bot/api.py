"""Backend API client for the Telegram bot."""

import httpx
import json
import os
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

BASE = os.getenv("BACKEND_BASE_URL", "http://localhost:8003")
SCAN_PARAMS = json.loads(os.getenv("SCAN_PARAMS_JSON", "{}"))

DEFAULT_SCAN_PARAMS = {
    "lambda_gain": 0.10,
    "threshold": 0.04,
    "lambda_days": 5,
    "orb_limits": {"square": 8, "opposition": 8, "conjunction": 6},
    "K_cap": 5.0
}

SCAN_PARAMS = {**DEFAULT_SCAN_PARAMS, **SCAN_PARAMS}


async def scan_quarter(quarter: str) -> Dict[str, Any]:
    """Scan a quarter for opportunities."""
    async with httpx.AsyncClient(timeout=120) as client:
        try:
            logger.info(f"Scanning quarter {quarter} with params: {SCAN_PARAMS}")
            r = await client.post(
                f"{BASE}/scan-quarter", 
                json={"quarter": quarter, **SCAN_PARAMS}
            )
            r.raise_for_status()
            result = r.json()
            logger.info(f"Scan completed for {quarter}: {len(result.get('opportunities', []))} opportunities")
            return result
        except Exception as e:
            logger.error(f"Error scanning quarter {quarter}: {e}")
            raise


async def get_opportunity_detail(opp_id: str, quarter: str) -> Dict[str, Any]:
    """Get opportunity details with contributions."""
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.get(
                f"{BASE}/opportunities/{opp_id}", 
                params={"quarter": quarter}
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Error fetching opportunity {opp_id}: {e}")
            raise


async def get_aspects(quarter: str) -> List[Dict[str, Any]]:
    """Get aspects for a quarter."""
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.get(
                f"{BASE}/astrology/aspects", 
                params={"quarter": quarter}
            )
            r.raise_for_status()
            result = r.json()
            return result.get("aspects", []) if isinstance(result, dict) else result
        except Exception as e:
            logger.error(f"Error fetching aspects for {quarter}: {e}")
            raise


async def health_check() -> bool:
    """Check if backend is healthy."""
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            r = await client.get(f"{BASE}/health")
            return r.status_code == 200
        except Exception as e:
            logger.error(f"Backend health check failed: {e}")
            return False


async def get_positions() -> List[Dict[str, Any]]:
    """Get current trading positions."""
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.get(f"{BASE}/positions")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            raise


async def get_pnl() -> Dict[str, Any]:
    """Get current P&L snapshot."""
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.get(f"{BASE}/pnl")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Error fetching P&L: {e}")
            raise


async def get_recent_fills(limit: int = 10) -> List[Dict[str, Any]]:
    """Get recent fills for notifications."""
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            r = await client.get(f"{BASE}/fills", params={"limit": limit})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Error fetching fills: {e}")
            raise


async def place_order(market_id: str, side: str, qty: float, limit_price: float = None, comment: str = None) -> Dict[str, Any]:
    """Place a paper trading order."""
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            payload = {
                "market_id": market_id,
                "side": side,
                "qty": qty
            }
            if limit_price is not None:
                payload["limit_price"] = limit_price
            if comment:
                payload["comment"] = comment
                
            r = await client.post(f"{BASE}/orders/place", json=payload)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Error placing order: {e}")
            raise


async def start_backtest(name: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Start a backtest run."""
    async with httpx.AsyncClient(timeout=120) as client:
        try:
            payload = {
                "name": name,
                **config
            }
            r = await client.post(f"{BASE}/backtest/start", json=payload)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Error starting backtest: {e}")
            raise


async def stop_backtest(test_run_id: str) -> Dict[str, Any]:
    """Stop a running backtest."""
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.post(f"{BASE}/backtest/{test_run_id}/stop")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Error stopping backtest: {e}")
            raise


async def get_backtest_status(test_run_id: str) -> Dict[str, Any]:
    """Get backtest status."""
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.get(f"{BASE}/backtest/{test_run_id}/status")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Error getting backtest status: {e}")
            raise


async def list_backtests(limit: int = 10) -> List[Dict[str, Any]]:
    """List recent backtest runs."""
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.get(f"{BASE}/backtest/runs", params={"limit": limit})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Error listing backtests: {e}")
            raise


async def get_kpis(test_run_id: str = None) -> Dict[str, Any]:
    """Get portfolio KPIs."""
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            params = {}
            if test_run_id:
                params["test_run_id"] = test_run_id
            r = await client.get(f"{BASE}/kpis", params=params)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Error getting KPIs: {e}")
            raise