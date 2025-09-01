"""Minimal Supabase helper for upserting market cache rows."""

from __future__ import annotations

import httpx
from typing import Any, Dict, List

try:
    from ..config import settings  # type: ignore
except Exception:  # pragma: no cover
    settings = None


def _base_url() -> str:
    assert settings is not None, "backend.config.settings is required"
    return f"{settings.supabase_url}/rest/v1"


def _headers(prefer_return: str = "minimal") -> Dict[str, str]:
    assert settings is not None, "backend.config.settings is required"
    # Use service role for both Authorization and apikey in server-side context
    return {
        "apikey": settings.supabase_service_role,
        "Authorization": f"Bearer {settings.supabase_service_role}",
        "Content-Type": "application/json",
        "Prefer": f"resolution=merge-duplicates,return={prefer_return}",
    }


async def upsert_markets_cache(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    url = f"{_base_url()}/markets_cache?on_conflict=id"
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json=rows, headers=_headers())
        resp.raise_for_status()

