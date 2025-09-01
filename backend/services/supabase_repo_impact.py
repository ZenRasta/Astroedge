"""Supabase repository adapter for impact map operations."""

import logging
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from uuid import uuid4

import httpx

try:
    from ..config import settings
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from config import settings

logger = logging.getLogger(__name__)


@dataclass
class RuleRow:
    """Represents a single rule row for impact_map_rules table."""
    planet1: str
    planet2: str
    aspect: str
    category: str
    weight: int


class SupabaseImpactRepository:
    """Repository for impact map operations."""
    
    def __init__(self):
        self.base_url = f"{settings.supabase_url}/rest/v1"
        self.headers = {
            "apikey": settings.supabase_service_role,
            "Authorization": f"Bearer {settings.supabase_service_role}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }

    async def insert_impact_map_version(self, json_blob: Dict[str, Dict[str, int]], notes: Optional[str], is_active: bool) -> str:
        """Insert a new impact map version and return the version ID."""
        payload = {
            "id": str(uuid4()),
            "json_blob": json_blob, 
            "notes": notes, 
            "is_active": is_active
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/impact_map_versions",
                json=payload,
                headers=self.headers,
                timeout=30.0
            )
            response.raise_for_status()
            data = response.json()
            
            if not data:
                raise RuntimeError("Failed to insert impact_map_version")
            
            return data[0]["id"]

    async def set_only_version_active(self, version_id: str) -> None:
        """Set only the specified version as active, deactivating all others."""
        async with httpx.AsyncClient() as client:
            # Deactivate all other versions
            await client.patch(
                f"{self.base_url}/impact_map_versions?id=neq.{version_id}",
                json={"is_active": False},
                headers=self.headers,
                timeout=30.0
            )
            
            # Ensure this one is active
            response = await client.patch(
                f"{self.base_url}/impact_map_versions?id=eq.{version_id}",
                json={"is_active": True},
                headers=self.headers,
                timeout=30.0
            )
            response.raise_for_status()

    async def insert_rules_bulk(self, version_id: str, rows: List[RuleRow]) -> int:
        """Insert multiple rule rows and return the count inserted."""
        if not rows:
            return 0
        
        payload = [{
            "version_id": version_id,
            "planet1": r.planet1, 
            "planet2": r.planet2,
            "aspect": r.aspect, 
            "category": r.category,
            "weight": r.weight
        } for r in rows]
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/impact_map_rules",
                json=payload,
                headers=self.headers,
                timeout=30.0
            )
            response.raise_for_status()
            data = response.json()
            
            return len(data or [])

    async def get_active_map_version_with_json(self) -> dict:
        """Get the active impact map version with its JSON blob."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/impact_map_versions?is_active=eq.true&order=created_at.desc&limit=1",
                headers=self.headers,
                timeout=30.0
            )
            response.raise_for_status()
            data = response.json()
            
            if not data:
                return {"version_id": None, "created_at": None, "notes": None, "map": {}}
            
            row = data[0]
            return {
                "version_id": row["id"],
                "created_at": row["created_at"],
                "notes": row.get("notes"),
                "map": row["json_blob"]
            }

    async def fetch_rules_for_version(self, version_id: str, planet1: str, planet2: str, aspect: str, tags: List[str]) -> List[dict]:
        """Fetch rules for a specific version, planet pair, aspect, and category tags."""
        params = [
            f"version_id=eq.{version_id}",
            f"planet1=eq.{planet1}",
            f"planet2=eq.{planet2}",
            f"aspect=eq.{aspect}"
        ]
        
        if tags:
            # PostgREST 'in' syntax
            tags_str = ",".join(tags)
            params.append(f"category=in.({tags_str})")
        
        url = f"{self.base_url}/impact_map_rules?" + "&".join(params)
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers, timeout=30.0)
            response.raise_for_status()
            
            return response.json() or []


# Global repository instance
_repo: Optional[SupabaseImpactRepository] = None


def get_repo() -> SupabaseImpactRepository:
    """Get global repository instance."""
    global _repo
    if _repo is None:
        _repo = SupabaseImpactRepository()
    return _repo


# Sync wrapper functions for backward compatibility
def insert_impact_map_version(json_blob: Dict[str, Dict[str, int]], notes: Optional[str], is_active: bool) -> str:
    """Insert a new impact map version and return the version ID."""
    import asyncio
    repo = get_repo()
    return asyncio.run(repo.insert_impact_map_version(json_blob, notes, is_active))


def set_only_version_active(version_id: str) -> None:
    """Set only the specified version as active, deactivating all others."""
    import asyncio
    repo = get_repo()
    return asyncio.run(repo.set_only_version_active(version_id))


def insert_rules_bulk(version_id: str, rows: List[RuleRow]) -> int:
    """Insert multiple rule rows and return the count inserted."""
    import asyncio
    repo = get_repo()
    return asyncio.run(repo.insert_rules_bulk(version_id, rows))


def get_active_map_version_with_json() -> dict:
    """Get the active impact map version with its JSON blob."""
    import asyncio
    repo = get_repo()
    return asyncio.run(repo.get_active_map_version_with_json())


def fetch_rules_for_version(version_id: str, planet1: str, planet2: str, aspect: str, tags: List[str]) -> List[dict]:
    """Fetch rules for a specific version, planet pair, aspect, and category tags."""
    import asyncio
    repo = get_repo()
    return asyncio.run(repo.fetch_rules_for_version(version_id, planet1, planet2, aspect, tags))