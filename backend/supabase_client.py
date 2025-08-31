import httpx
from typing import Dict, Any, List, Optional

try:
    from .config import settings
except ImportError:
    from config import settings


class SupabaseClient:
    """Minimal Supabase client wrapper using httpx for PostgREST API calls."""

    def __init__(self):
        self.base_url = f"{settings.supabase_url}/rest/v1"
        self.headers = {
            "apikey": settings.supabase_anon,
            "Authorization": f"Bearer {settings.supabase_service_role}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

    async def health_check(self) -> bool:
        """Simple health check against Supabase."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{settings.supabase_url}/health",
                    headers={"apikey": settings.supabase_anon},
                    timeout=10.0,
                )
                return response.status_code == 200
        except Exception:
            return False

    async def select(
        self,
        table: str,
        select: str = "*",
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Select data from a table."""
        url = f"{self.base_url}/{table}?select={select}"

        if filters:
            for key, value in filters.items():
                url += f"&{key}=eq.{value}"

        if limit:
            url += f"&limit={limit}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()

    async def insert(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert data into a table."""
        url = f"{self.base_url}/{table}"
        headers = {**self.headers, "Prefer": "return=representation"}

        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=data, headers=headers)
            response.raise_for_status()
            return response.json()

    async def update(
        self, table: str, data: Dict[str, Any], filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update data in a table."""
        url = f"{self.base_url}/{table}"

        for key, value in filters.items():
            url += f"?{key}=eq.{value}"

        headers = {**self.headers, "Prefer": "return=representation"}

        async with httpx.AsyncClient() as client:
            response = await client.patch(url, json=data, headers=headers)
            response.raise_for_status()
            return response.json()


# Global client instance
supabase = SupabaseClient()
