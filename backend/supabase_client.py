import httpx
from urllib.parse import quote
from typing import Dict, Any, List, Optional

try:
    from .config import settings
except ImportError:
    from config import settings


class SupabaseClient:
    """Minimal Supabase client wrapper using httpx for PostgREST API calls.

    Notes:
    - Accepts both `filters` and legacy `eq` keyword arguments for convenience.
    - Adds simple support for `is_null` filters on select.
    """

    def __init__(self):
        self.base_url = f"{settings.supabase_url}/rest/v1"
        # Use service role for both Authorization and apikey in server-side context
        self.headers = {
            "apikey": settings.supabase_service_role or settings.supabase_anon,
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
        # Backward-compat/ergonomics
        eq: Optional[Dict[str, Any]] = None,
        is_null: Optional[Dict[str, bool]] = None,
        where: Optional[list] = None,  # list of tuples: (column, op, value) with op in {eq,gt,gte,lt,lte,like,ilike}
    ) -> List[Dict[str, Any]]:
        """Select data from a table."""
        url = f"{self.base_url}/{table}?select={select}"

        # Support both `filters` and legacy `eq` kwargs
        merged_filters: Dict[str, Any] = {}
        if filters:
            merged_filters.update(filters)
        if eq:
            merged_filters.update(eq)

        if merged_filters:
            for key, value in merged_filters.items():
                url += f"&{key}=eq.{quote(str(value), safe='')}"

        # Handle IS NULL / NOT IS NULL filters
        if is_null:
            for key, flag in is_null.items():
                url += f"&{key}={'is.null' if flag else 'not.is.null'}"
        
        # Handle explicit where operator tuples
        if where:
            for cond in where:
                if not isinstance(cond, (list, tuple)) or len(cond) != 3:
                    continue
                col, op, val = cond
                url += f"&{col}={op}.{quote(str(val), safe='')}"

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
        self,
        table: str,
        data: Dict[str, Any],
        filters: Optional[Dict[str, Any]] = None,
        # Backward-compat alias: allow callers to pass eq={...}
        eq: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Update data in a table."""
        url = f"{self.base_url}/{table}"

        merged_filters: Dict[str, Any] = {}
        if filters:
            merged_filters.update(filters)
        if eq:
            merged_filters.update(eq)

        for key, value in merged_filters.items():
            url += f"?{key}=eq.{quote(str(value), safe='')}"

        headers = {**self.headers, "Prefer": "return=representation"}

        async with httpx.AsyncClient() as client:
            response = await client.patch(url, json=data, headers=headers)
            response.raise_for_status()
            return response.json()


# Global client instance
supabase = SupabaseClient()
