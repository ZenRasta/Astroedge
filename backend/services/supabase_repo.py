"""Supabase repository for aspect events and related data."""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from uuid import uuid4

import httpx

try:
    from ..config import settings
    from ..schemas import AspectEventIn, AspectEventOut, AspectSummary
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from config import settings
    from schemas import AspectEventIn, AspectEventOut, AspectSummary

logger = logging.getLogger(__name__)


class SupabaseAspectRepository:
    """Repository for aspect_events table operations."""
    
    def __init__(self):
        self.base_url = f"{settings.supabase_url}/rest/v1"
        self.headers = {
            "apikey": settings.supabase_service_role,
            "Authorization": f"Bearer {settings.supabase_service_role}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
    
    async def upsert_aspect_events(self, events: List[AspectEventIn]) -> int:
        """Upsert aspect events into database.
        
        Uses unique constraint on (quarter, planet1, planet2, aspect, peak_utc)
        to prevent duplicates and enable idempotent operations.
        
        Args:
            events: List of AspectEventIn objects to upsert
            
        Returns:
            Number of rows inserted or updated
        """
        if not events:
            return 0
            
        logger.info(f"Upserting {len(events)} aspect events")
        
        # Convert events to database format
        db_records = []
        for event in events:
            record = {
                "id": str(uuid4()),  # Will be ignored on conflict
                "quarter": event.quarter,
                "start_utc": event.start_utc.isoformat(),
                "peak_utc": event.peak_utc.isoformat(),
                "end_utc": event.end_utc.isoformat(),
                "planet1": event.planet1,
                "planet2": event.planet2,
                "aspect": event.aspect,
                "orb_deg": event.orb_deg,
                "severity": event.severity,
                "is_eclipse": event.is_eclipse,
                "notes": event.notes,
                "source": event.source,
                "confidence": event.confidence
            }
            db_records.append(record)
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/aspect_events",
                json=db_records,
                headers={
                    **self.headers,
                    "Prefer": "resolution=merge-duplicates,return=minimal"
                },
                timeout=30.0
            )
            
            if response.status_code not in [200, 201]:
                logger.error(f"Upsert failed: {response.status_code} {response.text}")
                response.raise_for_status()
        
        logger.info(f"Successfully upserted {len(events)} aspect events")
        return len(events)
    
    async def fetch_aspect_events(
        self,
        quarter: Optional[str] = None,
        planet1: Optional[str] = None,
        planet2: Optional[str] = None,
        aspect: Optional[str] = None,
        severity: Optional[str] = None,
        is_eclipse: Optional[bool] = None,
        limit: Optional[int] = None,
        order_by: str = "peak_utc"
    ) -> List[AspectEventOut]:
        """Fetch aspect events with optional filters.
        
        Args:
            quarter: Filter by quarter (e.g., "2025-Q3")
            planet1: Filter by first planet
            planet2: Filter by second planet  
            aspect: Filter by aspect type
            severity: Filter by severity (major/minor)
            is_eclipse: Filter by eclipse flag
            limit: Maximum number of records
            order_by: Column to order by (default: peak_utc)
            
        Returns:
            List of AspectEventOut objects
        """
        url = f"{self.base_url}/aspect_events"
        params = []
        
        # Build query parameters
        if quarter:
            params.append(f"quarter=eq.{quarter}")
        if planet1:
            params.append(f"planet1=eq.{planet1}")
        if planet2:
            params.append(f"planet2=eq.{planet2}")
        if aspect:
            params.append(f"aspect=eq.{aspect}")
        if severity:
            params.append(f"severity=eq.{severity}")
        if is_eclipse is not None:
            params.append(f"is_eclipse=eq.{is_eclipse}")
        if limit:
            params.append(f"limit={limit}")
        
        params.append(f"order={order_by}")
        
        if params:
            url += "?" + "&".join(params)
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers, timeout=30.0)
            response.raise_for_status()
            
            data = response.json()
            
            # Convert to Pydantic models
            events = []
            for record in data:
                # Parse datetime strings
                for dt_field in ['start_utc', 'peak_utc', 'end_utc', 'created_at', 'updated_at']:
                    if record[dt_field]:
                        record[dt_field] = datetime.fromisoformat(record[dt_field].replace('Z', '+00:00'))
                
                events.append(AspectEventOut(**record))
            
            logger.info(f"Fetched {len(events)} aspect events")
            return events
    
    async def get_aspect_summary(self, quarter: str) -> AspectSummary:
        """Get summary statistics for aspects in a quarter.
        
        Args:
            quarter: Quarter string
            
        Returns:
            AspectSummary with statistics
        """
        events = await self.fetch_aspect_events(quarter=quarter)
        
        if not events:
            return AspectSummary(
                total_aspects=0,
                by_severity={},
                by_aspect_type={},
                by_planet_pairs={},
                eclipse_count=0,
                average_orb=0.0,
                date_range={}
            )
        
        # Calculate statistics
        by_severity = {}
        by_aspect_type = {}
        by_planet_pairs = {}
        eclipse_count = 0
        total_orb = 0.0
        
        earliest_peak = events[0].peak_utc
        latest_peak = events[0].peak_utc
        
        for event in events:
            # Severity counts
            by_severity[event.severity] = by_severity.get(event.severity, 0) + 1
            
            # Aspect type counts
            by_aspect_type[event.aspect] = by_aspect_type.get(event.aspect, 0) + 1
            
            # Planet pair counts
            pair_key = f"{event.planet1}-{event.planet2}"
            by_planet_pairs[pair_key] = by_planet_pairs.get(pair_key, 0) + 1
            
            # Eclipse count
            if event.is_eclipse:
                eclipse_count += 1
            
            # Orb sum
            total_orb += event.orb_deg
            
            # Date range
            if event.peak_utc < earliest_peak:
                earliest_peak = event.peak_utc
            if event.peak_utc > latest_peak:
                latest_peak = event.peak_utc
        
        return AspectSummary(
            total_aspects=len(events),
            by_severity=by_severity,
            by_aspect_type=by_aspect_type,
            by_planet_pairs=by_planet_pairs,
            eclipse_count=eclipse_count,
            average_orb=round(total_orb / len(events), 3),
            date_range={
                "earliest": earliest_peak,
                "latest": latest_peak
            }
        )
    
    async def delete_aspect_events(self, quarter: str) -> int:
        """Delete all aspect events for a quarter.
        
        Args:
            quarter: Quarter string
            
        Returns:
            Number of deleted records
        """
        logger.warning(f"Deleting all aspect events for quarter {quarter}")
        
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f"{self.base_url}/aspect_events?quarter=eq.{quarter}",
                headers=self.headers,
                timeout=30.0
            )
            response.raise_for_status()
        
        logger.info(f"Deleted aspect events for quarter {quarter}")
        # Note: Supabase doesn't return count on DELETE, so we return 0
        return 0
    
    async def count_aspect_events(self, quarter: Optional[str] = None) -> int:
        """Count aspect events.
        
        Args:
            quarter: Optional quarter filter
            
        Returns:
            Number of aspect events
        """
        url = f"{self.base_url}/aspect_events?select=count"
        
        if quarter:
            url += f"&quarter=eq.{quarter}"
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers={**self.headers, "Prefer": "count=exact"},
                timeout=30.0
            )
            response.raise_for_status()
            
            # Supabase returns count in Content-Range header
            content_range = response.headers.get('content-range', '')
            if content_range:
                # Format: "0-24/25" -> extract total count
                total = content_range.split('/')[-1]
                return int(total) if total != '*' else 0
            
            return 0
    
    async def health_check(self) -> bool:
        """Check if aspect_events table is accessible.
        
        Returns:
            True if table is accessible
        """
        try:
            await self.count_aspect_events()
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False


# Global repository instance
_repo: Optional[SupabaseAspectRepository] = None


def get_repo() -> SupabaseAspectRepository:
    """Get global repository instance."""
    global _repo
    if _repo is None:
        _repo = SupabaseAspectRepository()
    return _repo


# Convenience functions
async def upsert_aspect_events(events: List[AspectEventIn]) -> int:
    """Upsert aspect events using global repository."""
    repo = get_repo()
    return await repo.upsert_aspect_events(events)


async def fetch_aspect_events(quarter: str) -> List[AspectEventOut]:
    """Fetch aspect events for a quarter using global repository."""
    repo = get_repo()
    return await repo.fetch_aspect_events(quarter=quarter)