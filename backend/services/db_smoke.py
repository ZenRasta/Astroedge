"""Database smoke tests for AstroEdge."""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
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


class DatabaseSmokeTests:
    """Database connectivity and basic CRUD operation tests."""
    
    def __init__(self, use_service_role: bool = True):
        """Initialize with either service role or anonymous key."""
        self.use_service_role = use_service_role
        self.base_url = f"{settings.supabase_url}/rest/v1"
        
        if use_service_role:
            self.headers = {
                "apikey": settings.supabase_service_role,
                "Authorization": f"Bearer {settings.supabase_service_role}",
                "Content-Type": "application/json",
                "Prefer": "return=representation"
            }
        else:
            self.headers = {
                "apikey": settings.supabase_anon,
                "Authorization": f"Bearer {settings.supabase_anon}",
                "Content-Type": "application/json",
                "Prefer": "return=representation"
            }
    
    async def health_check(self) -> bool:
        """Basic connectivity test."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{settings.supabase_url}/health",
                    headers={"apikey": self.headers["apikey"]},
                    timeout=10.0
                )
                return response.status_code == 200
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    async def verify_database_objects(self) -> Dict[str, bool]:
        """Verify that all expected database objects exist."""
        results = {}
        
        # Test each table by attempting to query it
        tables_to_check = [
            "impact_map_versions",
            "impact_map_rules", 
            "aspect_events",
            "markets",
            "aspect_contributions",
            "opportunities",
            "app_config"
        ]
        
        async with httpx.AsyncClient() as client:
            for table in tables_to_check:
                try:
                    url = f"{self.base_url}/{table}?limit=1"
                    response = await client.get(url, headers=self.headers, timeout=10.0)
                    results[table] = response.status_code == 200
                except Exception as e:
                    logger.error(f"Failed to verify table {table}: {e}")
                    results[table] = False
        
        return results
    
    async def test_impact_map_versions_crud(self) -> Dict[str, Any]:
        """Test insert/select operations on impact_map_versions."""
        test_id = str(uuid4())
        test_data = {
            "id": test_id,
            "is_active": False,
            "notes": f"Smoke test - {datetime.now(timezone.utc).isoformat()}",
            "json_blob": {
                "test": True,
                "created_by": "db_smoke_test",
                "rules": [
                    {
                        "planets": ["MARS", "SATURN"],
                        "aspect": "square", 
                        "category": "conflict",
                        "weight": 3
                    }
                ]
            }
        }
        
        result = {
            "insert_success": False,
            "select_success": False,
            "data_matches": False,
            "cleanup_success": False,
            "error": None
        }
        
        async with httpx.AsyncClient() as client:
            try:
                # INSERT test
                insert_response = await client.post(
                    f"{self.base_url}/impact_map_versions",
                    json=test_data,
                    headers=self.headers,
                    timeout=10.0
                )
                
                if insert_response.status_code in [200, 201]:
                    result["insert_success"] = True
                    inserted_data = insert_response.json()
                    if isinstance(inserted_data, list) and len(inserted_data) > 0:
                        inserted_data = inserted_data[0]
                    
                    # SELECT test
                    select_response = await client.get(
                        f"{self.base_url}/impact_map_versions?id=eq.{test_id}",
                        headers=self.headers,
                        timeout=10.0
                    )
                    
                    if select_response.status_code == 200:
                        result["select_success"] = True
                        selected_data = select_response.json()
                        
                        if len(selected_data) > 0:
                            selected_item = selected_data[0]
                            result["data_matches"] = (
                                selected_item["notes"] == test_data["notes"] and
                                selected_item["json_blob"]["test"] is True
                            )
                    
                    # CLEANUP - Delete test record
                    delete_response = await client.delete(
                        f"{self.base_url}/impact_map_versions?id=eq.{test_id}",
                        headers=self.headers,
                        timeout=10.0
                    )
                    result["cleanup_success"] = delete_response.status_code in [200, 204]
                else:
                    result["error"] = f"Insert failed with status {insert_response.status_code}: {insert_response.text}"
                    
            except Exception as e:
                result["error"] = str(e)
                
        return result
    
    async def test_aspect_events_list(self) -> Dict[str, Any]:
        """Test listing aspect_events (should be empty initially)."""
        result = {
            "list_success": False,
            "is_empty": False,
            "count": 0,
            "error": None
        }
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{self.base_url}/aspect_events?limit=100",
                    headers=self.headers,
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    result["list_success"] = True
                    data = response.json()
                    result["count"] = len(data)
                    result["is_empty"] = len(data) == 0
                else:
                    result["error"] = f"List failed with status {response.status_code}: {response.text}"
                    
            except Exception as e:
                result["error"] = str(e)
                
        return result
    
    async def test_app_config_read(self) -> Dict[str, Any]:
        """Test reading app configuration."""
        result = {
            "read_success": False,
            "has_config": False,
            "config_data": None,
            "error": None
        }
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{self.base_url}/app_config?id=eq.1",
                    headers=self.headers,
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    result["read_success"] = True
                    data = response.json()
                    if len(data) > 0:
                        result["has_config"] = True
                        result["config_data"] = data[0]
                else:
                    result["error"] = f"Config read failed with status {response.status_code}: {response.text}"
                    
            except Exception as e:
                result["error"] = str(e)
                
        return result
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all smoke tests and return comprehensive results."""
        role_type = "service_role" if self.use_service_role else "anon"
        
        results = {
            "role_type": role_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "health_check": False,
            "database_objects": {},
            "impact_map_versions_crud": {},
            "aspect_events_list": {},
            "app_config_read": {},
            "overall_success": False
        }
        
        try:
            # Run all tests
            results["health_check"] = await self.health_check()
            results["database_objects"] = await self.verify_database_objects()
            
            if self.use_service_role:
                # Only test write operations with service role
                results["impact_map_versions_crud"] = await self.test_impact_map_versions_crud()
            
            results["aspect_events_list"] = await self.test_aspect_events_list()
            results["app_config_read"] = await self.test_app_config_read()
            
            # Determine overall success
            critical_tests = [
                results["health_check"],
                all(results["database_objects"].values()),
                results["aspect_events_list"]["list_success"],
                results["app_config_read"]["read_success"]
            ]
            
            if self.use_service_role:
                critical_tests.append(
                    results["impact_map_versions_crud"]["insert_success"] and 
                    results["impact_map_versions_crud"]["select_success"]
                )
            
            results["overall_success"] = all(critical_tests)
            
        except Exception as e:
            results["error"] = str(e)
            logger.error(f"Smoke tests failed: {e}")
        
        return results


# Convenience functions for easy testing
async def test_service_role() -> Dict[str, Any]:
    """Test database connectivity with service role key."""
    tester = DatabaseSmokeTests(use_service_role=True)
    return await tester.run_all_tests()


async def test_anon_key() -> Dict[str, Any]:
    """Test database connectivity with anonymous key."""
    tester = DatabaseSmokeTests(use_service_role=False) 
    return await tester.run_all_tests()


async def run_comprehensive_tests() -> Dict[str, Any]:
    """Run tests with both service role and anonymous keys."""
    service_results = await test_service_role()
    anon_results = await test_anon_key()
    
    return {
        "service_role_tests": service_results,
        "anon_key_tests": anon_results,
        "both_passed": service_results["overall_success"] and anon_results["overall_success"]
    }


if __name__ == "__main__":
    async def main():
        print("Running database smoke tests...")
        results = await run_comprehensive_tests()
        
        print(f"\nService Role Tests: {'✓ PASS' if results['service_role_tests']['overall_success'] else '✗ FAIL'}")
        print(f"Anonymous Key Tests: {'✓ PASS' if results['anon_key_tests']['overall_success'] else '✗ FAIL'}")
        print(f"Overall: {'✓ ALL TESTS PASSED' if results['both_passed'] else '✗ SOME TESTS FAILED'}")
        
        return results
    
    asyncio.run(main())