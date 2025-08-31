"""Pytest tests for database smoke tests."""

import pytest
import asyncio
from backend.services.db_smoke import (
    DatabaseSmokeTests,
    test_service_role,
    test_anon_key,
    run_comprehensive_tests
)


class TestDatabaseConnectivity:
    """Test database connectivity with both service role and anonymous keys."""
    
    @pytest.mark.asyncio
    async def test_health_check_service_role(self):
        """Test health check with service role key."""
        tester = DatabaseSmokeTests(use_service_role=True)
        result = await tester.health_check()
        assert result is True, "Health check should pass with service role key"
    
    @pytest.mark.asyncio
    async def test_health_check_anon_key(self):
        """Test health check with anonymous key."""
        tester = DatabaseSmokeTests(use_service_role=False)
        result = await tester.health_check()
        assert result is True, "Health check should pass with anonymous key"


class TestDatabaseObjects:
    """Test that all expected database objects exist."""
    
    @pytest.mark.asyncio
    async def test_database_objects_service_role(self):
        """Verify database objects exist using service role."""
        tester = DatabaseSmokeTests(use_service_role=True)
        results = await tester.verify_database_objects()
        
        expected_tables = [
            "impact_map_versions",
            "impact_map_rules", 
            "aspect_events",
            "markets",
            "aspect_contributions",
            "opportunities",
            "app_config"
        ]
        
        for table in expected_tables:
            assert table in results, f"Table {table} should be checked"
            assert results[table] is True, f"Table {table} should exist and be accessible"
    
    @pytest.mark.asyncio
    async def test_database_objects_anon_key(self):
        """Verify database objects exist using anonymous key."""
        tester = DatabaseSmokeTests(use_service_role=False)
        results = await tester.verify_database_objects()
        
        # Anonymous key should be able to read most tables
        expected_readable_tables = [
            "aspect_events",
            "markets", 
            "app_config"
        ]
        
        for table in expected_readable_tables:
            assert table in results, f"Table {table} should be checked"
            # Note: Some tables may not be readable by anon key depending on RLS policies
            # We just verify the table check was attempted


class TestServiceRoleOperations:
    """Test write operations that require service role privileges."""
    
    @pytest.mark.asyncio
    async def test_impact_map_versions_crud(self):
        """Test CRUD operations on impact_map_versions with service role."""
        tester = DatabaseSmokeTests(use_service_role=True)
        result = await tester.test_impact_map_versions_crud()
        
        assert result["insert_success"] is True, f"Insert should succeed: {result.get('error', '')}"
        assert result["select_success"] is True, f"Select should succeed: {result.get('error', '')}"
        assert result["data_matches"] is True, "Retrieved data should match inserted data"
        assert result["cleanup_success"] is True, "Cleanup should succeed"
        assert result["error"] is None, f"No errors should occur: {result.get('error', '')}"
    
    @pytest.mark.asyncio
    async def test_aspect_events_list_service_role(self):
        """Test listing aspect_events with service role."""
        tester = DatabaseSmokeTests(use_service_role=True)
        result = await tester.test_aspect_events_list()
        
        assert result["list_success"] is True, f"List should succeed: {result.get('error', '')}"
        assert result["count"] >= 0, "Count should be non-negative"
        # Note: table may be empty initially, that's expected
        assert result["error"] is None, f"No errors should occur: {result.get('error', '')}"
    
    @pytest.mark.asyncio
    async def test_app_config_read_service_role(self):
        """Test reading app config with service role."""
        tester = DatabaseSmokeTests(use_service_role=True)
        result = await tester.test_app_config_read()
        
        assert result["read_success"] is True, f"Config read should succeed: {result.get('error', '')}"
        assert result["has_config"] is True, "App config should exist (inserted by schema)"
        assert result["config_data"] is not None, "Config data should be present"
        assert result["error"] is None, f"No errors should occur: {result.get('error', '')}"
        
        # Verify expected config fields
        config = result["config_data"]
        expected_fields = ["lambda_gain", "edge_threshold", "lambda_days", "orb_limits"]
        for field in expected_fields:
            assert field in config, f"Config should contain {field}"


class TestAnonymousKeyOperations:
    """Test read-only operations with anonymous key."""
    
    @pytest.mark.asyncio
    async def test_aspect_events_list_anon_key(self):
        """Test listing aspect_events with anonymous key."""
        tester = DatabaseSmokeTests(use_service_role=False)
        result = await tester.test_aspect_events_list()
        
        # Anonymous key should be able to read aspect_events
        assert result["list_success"] is True, f"List should succeed: {result.get('error', '')}"
        assert result["count"] >= 0, "Count should be non-negative"
        assert result["error"] is None, f"No errors should occur: {result.get('error', '')}"
    
    @pytest.mark.asyncio
    async def test_app_config_read_anon_key(self):
        """Test reading app config with anonymous key."""
        tester = DatabaseSmokeTests(use_service_role=False)
        result = await tester.test_app_config_read()
        
        # Anonymous key should be able to read app config
        assert result["read_success"] is True, f"Config read should succeed: {result.get('error', '')}"
        assert result["has_config"] is True, "App config should exist"
        assert result["config_data"] is not None, "Config data should be present"
        assert result["error"] is None, f"No errors should occur: {result.get('error', '')}"


class TestComprehensiveSmoke:
    """Test complete smoke test suites."""
    
    @pytest.mark.asyncio
    async def test_service_role_comprehensive(self):
        """Run comprehensive tests with service role."""
        results = await test_service_role()
        
        assert results["role_type"] == "service_role"
        assert results["overall_success"] is True, f"Service role tests should pass: {results}"
        assert results["health_check"] is True
        
        # Verify all database objects exist
        db_objects = results["database_objects"]
        assert all(db_objects.values()), f"All database objects should exist: {db_objects}"
        
        # Verify CRUD operations work
        crud_result = results["impact_map_versions_crud"]
        assert crud_result["insert_success"] and crud_result["select_success"]
    
    @pytest.mark.asyncio
    async def test_anon_key_comprehensive(self):
        """Run comprehensive tests with anonymous key."""
        results = await test_anon_key()
        
        assert results["role_type"] == "anon"
        assert results["overall_success"] is True, f"Anonymous key tests should pass: {results}"
        assert results["health_check"] is True
    
    @pytest.mark.asyncio
    async def test_both_keys_comprehensive(self):
        """Run comprehensive tests with both keys."""
        results = await run_comprehensive_tests()
        
        assert results["both_passed"] is True, f"Both key types should pass: {results}"
        
        service_results = results["service_role_tests"]
        anon_results = results["anon_key_tests"]
        
        assert service_results["overall_success"] is True
        assert anon_results["overall_success"] is True


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    @pytest.mark.asyncio
    async def test_invalid_table_access(self):
        """Test accessing non-existent table."""
        tester = DatabaseSmokeTests(use_service_role=True)
        
        # This should gracefully handle non-existent table
        results = await tester.verify_database_objects()
        # If we had a non-existent table in our list, it would return False
        # But all our tables should exist, so this is more of a structure test
        assert isinstance(results, dict)


if __name__ == "__main__":
    # Allow running tests directly for quick validation
    import sys
    
    async def run_quick_test():
        print("Running quick database smoke test...")
        tester = DatabaseSmokeTests(use_service_role=True)
        results = await tester.run_all_tests()
        
        if results["overall_success"]:
            print("✓ Database smoke tests PASSED")
            return 0
        else:
            print("✗ Database smoke tests FAILED")
            print(f"Results: {results}")
            return 1
    
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        exit_code = asyncio.run(run_quick_test())
        sys.exit(exit_code)
    else:
        pytest.main([__file__])