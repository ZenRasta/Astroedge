"""Comprehensive tests for astrology aspect computation."""

import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
import time

from backend.services.quarters import (
    parse_quarter, format_quarter, get_current_quarter,
    get_next_quarter, get_previous_quarter, quarter_contains_date
)
from backend.services.astrology import AstrologyEngine, get_engine, compute_discordant_aspects
from backend.services.supabase_repo import get_repo
from backend.schemas import AspectEventIn, OrbLimits


class TestQuarterParsing:
    """Test quarter parsing utilities."""
    
    def test_parse_quarter_q1(self):
        """Test Q1 parsing."""
        start, end = parse_quarter("2025-Q1")
        assert start == datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert end == datetime(2025, 4, 1, tzinfo=timezone.utc)
    
    def test_parse_quarter_q3(self):
        """Test Q3 parsing."""
        start, end = parse_quarter("2025-Q3")
        assert start == datetime(2025, 7, 1, tzinfo=timezone.utc)
        assert end == datetime(2025, 10, 1, tzinfo=timezone.utc)
    
    def test_parse_quarter_q4_year_wrap(self):
        """Test Q4 parsing wrapping to next year."""
        start, end = parse_quarter("2024-Q4")
        assert start == datetime(2024, 10, 1, tzinfo=timezone.utc)
        assert end == datetime(2025, 1, 1, tzinfo=timezone.utc)
    
    def test_parse_quarter_invalid_format(self):
        """Test invalid quarter formats."""
        with pytest.raises(ValueError, match="Invalid quarter format"):
            parse_quarter("2025-Q5")
        
        with pytest.raises(ValueError, match="Invalid quarter format"):
            parse_quarter("2025Q1")
            
        with pytest.raises(ValueError, match="Invalid quarter format"):
            parse_quarter("Q1-2025")
    
    def test_format_quarter(self):
        """Test datetime to quarter conversion."""
        assert format_quarter(datetime(2025, 1, 15)) == "2025-Q1"
        assert format_quarter(datetime(2025, 4, 1)) == "2025-Q2"
        assert format_quarter(datetime(2025, 8, 15)) == "2025-Q3"
        assert format_quarter(datetime(2025, 12, 31)) == "2025-Q4"
    
    def test_quarter_navigation(self):
        """Test quarter navigation functions."""
        assert get_next_quarter("2025-Q3") == "2025-Q4"
        assert get_next_quarter("2025-Q4") == "2026-Q1"
        
        assert get_previous_quarter("2025-Q3") == "2025-Q2"
        assert get_previous_quarter("2025-Q1") == "2024-Q4"
    
    def test_quarter_contains_date(self):
        """Test quarter date containment."""
        assert quarter_contains_date("2025-Q3", datetime(2025, 8, 15, tzinfo=timezone.utc))
        assert not quarter_contains_date("2025-Q3", datetime(2025, 6, 15, tzinfo=timezone.utc))
        assert not quarter_contains_date("2025-Q3", datetime(2025, 10, 1, tzinfo=timezone.utc))  # exclusive end


class TestAstrologyEngine:
    """Test core astrology engine functionality."""
    
    @pytest.fixture
    def engine(self):
        """Get test astrology engine."""
        return AstrologyEngine()
    
    def test_engine_initialization(self, engine):
        """Test engine can initialize properly."""
        # Don't initialize in test by default due to download time
        assert not engine._initialized
        assert engine.ephemeris_file == 'de440s.bsp'
    
    def test_wrap_degrees(self):
        """Test angle wrapping utility."""
        assert AstrologyEngine.wrap_deg(0) == 0
        assert AstrologyEngine.wrap_deg(360) == 0
        assert AstrologyEngine.wrap_deg(370) == 10
        assert AstrologyEngine.wrap_deg(-10) == 350
        assert AstrologyEngine.wrap_deg(720) == 0
    
    def test_delta_to_target(self):
        """Test aspect angle delta calculation."""
        # Test conjunction (target = 0°)
        assert abs(AstrologyEngine.delta_to_target(0, 0)) < 0.001
        assert abs(AstrologyEngine.delta_to_target(1, 0) - 1) < 0.001
        assert abs(AstrologyEngine.delta_to_target(359, 0) - (-1)) < 0.001
        
        # Test square (target = 90°)
        assert abs(AstrologyEngine.delta_to_target(90, 90)) < 0.001
        assert abs(AstrologyEngine.delta_to_target(92, 90) - 2) < 0.001
        assert abs(AstrologyEngine.delta_to_target(88, 90) - (-2)) < 0.001
        
        # Test opposition (target = 180°)
        assert abs(AstrologyEngine.delta_to_target(180, 180)) < 0.001
        assert abs(AstrologyEngine.delta_to_target(182, 180) - 2) < 0.001
        assert abs(AstrologyEngine.delta_to_target(178, 180) - (-2)) < 0.001
        
        # Test wrap-around cases
        assert abs(AstrologyEngine.delta_to_target(2, 358) - 4) < 0.001  # Close conjunction across 0°
        assert abs(AstrologyEngine.delta_to_target(358, 2) - (-4)) < 0.001
    
    def test_canonical_pair_ordering(self, engine):
        """Test planet pair canonical ordering."""
        # SUN comes before MOON
        assert engine.canonical_pair('MOON', 'SUN') == ('SUN', 'MOON')
        assert engine.canonical_pair('SUN', 'MOON') == ('SUN', 'MOON')
        
        # MARS comes before JUPITER
        assert engine.canonical_pair('JUPITER', 'MARS') == ('MARS', 'JUPITER')
        assert engine.canonical_pair('MARS', 'JUPITER') == ('MARS', 'JUPITER')
        
        # PLUTO comes last
        assert engine.canonical_pair('PLUTO', 'SUN') == ('SUN', 'PLUTO')


class TestAstrologyComputation:
    """Test aspect computation with real ephemeris (slow tests)."""
    
    @pytest.fixture
    def initialized_engine(self):
        """Get initialized astrology engine."""
        engine = get_engine()
        if not engine._initialized:
            engine.initialize()
        return engine
    
    @pytest.mark.slow
    def test_engine_can_compute_positions(self, initialized_engine):
        """Test that engine can compute planetary positions."""
        from skyfield.api import load
        
        ts = load.timescale()
        t = ts.utc(2025, 1, 1, 12, 0)  # New Year's Day 2025, noon UTC
        
        # Test Sun position
        sun_lon = initialized_engine.ecl_lon_deg(t, 'SUN')
        assert 0 <= sun_lon < 360
        
        # Test Moon position  
        moon_lon = initialized_engine.ecl_lon_deg(t, 'MOON')
        assert 0 <= moon_lon < 360
        
        # Test Moon latitude
        moon_lat = initialized_engine.moon_ecliptic_lat_deg(t)
        assert -6 <= moon_lat <= 6  # Moon's latitude is bounded
    
    @pytest.mark.slow
    def test_orb_calculation(self, initialized_engine):
        """Test orb calculation between planets."""
        from skyfield.api import load
        
        ts = load.timescale()
        t = ts.utc(2025, 1, 1, 12, 0)
        
        # Compute orb between Sun and Moon for conjunction
        orb = initialized_engine.compute_orb(t, 'SUN', 'MOON', 0.0)
        assert 0 <= orb <= 180  # Orb should be valid
    
    @pytest.mark.slow
    @pytest.mark.timeout(600)  # 10 minute timeout for full computation
    def test_compute_quarter_aspects_basic(self):
        """Test computing aspects for a quarter (minimal test)."""
        # Use a recent quarter to avoid edge cases
        quarter = "2025-Q1"
        
        # Use small orb limits to reduce computation time
        orb_limits = {"conjunction": 3.0, "square": 4.0, "opposition": 4.0}
        
        events = compute_discordant_aspects(quarter, orb_limits)
        
        # Basic validation
        assert isinstance(events, list)
        
        if events:  # If any aspects found
            # Check first event structure
            event = events[0]
            assert isinstance(event, AspectEventIn)
            assert event.quarter == quarter
            assert event.start_utc < event.peak_utc < event.end_utc
            assert event.aspect in ['conjunction', 'square', 'opposition']
            assert event.severity in ['major', 'minor']
            assert 0 <= event.orb_deg <= max(orb_limits.values())
            
            # Check that peak is within quarter
            q_start, q_end = parse_quarter(quarter)
            assert q_start <= event.peak_utc < q_end
            
            # Check canonical ordering
            engine = get_engine()
            expected_p1, expected_p2 = engine.canonical_pair(event.planet1, event.planet2)
            assert (event.planet1, event.planet2) == (expected_p1, expected_p2)
    
    @pytest.mark.slow
    def test_aspect_severity_classification(self):
        """Test that aspects are classified by severity correctly."""
        quarter = "2025-Q1"
        orb_limits = {"conjunction": 6.0, "square": 8.0, "opposition": 8.0}
        
        events = compute_discordant_aspects(quarter, orb_limits)
        
        if events:
            for event in events:
                if event.orb_deg <= 1.0:
                    assert event.severity == 'major'
                else:
                    assert event.severity == 'minor'
    
    @pytest.mark.slow
    def test_sun_moon_eclipse_detection(self):
        """Test eclipse detection for Sun-Moon aspects."""
        # This is hard to test without knowing exact eclipse dates
        # Just verify the structure works
        quarter = "2025-Q1"
        
        events = compute_discordant_aspects(quarter)
        
        sun_moon_aspects = [
            e for e in events 
            if {e.planet1, e.planet2} == {'SUN', 'MOON'}
            and e.aspect in ['conjunction', 'opposition']
        ]
        
        # Check that eclipse detection ran (is_eclipse is either True or False)
        for event in sun_moon_aspects:
            assert isinstance(event.is_eclipse, bool)
            if event.is_eclipse:
                assert event.notes == "near node"


class TestAspectRepository:
    """Test database repository operations."""
    
    @pytest.mark.asyncio
    async def test_repository_health_check(self):
        """Test repository health check."""
        repo = get_repo()
        health = await repo.health_check()
        assert isinstance(health, bool)
    
    @pytest.mark.asyncio
    async def test_count_aspect_events(self):
        """Test counting aspect events."""
        repo = get_repo()
        count = await repo.count_aspect_events()
        assert isinstance(count, int)
        assert count >= 0
    
    @pytest.mark.asyncio
    async def test_fetch_empty_quarter(self):
        """Test fetching from non-existent quarter."""
        repo = get_repo()
        # Use a quarter far in the future that definitely has no data
        events = await repo.fetch_aspect_events(quarter="2099-Q1")
        assert events == []


@pytest.mark.integration
class TestAspectIntegration:
    """Integration tests for full aspect generation pipeline."""
    
    @pytest.mark.asyncio
    async def test_compute_and_store_aspects(self):
        """Test computing and storing aspects (idempotency check)."""
        quarter = "2030-Q1"  # Use future quarter to avoid conflicts
        
        repo = get_repo()
        
        # Clean up any existing data
        await repo.delete_aspect_events(quarter)
        
        # First generation
        events1 = compute_discordant_aspects(quarter, {"conjunction": 3.0, "square": 4.0, "opposition": 4.0})
        if events1:
            count1 = await repo.upsert_aspect_events(events1)
            assert count1 > 0
            
            # Second generation (should be idempotent)
            events2 = compute_discordant_aspects(quarter, {"conjunction": 3.0, "square": 4.0, "opposition": 4.0})
            count2 = await repo.upsert_aspect_events(events2)
            
            # Check idempotency (counts should match)
            stored_events = await repo.fetch_aspect_events(quarter=quarter)
            assert len(stored_events) == len(events1)
            
            # Clean up
            await repo.delete_aspect_events(quarter)
    
    @pytest.mark.asyncio
    async def test_aspect_summary_generation(self):
        """Test summary statistics generation."""
        quarter = "2030-Q2"  # Use future quarter to avoid conflicts
        
        repo = get_repo()
        
        # Clean up
        await repo.delete_aspect_events(quarter)
        
        # Generate and store some test aspects
        events = compute_discordant_aspects(quarter, {"conjunction": 2.0, "square": 3.0, "opposition": 3.0})
        
        if events:
            await repo.upsert_aspect_events(events)
            
            summary = await repo.get_aspect_summary(quarter)
            
            assert summary.total_aspects == len(events)
            assert isinstance(summary.by_severity, dict)
            assert isinstance(summary.by_aspect_type, dict)
            assert summary.average_orb >= 0
            
            # Clean up
            await repo.delete_aspect_events(quarter)


class TestPerformance:
    """Performance and timing tests."""
    
    @pytest.mark.slow
    def test_computation_performance(self):
        """Test that computation completes within reasonable time."""
        quarter = "2025-Q2"
        start_time = time.time()
        
        # Use conservative orb limits to reduce computation
        orb_limits = {"conjunction": 4.0, "square": 5.0, "opposition": 5.0}
        
        events = compute_discordant_aspects(quarter, orb_limits)
        
        elapsed = time.time() - start_time
        
        # Should complete within 10 minutes (600s) even on slow systems
        assert elapsed < 600, f"Computation took {elapsed:.1f}s, expected < 600s"
        
        # Log performance info
        print(f"Computed {len(events)} aspects in {elapsed:.2f}s")
        if events:
            print(f"Performance: {len(events)/elapsed:.2f} aspects/second")


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    def test_invalid_planet_name(self):
        """Test handling of invalid planet names."""
        engine = AstrologyEngine()
        
        with pytest.raises((KeyError, ValueError)):
            # This should fail during initialization or computation
            engine.canonical_pair('INVALID', 'SUN')
    
    def test_invalid_quarter_computation(self):
        """Test computation with invalid quarter."""
        with pytest.raises(ValueError):
            compute_discordant_aspects("invalid-quarter")
    
    @pytest.mark.asyncio
    async def test_repository_error_handling(self):
        """Test repository error handling with invalid data."""
        repo = get_repo()
        
        # Test with empty events list
        result = await repo.upsert_aspect_events([])
        assert result == 0


# Fixtures for database testing
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


if __name__ == "__main__":
    # Allow running specific test categories
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--fast":
            # Run only fast tests
            pytest.main([__file__, "-v", "-m", "not slow"])
        elif sys.argv[1] == "--slow": 
            # Run only slow tests
            pytest.main([__file__, "-v", "-m", "slow"])
        elif sys.argv[1] == "--integration":
            # Run integration tests
            pytest.main([__file__, "-v", "-m", "integration"])
        else:
            pytest.main([__file__, "-v"])
    else:
        pytest.main([__file__, "-v"])