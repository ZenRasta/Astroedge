"""Astrology engine using Skyfield for planetary aspect calculations."""

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
from skyfield.api import load, Timescale, Time
from skyfield.framelib import ecliptic_frame

try:
    from .quarters import parse_quarter
    from ..schemas import AspectEventIn, OrbLimits
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from services.quarters import parse_quarter
    from schemas import AspectEventIn, OrbLimits

logger = logging.getLogger(__name__)

# Planet ordering for canonical pair sorting
PLANET_ORDER = [
    'SUN', 'MOON', 'MERCURY', 'VENUS', 'MARS',
    'JUPITER', 'SATURN', 'URANUS', 'NEPTUNE', 'PLUTO'
]

# Skyfield body mappings
SKYFIELD_BODIES = {
    'SUN': 'sun',
    'MOON': 'moon', 
    'MERCURY': 'mercury barycenter',
    'VENUS': 'venus barycenter',
    'MARS': 'mars barycenter',
    'JUPITER': 'jupiter barycenter',
    'SATURN': 'saturn barycenter',
    'URANUS': 'uranus barycenter',
    'NEPTUNE': 'neptune barycenter',
    'PLUTO': 'pluto barycenter',
}

# Aspect definitions
ASPECTS = {
    'conjunction': 0.0,
    'square': 90.0,
    'opposition': 180.0,
}


class AstrologyEngine:
    """Core astrology engine for planetary aspect calculations."""
    
    def __init__(self, ephemeris_file: str = 'de440s.bsp'):
        """Initialize the astrology engine.
        
        Args:
            ephemeris_file: Skyfield ephemeris file name (default: de440s.bsp)
        """
        self.ephemeris_file = ephemeris_file
        self.ts: Optional[Timescale] = None
        self.eph = None
        self.earth = None
        self.bodies: Dict[str, any] = {}
        self._initialized = False
        
    def initialize(self) -> None:
        """Initialize Skyfield components and load ephemeris."""
        if self._initialized:
            return
            
        logger.info(f"Loading Skyfield ephemeris: {self.ephemeris_file}")
        start_time = time.time()
        
        # Load timescale and ephemeris
        self.ts = load.timescale()
        self.eph = load(self.ephemeris_file)
        self.earth = self.eph['earth']
        
        # Load all planetary bodies
        for planet, skyfield_name in SKYFIELD_BODIES.items():
            try:
                self.bodies[planet] = self.eph[skyfield_name]
                logger.debug(f"Loaded {planet} -> {skyfield_name}")
            except KeyError as e:
                logger.error(f"Failed to load {planet} ({skyfield_name}): {e}")
                raise ValueError(f"Ephemeris missing body: {skyfield_name}")
        
        self._initialized = True
        load_time = time.time() - start_time
        logger.info(f"Astrology engine initialized in {load_time:.2f}s")
    
    def ecl_lon_deg(self, t: Time, planet: str) -> float:
        """Get geocentric ecliptic longitude for a planet at time t.
        
        Args:
            t: Skyfield Time object
            planet: Planet name (e.g., 'MARS')
            
        Returns:
            Ecliptic longitude in degrees [0, 360)
        """
        if not self._initialized:
            self.initialize()
            
        body = self.bodies[planet]
        astrometric = self.earth.at(t).observe(body)
        lat, lon, distance = astrometric.frame_latlon(ecliptic_frame)
        return self.wrap_deg(lon.degrees)
    
    def moon_ecliptic_lat_deg(self, t: Time) -> float:
        """Get geocentric ecliptic latitude for the Moon at time t.
        
        Args:
            t: Skyfield Time object
            
        Returns:
            Ecliptic latitude in degrees
        """
        if not self._initialized:
            self.initialize()
            
        astrometric = self.earth.at(t).observe(self.bodies['MOON'])
        lat, lon, distance = astrometric.frame_latlon(ecliptic_frame)
        return lat.degrees
    
    @staticmethod
    def wrap_deg(x: float) -> float:
        """Wrap angle to [0, 360) degrees."""
        return (x % 360.0 + 360.0) % 360.0
    
    @staticmethod
    def delta_to_target(long_diff: float, target: float) -> float:
        """Calculate smallest signed distance to target aspect angle.
        
        Args:
            long_diff: Longitude difference in degrees
            target: Target aspect angle (0, 90, 180)
            
        Returns:
            Signed distance to target in degrees [-180, +180]
        """
        d = ((long_diff - target + 180.0) % 360.0) - 180.0
        return d
    
    def canonical_pair(self, p1: str, p2: str) -> Tuple[str, str]:
        """Sort planet pair in canonical order.
        
        Args:
            p1, p2: Planet names
            
        Returns:
            Tuple of (planet1, planet2) in canonical order
        """
        i1, i2 = PLANET_ORDER.index(p1), PLANET_ORDER.index(p2)
        return (p1, p2) if i1 < i2 else (p2, p1)
    
    def compute_orb(self, t: Time, p1: str, p2: str, target: float) -> float:
        """Compute orb (deviation from exact aspect) at time t.
        
        Args:
            t: Skyfield Time object
            p1, p2: Planet names
            target: Target aspect angle
            
        Returns:
            Absolute orb in degrees
        """
        lon1 = self.ecl_lon_deg(t, p1)
        lon2 = self.ecl_lon_deg(t, p2)
        long_diff = self.wrap_deg(lon2 - lon1)
        delta = self.delta_to_target(long_diff, target)
        return abs(delta)
    
    def is_within_orb(self, t: Time, p1: str, p2: str, target: float, orb_limit: float) -> bool:
        """Check if planets are within orb limit for aspect.
        
        Args:
            t: Skyfield Time object
            p1, p2: Planet names
            target: Target aspect angle
            orb_limit: Maximum orb in degrees
            
        Returns:
            True if within orb limit
        """
        return self.compute_orb(t, p1, p2, target) <= orb_limit
    
    def scan_intervals(
        self,
        start_dt: datetime,
        end_dt: datetime,
        pair: Tuple[str, str],
        target: float,
        orb_limit: float,
        dt_hours: float
    ) -> List[Tuple[datetime, datetime]]:
        """Scan time range to find intervals where aspect is within orb.
        
        Args:
            start_dt, end_dt: Time range to scan
            pair: Planet pair tuple
            target: Target aspect angle
            orb_limit: Maximum orb in degrees
            dt_hours: Step size in hours
            
        Returns:
            List of (enter_time, exit_time) intervals
        """
        if not self._initialized:
            self.initialize()
            
        intervals = []
        current_time = start_dt
        dt_step = timedelta(hours=dt_hours)
        in_orb = False
        enter_time = None
        
        p1, p2 = pair
        
        while current_time < end_dt:
            t = self.ts.from_datetime(current_time)
            within_orb = self.is_within_orb(t, p1, p2, target, orb_limit)
            
            if within_orb and not in_orb:
                # Entering orb
                in_orb = True
                enter_time = current_time
            elif not within_orb and in_orb:
                # Exiting orb
                in_orb = False
                if enter_time:
                    intervals.append((enter_time, current_time))
                    enter_time = None
            
            current_time += dt_step
        
        # Handle case where we end while still in orb
        if in_orb and enter_time:
            intervals.append((enter_time, end_dt))
        
        return intervals
    
    def refine_boundary(
        self,
        t_start: datetime,
        t_end: datetime,
        pair: Tuple[str, str],
        target: float,
        orb_limit: float,
        entering: bool,
        tolerance_minutes: float = 2.0
    ) -> datetime:
        """Refine boundary crossing time using binary search.
        
        Args:
            t_start, t_end: Search bounds
            pair: Planet pair
            target: Target aspect angle
            orb_limit: Orb limit
            entering: True if entering orb, False if exiting
            tolerance_minutes: Stop when interval is this small
            
        Returns:
            Refined boundary time
        """
        p1, p2 = pair
        tolerance = timedelta(minutes=tolerance_minutes)
        
        while (t_end - t_start) > tolerance:
            t_mid = t_start + (t_end - t_start) / 2
            t_skyfield = self.ts.from_datetime(t_mid)
            within_orb = self.is_within_orb(t_skyfield, p1, p2, target, orb_limit)
            
            if entering:
                if within_orb:
                    t_end = t_mid  # Boundary is earlier
                else:
                    t_start = t_mid  # Boundary is later
            else:  # exiting
                if within_orb:
                    t_start = t_mid  # Boundary is later
                else:
                    t_end = t_mid  # Boundary is earlier
        
        return t_start + (t_end - t_start) / 2
    
    def find_peak(
        self,
        t_start: datetime,
        t_end: datetime,
        pair: Tuple[str, str],
        target: float,
        refine_minutes: float = 30.0
    ) -> Tuple[datetime, float]:
        """Find time of minimum orb (exact aspect) within interval.
        
        Args:
            t_start, t_end: Search interval
            pair: Planet pair
            target: Target aspect angle
            refine_minutes: Initial step size for refinement
            
        Returns:
            Tuple of (peak_time, minimum_orb)
        """
        p1, p2 = pair
        
        # Start with coarse grid search
        best_time = t_start
        best_orb = float('inf')
        step = timedelta(minutes=refine_minutes)
        
        current_time = t_start
        while current_time <= t_end:
            t_skyfield = self.ts.from_datetime(current_time)
            orb = self.compute_orb(t_skyfield, p1, p2, target)
            
            if orb < best_orb:
                best_orb = orb
                best_time = current_time
                
            current_time += step
        
        # Refine around best time with smaller steps
        step = timedelta(minutes=5.0)
        search_window = timedelta(minutes=refine_minutes)
        
        search_start = max(t_start, best_time - search_window)
        search_end = min(t_end, best_time + search_window)
        
        current_time = search_start
        while current_time <= search_end:
            t_skyfield = self.ts.from_datetime(current_time)
            orb = self.compute_orb(t_skyfield, p1, p2, target)
            
            if orb < best_orb:
                best_orb = orb
                best_time = current_time
                
            current_time += step
        
        # Round to nearest minute for consistency
        rounded_time = best_time.replace(second=0, microsecond=0)
        rounded_time += timedelta(minutes=round(best_time.second / 60))
        
        # Recalculate orb at rounded time
        t_final = self.ts.from_datetime(rounded_time)
        final_orb = self.compute_orb(t_final, p1, p2, target)
        
        return rounded_time, final_orb
    
    def compute_discordant_aspects(
        self,
        quarter: str,
        orb_limits: Optional[Dict[str, float]] = None
    ) -> List[AspectEventIn]:
        """Compute all discordant aspects for a quarter.
        
        Args:
            quarter: Quarter string like "2025-Q3"
            orb_limits: Optional orb limit overrides
            
        Returns:
            List of AspectEventIn objects
        """
        logger.info(f"Computing aspects for quarter {quarter}")
        start_time = time.time()
        
        if not self._initialized:
            self.initialize()
            
        # Parse quarter and set up orb limits
        q_start, q_end = parse_quarter(quarter)
        ol = OrbLimits(**(orb_limits or {}))
        
        events: List[AspectEventIn] = []
        
        # Generate all planet pairs
        planet_pairs = list(combinations(PLANET_ORDER, 2))
        logger.info(f"Processing {len(planet_pairs)} planet pairs × 3 aspects = {len(planet_pairs) * 3} combinations")
        
        for raw_p1, raw_p2 in planet_pairs:
            # Canonicalize pair order
            p1, p2 = self.canonical_pair(raw_p1, raw_p2)
            has_moon = 'MOON' in (p1, p2)
            
            # Adjust step size based on Moon presence (Moon moves fast)
            dt_hours = 1.0 if has_moon else 6.0
            
            logger.debug(f"Processing pair {p1}-{p2} (step: {dt_hours}h)")
            
            # Check each aspect type
            for aspect_name, target_angle in ASPECTS.items():
                orb_limit = getattr(ol, aspect_name)
                
                # Find intervals where aspect is within orb
                intervals = self.scan_intervals(
                    q_start, q_end, (p1, p2), target_angle, orb_limit, dt_hours
                )
                
                for raw_enter, raw_exit in intervals:
                    # Refine boundaries
                    enter_time = self.refine_boundary(
                        raw_enter - timedelta(hours=dt_hours),
                        raw_enter + timedelta(hours=dt_hours),
                        (p1, p2), target_angle, orb_limit, entering=True
                    )
                    
                    exit_time = self.refine_boundary(
                        raw_exit - timedelta(hours=dt_hours),
                        raw_exit + timedelta(hours=dt_hours),
                        (p1, p2), target_angle, orb_limit, entering=False
                    )
                    
                    # Find exact peak
                    peak_time, min_orb = self.find_peak(
                        enter_time, exit_time, (p1, p2), target_angle
                    )
                    
                    # Determine severity
                    severity = 'major' if min_orb <= 1.0 else 'minor'
                    
                    # Check for eclipse (Sun-Moon only)
                    is_eclipse = False
                    notes = None
                    if {p1, p2} == {'SUN', 'MOON'} and aspect_name in ('conjunction', 'opposition'):
                        t_peak = self.ts.from_datetime(peak_time)
                        moon_lat = abs(self.moon_ecliptic_lat_deg(t_peak))
                        is_eclipse = moon_lat <= ol.eclipse_lat_thresh
                        if is_eclipse:
                            notes = "near node"
                    
                    # Create aspect event
                    event = AspectEventIn(
                        quarter=quarter,
                        start_utc=enter_time,
                        peak_utc=peak_time,
                        end_utc=exit_time,
                        planet1=p1,
                        planet2=p2,
                        aspect=aspect_name,
                        orb_deg=round(min_orb, 3),
                        severity=severity,
                        is_eclipse=is_eclipse,
                        notes=notes
                    )
                    
                    events.append(event)
                    
                    logger.debug(
                        f"Found {aspect_name}: {p1}-{p2} at {peak_time.strftime('%m-%d %H:%M')} "
                        f"(orb: {min_orb:.3f}°, {severity})"
                    )
        
        # Filter to ensure peak is within quarter (safety check)
        quarter_events = [e for e in events if q_start <= e.peak_utc < q_end]
        
        elapsed = time.time() - start_time
        logger.info(
            f"Computed {len(quarter_events)} aspects for {quarter} "
            f"in {elapsed:.2f}s ({len(events) - len(quarter_events)} filtered out)"
        )
        
        return quarter_events


# Global engine instance (initialized lazily)
_engine: Optional[AstrologyEngine] = None


def get_engine() -> AstrologyEngine:
    """Get global astrology engine instance."""
    global _engine
    if _engine is None:
        _engine = AstrologyEngine()
    return _engine


# Convenience function
def compute_discordant_aspects(
    quarter: str,
    orb_limits: Optional[Dict[str, float]] = None
) -> List[AspectEventIn]:
    """Compute discordant aspects for a quarter using global engine."""
    engine = get_engine()
    return engine.compute_discordant_aspects(quarter, orb_limits)