"""Service for parsing, validating, and managing impact maps."""

from typing import Dict, List, Tuple
from dataclasses import dataclass

try:
    from ..schemas import Planet, Aspect, Category, ImpactMapPost
    from .supabase_repo_impact import (
        RuleRow,
        insert_impact_map_version,
        set_only_version_active,
        insert_rules_bulk,
        get_active_map_version_with_json,
        fetch_rules_for_version,
    )
    from .util_planets import canonical_pair, VALID_PLANETS, VALID_ASPECTS, VALID_CATEGORIES
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from schemas import Planet, Aspect, Category, ImpactMapPost
    from services.supabase_repo_impact import (
        RuleRow,
        insert_impact_map_version,
        set_only_version_active,
        insert_rules_bulk,
        get_active_map_version_with_json,
        fetch_rules_for_version,
    )
    from services.util_planets import canonical_pair, VALID_PLANETS, VALID_ASPECTS, VALID_CATEGORIES


def parse_key(key: str) -> Tuple[str, str, str]:
    """Parse impact map key in format '(PLANET1,PLANET2)|aspect'."""
    try:
        pair, aspect = key.split("|")
        if not (pair.startswith("(") and pair.endswith(")")):
            raise ValueError("Key must have format '(PLANET1,PLANET2)|aspect'")
        
        p1, p2 = pair[1:-1].split(",")
        p1, p2 = p1.strip(), p2.strip()
        
        if p1 not in VALID_PLANETS or p2 not in VALID_PLANETS:
            raise ValueError("Invalid planet in key")
        if aspect not in VALID_ASPECTS:
            raise ValueError("Invalid aspect in key")
        
        # Return in canonical order
        a, b = canonical_pair(p1, p2)
        return a, b, aspect
    except Exception as e:
        raise ValueError(f"Bad key format '{key}': {e}")


def validate_weight_map(weights: Dict[Category, int]) -> Dict[Category, int]:
    """Validate weight map and return only non-zero weights."""
    clean: Dict[Category, int] = {}
    
    for cat, w in weights.items():
        if cat not in VALID_CATEGORIES:
            raise ValueError(f"Unknown category '{cat}'")
        if not isinstance(w, int):
            raise ValueError(f"Weight for {cat} must be integer")
        if w < -3 or w > 3:
            raise ValueError(f"Weight for {cat} out of bounds [-3,3]: {w}")
        
        # Skip zeros - we only persist non-zero weights
        if w != 0:
            clean[cat] = w
    
    return clean


def to_keyed_map(payload: ImpactMapPost) -> Dict[str, Dict[Category, int]]:
    """Convert payload to keyed map format for storage."""
    if payload.map_by_key:
        # Validate each key and weights
        keyed: Dict[str, Dict[Category, int]] = {}
        for k, wmap in payload.map_by_key.items():
            # Validate key format
            _ = parse_key(k)
            # Validate and clean weights
            keyed[k] = validate_weight_map(wmap)
        return keyed
    
    # Convert nested format to keyed format
    keyed: Dict[str, Dict[Category, int]] = {}
    for item in (payload.map_nested or []):
        p1, p2 = canonical_pair(item.planets[0], item.planets[1])
        key = f"({p1},{p2})|{item.aspect}"
        wmap = validate_weight_map(item.weights)
        keyed[key] = wmap
    
    return keyed


def explode_rules(version_id: str, keyed_map: Dict[str, Dict[Category, int]]) -> List[RuleRow]:
    """Explode keyed map into individual rule rows."""
    rows: List[RuleRow] = []
    
    for k, weights in keyed_map.items():
        p1, p2, aspect = parse_key(k)
        for cat, w in weights.items():
            rows.append(RuleRow(p1, p2, aspect, cat, w))
    
    return rows


def create_new_impact_map(payload: ImpactMapPost) -> str:
    """
    Create a new impact map version:
    1. Validate and convert to keyed format
    2. Insert version with JSON blob
    3. Explode and insert rules
    4. Optionally activate version
    
    Returns: version_id
    """
    # Validate and convert to keyed format
    keyed_map = to_keyed_map(payload)
    
    # Count non-zero entries for sanity check
    nonzero_entries = sum(len(weights) for weights in keyed_map.values())
    if nonzero_entries == 0:
        raise ValueError("Map contains no non-zero weights")
    
    # Insert new version
    version_id = insert_impact_map_version(
        json_blob=keyed_map, 
        notes=payload.notes, 
        is_active=payload.activate
    )
    
    # Explode rules and bulk insert
    rows = explode_rules(version_id, keyed_map)
    insert_rules_bulk(version_id, rows)
    
    # If activate=True, ensure only this version is active
    if payload.activate:
        set_only_version_active(version_id)
    
    return version_id


def get_active_map() -> dict:
    """Get the active impact map version with its JSON blob."""
    return get_active_map_version_with_json()


def weight_for(planets: Tuple[str, str], aspect: str, tags: List[str]) -> float:
    """
    Sum weights for the active version across provided category tags.
    Returns 0.0 if no active version or no matching rules.
    """
    active = get_active_map_version_with_json()
    version_id = active.get("version_id")
    
    if not version_id:
        return 0.0
    
    # Canonicalize planet pair
    p1, p2 = canonical_pair(planets[0], planets[1])
    
    # Fetch matching rules
    rows = fetch_rules_for_version(version_id, p1, p2, aspect, tags)
    
    # Sum weights
    return float(sum(r["weight"] for r in rows))