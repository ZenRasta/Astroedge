"""Utility definitions for planets, aspects, and categories."""

from typing import List, Tuple

VALID_PLANETS: List[str] = [
    'SUN', 'MOON', 'MERCURY', 'VENUS', 'MARS', 
    'JUPITER', 'SATURN', 'URANUS', 'NEPTUNE', 'PLUTO'
]

VALID_ASPECTS: List[str] = ['conjunction', 'square', 'opposition']

VALID_CATEGORIES: List[str] = [
    'geopolitics', 'conflict', 'accidents_infrastructure', 'legal_regulatory', 'markets_finance',
    'communications_tech', 'public_sentiment', 'sports', 'entertainment', 'science_health', 'weather'
]


def canonical_pair(p1: str, p2: str) -> Tuple[str, str]:
    """Return planet pair in canonical order based on VALID_PLANETS ordering."""
    order = {name: i for i, name in enumerate(VALID_PLANETS)}
    return (p1, p2) if order[p1] < order[p2] else (p2, p1)