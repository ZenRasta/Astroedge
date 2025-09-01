"""Pydantic models for AstroEdge API."""

from datetime import datetime
from typing import Literal, Optional, Dict, Any, List
from pydantic import BaseModel, Field, validator


# Planet and aspect enums matching database types
Planet = Literal[
    'SUN', 'MOON', 'MERCURY', 'VENUS', 'MARS', 
    'JUPITER', 'SATURN', 'URANUS', 'NEPTUNE', 'PLUTO'
]

Aspect = Literal['conjunction', 'square', 'opposition']
Severity = Literal['major', 'minor']


class AspectEventIn(BaseModel):
    """Input model for aspect events (before database insert)."""
    quarter: str = Field(..., description="Quarter string like '2025-Q3'")
    start_utc: datetime = Field(..., description="Aspect begins (entering orb)")
    peak_utc: datetime = Field(..., description="Exact aspect time (minimum orb)")
    end_utc: datetime = Field(..., description="Aspect ends (exiting orb)")
    planet1: Planet = Field(..., description="First planet (canonical order)")
    planet2: Planet = Field(..., description="Second planet (canonical order)")
    aspect: Aspect = Field(..., description="Aspect type")
    orb_deg: float = Field(..., ge=0, le=180, description="Orb in degrees at peak")
    severity: Severity = Field(..., description="Major (≤1°) or minor (>1°)")
    is_eclipse: bool = Field(default=False, description="True if Sun-Moon near node")
    notes: Optional[str] = Field(None, description="Optional notes (e.g., 'near node')")
    source: str = Field(default="skyfield-de440s", description="Ephemeris source")
    confidence: float = Field(default=0.90, ge=0, le=1, description="Confidence score")
    
    @validator('quarter')
    def validate_quarter(cls, v):
        """Validate quarter format."""
        import re
        if not re.match(r'^\d{4}-Q[1-4]$', v):
            raise ValueError("Quarter must be in format 'YYYY-Q[1-4]'")
        return v
    
    @validator('orb_deg')
    def validate_orb(cls, v):
        """Round orb to 3 decimal places."""
        return round(v, 3)
    
    @validator('peak_utc', 'start_utc', 'end_utc')
    def validate_timezone(cls, v):
        """Ensure all datetimes are timezone-aware UTC."""
        if v.tzinfo is None:
            raise ValueError("Datetime must be timezone-aware")
        return v.astimezone(datetime.now().astimezone().tzinfo.utc) if v.tzinfo != datetime.now().astimezone().tzinfo.utc else v
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class AspectEventOut(BaseModel):
    """Output model for aspect events (from database)."""
    id: str = Field(..., description="UUID primary key")
    quarter: str
    start_utc: datetime
    peak_utc: datetime 
    end_utc: datetime
    planet1: Planet
    planet2: Planet
    aspect: Aspect
    orb_deg: float
    severity: Severity
    is_eclipse: bool
    notes: Optional[str] = None
    source: str
    confidence: float
    created_at: datetime
    updated_at: datetime
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class OrbLimits(BaseModel):
    """Configuration for aspect orb limits."""
    square: float = Field(default=8.0, ge=0, le=45, description="Square orb limit in degrees")
    opposition: float = Field(default=8.0, ge=0, le=45, description="Opposition orb limit in degrees") 
    conjunction: float = Field(default=6.0, ge=0, le=45, description="Conjunction orb limit in degrees")
    eclipse_lat_thresh: float = Field(default=1.5, ge=0, le=10, description="Eclipse latitude threshold in degrees")


class GeneratePayload(BaseModel):
    """Payload for generating aspect events."""
    quarter: str = Field(..., description="Quarter to generate aspects for")
    orb_limits: Optional[Dict[str, float]] = Field(None, description="Optional orb limits override")
    force_regenerate: bool = Field(default=False, description="Force regeneration even if data exists")
    
    @validator('quarter')
    def validate_quarter(cls, v):
        """Validate quarter format."""
        import re
        if not re.match(r'^\d{4}-Q[1-4]$', v):
            raise ValueError("Quarter must be in format 'YYYY-Q[1-4]'")
        return v


class GenerateResponse(BaseModel):
    """Response from aspect generation."""
    quarter: str
    inserted_or_updated: int = Field(..., description="Number of rows inserted or updated")
    total_aspects: int = Field(..., description="Total aspects found")
    execution_time_seconds: float = Field(..., description="Time taken to generate aspects")
    summary: Dict[str, Any] = Field(default_factory=dict, description="Summary statistics")


class AspectListResponse(BaseModel):
    """Response for listing aspect events."""
    quarter: str
    aspects: List[AspectEventOut]
    total_count: int
    summary: Dict[str, Any] = Field(default_factory=dict)


class AspectSummary(BaseModel):
    """Summary statistics for aspect events."""
    total_aspects: int
    by_severity: Dict[str, int] = Field(default_factory=dict)  # major/minor counts
    by_aspect_type: Dict[str, int] = Field(default_factory=dict)  # conjunction/square/opposition counts
    by_planet_pairs: Dict[str, int] = Field(default_factory=dict)  # planet pair counts
    eclipse_count: int = Field(default=0)
    average_orb: float = Field(default=0.0)
    date_range: Dict[str, datetime] = Field(default_factory=dict)  # earliest/latest peaks


class HealthCheckResponse(BaseModel):
    """Health check response."""
    status: str
    version: str
    supabase: str
    ephemeris_loaded: bool = Field(default=False)
    ephemeris_file: Optional[str] = None


class ErrorResponse(BaseModel):
    """Standard error response."""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    code: Optional[str] = Field(None, description="Error code")


class QuarterInfo(BaseModel):
    """Information about a quarter."""
    quarter: str
    start_date: datetime
    end_date: datetime
    current: bool = Field(default=False)
    days_total: int
    days_remaining: Optional[int] = None


# API Request/Response models for market integration (future)
class MarketAnalysisRequest(BaseModel):
    """Request for market analysis with aspects."""
    quarter: str
    market_deadline: datetime
    market_categories: List[str] = Field(default_factory=list)
    include_aspects: bool = Field(default=True)


class MarketAspectInfluence(BaseModel):
    """Aspect influence on a market."""
    aspect_id: str
    temporal_weight: float
    angular_weight: float  
    severity_weight: float
    category_weight: float
    total_contribution: float
    explanation: str


# Impact Map schemas
Category = Literal[
    'geopolitics', 'conflict', 'accidents_infrastructure', 'legal_regulatory', 'markets_finance',
    'communications_tech', 'public_sentiment', 'sports', 'entertainment', 'science_health', 'weather'
]


class ImpactMapKeyedIn(BaseModel):
    """Input model with keyed format for impact maps."""
    activate: bool = True
    notes: Optional[str] = None
    map_by_key: Dict[str, Dict[Category, int]]


class ImpactMapNestedItem(BaseModel):
    """Individual item in nested format."""
    planets: List[Planet] = Field(min_items=2, max_items=2)
    aspect: Aspect
    weights: Dict[Category, int]


class ImpactMapNestedIn(BaseModel):
    """Input model with nested format for impact maps."""
    activate: bool = True
    notes: Optional[str] = None
    map_nested: List[ImpactMapNestedItem]


class ImpactMapPost(BaseModel):
    """Main input model for POST /impact-map endpoint."""
    activate: bool = True
    notes: Optional[str] = None
    map_by_key: Optional[Dict[str, Dict[Category, int]]] = None
    map_nested: Optional[List[ImpactMapNestedItem]] = None

    @validator('map_nested')
    def at_least_one(cls, v, values):
        """Ensure at least one map format is provided."""
        map_by_key = values.get('map_by_key')
        map_nested = v
        
        if not map_by_key and not map_nested:
            raise ValueError('Provide map_by_key or map_nested')
        return v


class ImpactMapActiveOut(BaseModel):
    """Response model for GET /impact-map/active endpoint."""
    version_id: Optional[str]
    created_at: Optional[str]
    notes: Optional[str] = None
    map: Dict[str, Dict[Category, int]]


# Polymarket schemas
RulesClarity = Literal['clear', 'ambiguous', 'unclear']


class MarketRaw(BaseModel):
    """Raw market data from Gamma API."""
    id: str
    title: str
    description: Optional[str] = None
    rules: Optional[str] = None
    deadline_utc: datetime
    tokens: List[dict] = Field(default_factory=list)  # raw Gamma tokens[] for YES/NO lookup


class OrderbookL1(BaseModel):
    """Level 1 order book data from CLOB API."""
    bid_yes: Optional[float] = None  # best bid price
    ask_yes: Optional[float] = None  # best ask price
    bid_sz_usdc: float = 0.0         # top bid size ($)
    ask_sz_usdc: float = 0.0         # top ask size ($)


class MarketNormalized(BaseModel):
    """Normalized market with computed pricing and liquidity metrics."""
    id: str
    title: str
    description: Optional[str]
    rules: Optional[str]
    deadline_utc: datetime
    price_yes: float                 # mid price
    spread: float
    top_depth_usdc: float
    liquidity_score: float
    rules_clarity: Optional[RulesClarity] = None
    category_tags: List[Category] = Field(default_factory=list)


class TaggerIn(BaseModel):
    """Input for LLM market tagging."""
    id: str
    title: str
    description: Optional[str] = None
    rules: Optional[str] = None


class TaggerOut(BaseModel):
    """Output from LLM market tagging."""
    market_id: str
    rules_clarity: RulesClarity
    category_tags: List[Category] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    explanation: Optional[str] = None


class OpportunityOut(BaseModel):
    """Output model for opportunities from scan results."""
    id: str
    market_id: str
    quarter: str
    p0: float = Field(..., description="Base probability")
    s_astro: float = Field(..., description="Astro score")
    p_astro: float = Field(..., description="Astro-adjusted probability")
    edge_net: float = Field(..., description="Net edge after fees")
    size_fraction: float = Field(..., description="Position size fraction")
    decision: str = Field(..., description="Trading decision (BUY/SELL/HOLD)")
    created_at: datetime
    
    # Market details (joined)
    title: Optional[str] = Field(None, description="Market title")
    deadline_utc: Optional[datetime] = Field(None, description="Market deadline")
    market_rules_clarity: Optional[RulesClarity] = Field(None, description="Rules clarity")
    market_liquidity_score: Optional[float] = Field(None, description="Liquidity score")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }