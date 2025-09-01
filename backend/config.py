from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Dict
import json


class Settings(BaseSettings):
    # Supabase Configuration
    supabase_url: str = Field(..., env="SUPABASE_URL")
    supabase_service_role: str = Field(..., env="SUPABASE_SERVICE_ROLE")
    supabase_anon: str = Field(..., env="SUPABASE_ANON")

    # Trading Configuration
    fee_bps_default: int = Field(default=60, env="FEE_BPS_DEFAULT")
    spread_default: float = Field(default=0.01, env="SPREAD_DEFAULT")
    slippage_default: float = Field(default=0.005, env="SLIPPAGE_DEFAULT")

    # Astro Scoring Parameters
    lambda_gain: float = Field(default=0.10, env="LAMBDA_GAIN")
    edge_threshold: float = Field(default=0.04, env="EDGE_THRESHOLD")
    lambda_days: int = Field(default=5, env="LAMBDA_DAYS")
    orb_limits_json: str = Field(
        default='{"square": 8, "opposition": 8, "conjunction": 6}',
        env="ORB_LIMITS_JSON",
    )
    k_cap: float = Field(default=5.0, env="K_CAP")

    # Application Settings
    app_version: str = Field(default="0.1.0", env="APP_VERSION")
    debug: bool = Field(default=False, env="DEBUG")

    # Polymarket Integration
    poly_base_url: str = Field(default="https://gamma-api.polymarket.com", env="POLY_BASE_URL")
    clob_base_url: str = Field(default="https://clob.polymarket.com", env="CLOB_BASE_URL")
    poly_timeout_s: int = Field(default=10, env="POLY_TIMEOUT_S")
    redis_url: str = Field(default="redis://redis:6379/0", env="REDIS_URL")
    
    # LLM Configuration
    llm_model: str = Field(default="openai/gpt-4o-mini", env="LLM_MODEL")
    openrouter_api_key: str = Field(default="test-key-not-set", env="OPENROUTER_API_KEY")
    
    # Caching Configuration
    tag_cache_ttl_sec: int = Field(default=604800, env="TAG_CACHE_TTL_SEC")  # 7 days
    orderbook_cache_ttl_sec: int = Field(default=3, env="ORDERBOOK_CACHE_TTL_SEC")  # 3 seconds
    
    # Liquidity Configuration
    liquidity_spread_wide: float = Field(default=0.05, env="LIQUIDITY_SPREAD_WIDE")  # 5%
    liquidity_depth_max_usdc: float = Field(default=5000.0, env="LIQUIDITY_DEPTH_MAX_USDC")
    liquidity_min_score: float = Field(default=0.50, env="LIQUIDITY_MIN_SCORE")  # θ threshold
    
    # Concurrency Configuration
    scan_concurrency: int = Field(default=16, env="SCAN_CONCURRENCY")
    books_batch: int = Field(default=40, env="BOOKS_BATCH")
    
    # Telegram Bot Configuration (optional)
    telegram_bot_token: str = Field(default="", env="TELEGRAM_BOT_TOKEN")
    backend_base_url: str = Field(default="http://localhost:8003", env="BACKEND_BASE_URL")
    default_quarter: str = Field(default="2025-Q3", env="DEFAULT_QUARTER")
    scan_params_json: str = Field(default='{}', env="SCAN_PARAMS_JSON")
    
    # Paper / Live Trading
    execution_mode: str = Field(default="paper", env="EXECUTION_MODE")
    live_clob_enabled: bool = Field(default=False, env="LIVE_CLOB_ENABLED")
    closed_only_mode: bool = Field(default=True, env="CLOSED_ONLY_MODE")
    
    # Fees & Slippage
    taker_fee_bps: int = Field(default=60, env="TAKER_FEE_BPS")
    slippage_a: float = Field(default=0.0005, env="SLIPPAGE_A")
    slippage_b: float = Field(default=0.0025, env="SLIPPAGE_B")
    mark_method: str = Field(default="mid", env="MARK_METHOD")
    
    # Risk Management
    max_size_fraction: float = Field(default=0.05, env="MAX_SIZE_FRACTION")
    max_per_market_usdc: float = Field(default=1000.0, env="MAX_PER_MARKET_USDC")
    max_per_theme_usdc: float = Field(default=2500.0, env="MAX_PER_THEME_USDC")
    daily_max_drawdown_usdc: float = Field(default=250.0, env="DAILY_MAX_DRAWDOWN_USDC")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def orb_limits(self) -> Dict[str, float]:
        """Parse orb limits from JSON string."""
        return json.loads(self.orb_limits_json)


settings = Settings()
