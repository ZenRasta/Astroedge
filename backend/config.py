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

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def orb_limits(self) -> Dict[str, float]:
        """Parse orb limits from JSON string."""
        return json.loads(self.orb_limits_json)


settings = Settings()
