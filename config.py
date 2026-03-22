"""
config.py — All settings and shared singletons.

Load once at import time. All other modules import from here.
The anthropic client is created once so every module shares it.
"""

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    # Anthropic
    anthropic_api_key: str = field(
        default_factory=lambda: os.getenv("ANTHROPIC_API_KEY", "")
    )

    # Scraper
    area: str = field(default_factory=lambda: os.getenv("AREA", "Solihull"))
    trade_type: str = field(
        default_factory=lambda: os.getenv("TRADE_TYPE", "plumbers")
    )
    max_businesses: int = field(
        default_factory=lambda: int(os.getenv("MAX_BUSINESSES", "20"))
    )
    politeness_delay: float = field(
        default_factory=lambda: float(os.getenv("POLITENESS_DELAY_SECONDS", "1.5"))
    )
    job_timeout: int = field(
        default_factory=lambda: int(os.getenv("JOB_TIMEOUT_SECONDS", "600"))
    )

    # Feature flags
    llm_fallback_enabled: bool = field(
        default_factory=lambda: os.getenv("LLM_FALLBACK_ENABLED", "false").lower()
        == "true"
    )

    # Data sources — toggle individual scrapers on/off
    sources_enabled: dict = field(
        default_factory=lambda: {
            "checkatrade": True,
            "yell": os.getenv("YELL_ENABLED", "false").lower() == "true",
        }
    )

    # Database
    database_path: str = field(
        default_factory=lambda: os.getenv("DATABASE_PATH", "jobs.db")
    )

    # LLM model
    model: str = "claude-3-5-haiku-20241022"

    def has_api_key(self) -> bool:
        return bool(self.anthropic_api_key)


settings = Settings()

# Single shared Anthropic client — only create if key is present
# Other modules should check settings.has_api_key() before using this
_anthropic_client = None


def get_anthropic_client():
    """Return shared Anthropic client. Raises RuntimeError if key not configured."""
    global _anthropic_client
    if _anthropic_client is None:
        if not settings.has_api_key():
            raise RuntimeError(
                "ANTHROPIC_API_KEY not set. Set it in .env or disable "
                "LLM features with LLM_FALLBACK_ENABLED=false."
            )
        import anthropic

        _anthropic_client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    return _anthropic_client
