"""Formatting utilities for the Telegram bot."""

from datetime import datetime
from typing import Optional


def current_quarter(dt: Optional[datetime] = None) -> str:
    """Get current quarter string."""
    dt = dt or datetime.utcnow()
    q = (dt.month - 1) // 3 + 1
    return f"{dt.year}-Q{q}"


def next_quarter(dt: Optional[datetime] = None) -> str:
    """Get next quarter string."""
    dt = dt or datetime.utcnow()
    q = (dt.month - 1) // 3 + 1
    y = dt.year
    q += 1
    if q > 4:
        q, y = 1, y + 1
    return f"{y}-Q{q}"


def fmt_pct(x: float) -> str:
    """Format decimal as percentage."""
    return f"{100 * x:.1f}%"


def fmt_datetime(dt_str: str) -> str:
    """Format datetime string for display."""
    try:
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return dt.strftime("%m/%d %H:%M")
    except:
        return dt_str[:10]  # fallback to date only


def truncate_title(title: str, max_length: int = 60) -> str:
    """Truncate title if too long."""
    if len(title) <= max_length:
        return title
    return title[:max_length-3] + "..."