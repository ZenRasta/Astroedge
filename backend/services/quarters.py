"""Quarter parsing utilities for AstroEdge."""

import re
from datetime import datetime, timezone
from typing import Tuple


def parse_quarter(quarter: str) -> Tuple[datetime, datetime]:
    """Parse quarter string like 'YYYY-Q1' into UTC datetime range.
    
    Args:
        quarter: Quarter string in format "YYYY-Q1", "YYYY-Q2", "YYYY-Q3", or "YYYY-Q4"
        
    Returns:
        Tuple of (start_utc, end_utc_exclusive) where:
        - start_utc: Beginning of quarter (inclusive)
        - end_utc_exclusive: End of quarter (exclusive)
        
    Quarter mappings:
        Q1: January 1 - March 31 (Jan-Mar)
        Q2: April 1 - June 30 (Apr-Jun)  
        Q3: July 1 - September 30 (Jul-Sep)
        Q4: October 1 - December 31 (Oct-Dec)
        
    Examples:
        parse_quarter("2025-Q3") -> (2025-07-01 00:00:00+00:00, 2025-10-01 00:00:00+00:00)
        parse_quarter("2024-Q1") -> (2024-01-01 00:00:00+00:00, 2024-04-01 00:00:00+00:00)
    """
    # Validate and parse quarter string
    pattern = r'^(\d{4})-Q([1-4])$'
    match = re.match(pattern, quarter)
    
    if not match:
        raise ValueError(f"Invalid quarter format: {quarter}. Expected format: YYYY-Q[1-4]")
    
    year = int(match.group(1))
    q_num = int(match.group(2))
    
    # Map quarter number to start month
    quarter_start_months = {
        1: 1,   # January
        2: 4,   # April  
        3: 7,   # July
        4: 10,  # October
    }
    
    start_month = quarter_start_months[q_num]
    
    # Calculate start and end dates
    start_utc = datetime(year, start_month, 1, tzinfo=timezone.utc)
    
    # End date is start of next quarter (exclusive)
    if q_num == 4:
        # Q4 wraps to next year's Q1
        end_utc = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        # Next quarter in same year
        end_month = quarter_start_months[q_num + 1]
        end_utc = datetime(year, end_month, 1, tzinfo=timezone.utc)
    
    return start_utc, end_utc


def format_quarter(dt: datetime) -> str:
    """Convert datetime to quarter string.
    
    Args:
        dt: Datetime to convert (any timezone, will be converted to UTC)
        
    Returns:
        Quarter string in format "YYYY-Q[1-4]"
        
    Examples:
        format_quarter(datetime(2025, 8, 15)) -> "2025-Q3"
        format_quarter(datetime(2024, 2, 1)) -> "2024-Q1"
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    
    month = dt.month
    year = dt.year
    
    if 1 <= month <= 3:
        quarter_num = 1
    elif 4 <= month <= 6:
        quarter_num = 2
    elif 7 <= month <= 9:
        quarter_num = 3
    else:  # 10-12
        quarter_num = 4
        
    return f"{year}-Q{quarter_num}"


def get_current_quarter() -> str:
    """Get the current quarter string based on UTC time.
    
    Returns:
        Current quarter string in format "YYYY-Q[1-4]"
    """
    return format_quarter(datetime.now(timezone.utc))


def get_next_quarter(quarter: str) -> str:
    """Get the next quarter string.
    
    Args:
        quarter: Quarter string in format "YYYY-Q[1-4]"
        
    Returns:
        Next quarter string
        
    Examples:
        get_next_quarter("2025-Q3") -> "2025-Q4"
        get_next_quarter("2025-Q4") -> "2026-Q1"
    """
    start_dt, _ = parse_quarter(quarter)
    
    # Add 3 months to get to next quarter
    if start_dt.month == 10:  # Q4 -> next year Q1
        next_dt = datetime(start_dt.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        next_dt = datetime(start_dt.year, start_dt.month + 3, 1, tzinfo=timezone.utc)
    
    return format_quarter(next_dt)


def get_previous_quarter(quarter: str) -> str:
    """Get the previous quarter string.
    
    Args:
        quarter: Quarter string in format "YYYY-Q[1-4]"
        
    Returns:
        Previous quarter string
        
    Examples:
        get_previous_quarter("2025-Q3") -> "2025-Q2"
        get_previous_quarter("2025-Q1") -> "2024-Q4"
    """
    start_dt, _ = parse_quarter(quarter)
    
    # Subtract 3 months to get to previous quarter
    if start_dt.month == 1:  # Q1 -> previous year Q4
        prev_dt = datetime(start_dt.year - 1, 10, 1, tzinfo=timezone.utc)
    else:
        prev_dt = datetime(start_dt.year, start_dt.month - 3, 1, tzinfo=timezone.utc)
    
    return format_quarter(prev_dt)


def quarter_contains_date(quarter: str, dt: datetime) -> bool:
    """Check if a datetime falls within a quarter.
    
    Args:
        quarter: Quarter string in format "YYYY-Q[1-4]"
        dt: Datetime to check (any timezone)
        
    Returns:
        True if datetime falls within the quarter (inclusive start, exclusive end)
    """
    start_utc, end_utc = parse_quarter(quarter)
    
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    
    return start_utc <= dt < end_utc