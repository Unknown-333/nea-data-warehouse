"""
Utility functions for data cleaning and normalization.

Handles the messy reality of PDF-extracted data: merged cells,
inconsistent formatting, Nepali number strings, etc.
"""

import re
from typing import Optional, Union


def clean_numeric(value: Union[str, None]) -> Optional[float]:
    """
    Convert a raw PDF-extracted string to a clean float.
    
    Handles:
      - Comma-separated thousands: "32,269" → 32269.0
      - Whitespace/newlines: " 1,234 \\n" → 1234.0
      - Parenthesized negatives: "(500)" → -500.0
      - Dash/empty = None
      - Already numeric values
    
    Args:
        value: Raw string from PDF extraction
        
    Returns:
        Cleaned float or None if not parseable
    """
    if value is None:
        return None
    
    # Convert to string and strip whitespace
    s = str(value).strip().replace('\n', '').replace('\r', '')
    
    # Empty, dash, or N/A → None
    if s in ('', '-', '--', 'N/A', 'n/a', 'NA', 'None', 'null'):
        return None
    
    # Handle parenthesized negatives: (500) → -500
    is_negative = False
    if s.startswith('(') and s.endswith(')'):
        s = s[1:-1]
        is_negative = True
    
    # Handle explicit negative sign
    if s.startswith('-'):
        s = s[1:]
        is_negative = True
    
    # Remove commas and spaces within numbers
    s = s.replace(',', '').replace(' ', '')
    
    try:
        result = float(s)
        return -result if is_negative else result
    except ValueError:
        return None


def clean_text(value: Union[str, None]) -> Optional[str]:
    """
    Normalize text extracted from PDF cells.
    
    - Strips whitespace, collapses internal whitespace
    - Removes newlines within cell content
    - Returns None for empty strings
    """
    if value is None:
        return None
    
    s = str(value).strip()
    # Collapse internal whitespace and newlines
    s = re.sub(r'\s+', ' ', s)
    
    return s if s else None


def parse_time(value: Union[str, None]) -> Optional[str]:
    """
    Normalize time strings from PDFs.
    
    Examples:
      - "18:00" → "18:00"
      - "6:00 PM" → "18:00"
      - "18.00" → "18:00"
    """
    if value is None:
        return None
    
    s = str(value).strip().replace('.', ':')
    
    # Handle 12-hour format
    match = re.match(r'(\d{1,2}):(\d{2})\s*(AM|PM|am|pm)?', s)
    if match:
        hour = int(match.group(1))
        minute = match.group(2)
        period = match.group(3)
        
        if period and period.upper() == 'PM' and hour != 12:
            hour += 12
        elif period and period.upper() == 'AM' and hour == 12:
            hour = 0
        
        return f"{hour:02d}:{minute}"
    
    return s


def safe_get(lst: list, index: int, default=None):
    """Safely get an item from a list by index."""
    try:
        return lst[index] if index < len(lst) else default
    except (IndexError, TypeError):
        return default


def extract_date_from_filename(filename: str) -> Optional[str]:
    """
    Extract BS date from NDOR filename.
    
    Patterns:
      - "NDOR 2081_09_23.pdf" → "2081/09/23"
      - "NDOR_2081_09_23.pdf" → "2081/09/23"
      - "NDOR 2081_9_23.pdf"  → "2081/09/23" 
    """
    match = re.search(r'NDOR\s*_?\s*(\d{4})_(\d{1,2})_(\d{1,2})', filename)
    if match:
        year = match.group(1)
        month = match.group(2).zfill(2)
        day = match.group(3).zfill(2)
        return f"{year}/{month}/{day}"
    return None
