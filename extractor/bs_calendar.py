"""
Bikram Sambat (BS) ↔ Gregorian (AD) Calendar Converter.

Nepal uses the Bikram Sambat calendar officially. NDOR PDFs use BS dates
in filenames and report headers. This module handles all date conversions
needed for the pipeline.

Uses the `nepali-datetime` library for accurate conversion, accounting
for the irregular month lengths (29-32 days) in BS calendar.
"""

import logging
import datetime

logger = logging.getLogger(__name__)
from typing import Generator, Optional, Tuple

import nepali_datetime


def bs_to_ad(bs_date_str: str) -> Optional[datetime.date]:
    """
    Convert a Bikram Sambat date string to Gregorian date.
    
    Args:
        bs_date_str: Date in format "YYYY/MM/DD" or "YYYY-MM-DD"
                     e.g., "2081/09/23" or "2081-09-23"
    
    Returns:
        datetime.date in Gregorian calendar, or None if invalid
    
    Examples:
        >>> bs_to_ad("2081/09/23")
        datetime.date(2025, 1, 7)
    """
    try:
        # Normalize separator
        clean = bs_date_str.strip().replace('-', '/')
        parts = clean.split('/')
        
        if len(parts) != 3:
            return None
        
        year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
        
        bs_date = nepali_datetime.date(year, month, day)
        ad_date = bs_date.to_datetime_date()
        
        return ad_date
    except (ValueError, OverflowError) as e:
        logger.warning("BS→AD conversion failed for '%s': %s", bs_date_str, e)
        return None


def ad_to_bs(ad_date: datetime.date) -> Optional[str]:
    """
    Convert a Gregorian date to Bikram Sambat string.
    
    Args:
        ad_date: datetime.date in Gregorian calendar
    
    Returns:
        String in "YYYY/MM/DD" format (BS), or None if out of range
    """
    try:
        bs_date = nepali_datetime.date.from_datetime_date(ad_date)
        return f"{bs_date.year}/{bs_date.month:02d}/{bs_date.day:02d}"
    except (ValueError, OverflowError) as e:
        logger.warning("AD→BS conversion failed for '%s': %s", ad_date, e)
        return None


def generate_bs_date_range(
    start_bs: str,
    end_bs: str
) -> Generator[str, None, None]:
    """
    Generate a sequence of BS dates between start and end (inclusive).
    
    Handles the irregular month lengths of BS calendar (29-32 days).
    
    Args:
        start_bs: Start date in "YYYY/MM/DD" format
        end_bs:   End date in "YYYY/MM/DD" format
    
    Yields:
        BS date strings in "YYYY/MM/DD" format
    
    Example:
        >>> list(generate_bs_date_range("2081/09/23", "2081/09/25"))
        ['2081/09/23', '2081/09/24', '2081/09/25']
    """
    start_ad = bs_to_ad(start_bs)
    end_ad = bs_to_ad(end_bs)
    
    if start_ad is None or end_ad is None:
        return
    
    current = start_ad
    while current <= end_ad:
        bs_str = ad_to_bs(current)
        if bs_str:
            yield bs_str
        current += datetime.timedelta(days=1)


def bs_date_to_filename_format(bs_date_str: str) -> str:
    """
    Convert BS date to the NDOR filename format used by NEA.
    
    Args:
        bs_date_str: "YYYY/MM/DD" or "YYYY-MM-DD"
    
    Returns:
        String like "2081_09_23" (used in filenames and URLs)
    """
    clean = bs_date_str.strip().replace('-', '/').replace('/', '_')
    return clean


def get_bs_season(month: int) -> str:
    """
    Classify a BS month into Nepal's hydrological season.
    
    - Monsoon/Wet: Shrawan(4) to Ashwin(6) → High hydro generation
    - Winter/Dry:  Mangsir(8) to Falgun(11) → Low generation, high import
    - Pre-monsoon: Chaitra(12) to Ashadh(3) → Reservoir depletion
    
    Args:
        month: BS month number (1=Baisakh through 12=Chaitra)
    
    Returns:
        Season name string
    """
    if month in (4, 5, 6):
        return 'monsoon'
    elif month in (7, 8, 9, 10, 11):
        return 'dry'
    else:  # 1, 2, 3, 12
        return 'pre_monsoon'


def get_nepal_fiscal_year(bs_year: int, bs_month: int) -> str:
    """
    Get Nepal fiscal year from BS date.
    
    Nepal's fiscal year starts on 1st Shrawan (month 4, ~mid-July).
    FY 2081/82 = Shrawan 2081 to Ashadh 2082.
    
    Args:
        bs_year: BS year
        bs_month: BS month (1-12)
    
    Returns:
        Fiscal year string like "2081/82"
    """
    if bs_month >= 4:  # Shrawan onwards = new FY
        return f"{bs_year}/{(bs_year + 1) % 100:02d}"
    else:
        return f"{bs_year - 1}/{bs_year % 100:02d}"


def parse_bs_date_from_text(text: str) -> Optional[str]:
    """
    Extract BS date from PDF text content.
    
    Looks for patterns like:
      - "For Date: 2081/09/23"
      - "Date: 2081/09/23 ( 2025/01/7 )"
      - "2081/9/23"
    
    Returns:
        BS date string in "YYYY/MM/DD" format, or None
    """
    import re
    
    # Pattern: 4-digit year / 1-2 digit month / 1-2 digit day
    patterns = [
        r'(?:Date|date)\s*:\s*(\d{4})\s*/\s*(\d{1,2})\s*/\s*(\d{1,2})',
        r'(\d{4})\s*/\s*(\d{1,2})\s*/\s*(\d{1,2})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            year = match.group(1)
            month = match.group(2).zfill(2)
            day = match.group(3).zfill(2)
            return f"{year}/{month}/{day}"
    
    return None
