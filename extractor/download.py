"""
NDOR PDF Downloader.

Downloads Nepal Daily Operational Report PDFs from the NEA website.
PDFs follow a deterministic URL pattern based on BS (Bikram Sambat) dates.

URL Pattern:
  https://www.nea.org.np/admin/assets/uploads/ldc/NDOR%20{YEAR}_{MONTH}_{DAY}.pdf

Usage:
    # Download a single day
    python -m extractor.download --date 2081/09/23
    
    # Download a date range
    python -m extractor.download --start 2081/09/01 --end 2081/09/30
    
    # Download last 30 days
    python -m extractor.download --days 30
"""

import os
import sys
import time
import logging
import argparse
import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extractor.bs_calendar import (
    bs_to_ad,
    ad_to_bs,
    generate_bs_date_range,
    bs_date_to_filename_format,
)


# ─── Configuration ────────────────────────────────────────────
BASE_URL = "https://www.nea.org.np/admin/assets/uploads/ldc"
OUTPUT_DIR = Path("data/raw_pdfs")
SAMPLE_DIR = Path("data/sample")

# Browser-like headers to avoid blocks
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nea.org.np/dailyOperationalReports",
}

# Rate limiting
DELAY_SECONDS = 1.5  # Be respectful to NEA's servers


def construct_url(bs_date: str) -> str:
    """
    Build the download URL for an NDOR PDF.
    
    Args:
        bs_date: BS date in "YYYY/MM/DD" format
    
    Returns:
        Full URL string
    
    Example:
        >>> construct_url("2081/09/23")
        'https://www.nea.org.np/admin/assets/uploads/ldc/NDOR%202081_09_23.pdf'
    """
    parts = bs_date.split('/')
    year, month, day = parts[0], parts[1].zfill(2), parts[2].zfill(2)
    filename = f"NDOR {year}_{month}_{day}.pdf"
    return f"{BASE_URL}/{requests.utils.quote(filename)}"


def construct_local_path(bs_date: str, output_dir: Path = OUTPUT_DIR) -> Path:
    """Build the local file path for a downloaded PDF."""
    parts = bs_date.split('/')
    year, month, day = parts[0], parts[1].zfill(2), parts[2].zfill(2)
    return output_dir / f"NDOR_{year}_{month}_{day}.pdf"


def download_pdf(
    bs_date: str,
    output_dir: Path = OUTPUT_DIR,
    force: bool = False,
    session: Optional[requests.Session] = None,
) -> Tuple[bool, str]:
    """
    Download a single NDOR PDF.
    
    Args:
        bs_date: BS date in "YYYY/MM/DD" format
        output_dir: Directory to save PDFs
        force: Re-download even if file exists
        session: Optional requests.Session for connection reuse
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    local_path = construct_local_path(bs_date, output_dir)
    
    # Skip if already downloaded (unless force)
    if local_path.exists() and not force:
        return True, f"Already exists: {local_path.name}"
    
    url = construct_url(bs_date)
    ad_date = bs_to_ad(bs_date)
    ad_str = ad_date.isoformat() if ad_date else "unknown"
    
    try:
        requester = session or requests
        response = requester.get(url, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            # Verify it's a PDF (check magic bytes)
            if response.content[:4] == b'%PDF':
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                size_kb = len(response.content) / 1024
                return True, f"Downloaded: {local_path.name} ({size_kb:.0f}KB) [AD: {ad_str}]"
            else:
                return False, f"Not a PDF: {bs_date} (AD: {ad_str}) — possibly a holiday"
        elif response.status_code == 404:
            return False, f"Not found (404): {bs_date} (AD: {ad_str}) — no report for this date"
        else:
            return False, f"HTTP {response.status_code}: {bs_date} (AD: {ad_str})"
    
    except requests.RequestException as e:
        return False, f"Error downloading {bs_date}: {e}"


def download_date_range(
    start_bs: str,
    end_bs: str,
    output_dir: Path = OUTPUT_DIR,
    force: bool = False,
) -> dict:
    """
    Download NDOR PDFs for a range of BS dates.
    
    Args:
        start_bs: Start BS date "YYYY/MM/DD"
        end_bs:   End BS date "YYYY/MM/DD"
        output_dir: Download directory
        force: Re-download existing files
    
    Returns:
        Summary dict with counts and details
    """
    results = {"success": 0, "skipped": 0, "failed": 0, "details": []}
    
    # Use session for connection pooling
    session = requests.Session()
    session.headers.update(HEADERS)
    
    dates = list(generate_bs_date_range(start_bs, end_bs))
    total = len(dates)
    
    logger.info("Downloading %d NDOR PDFs: %s → %s (BS) to %s", total, start_bs, end_bs, output_dir.absolute())
    
    for i, bs_date in enumerate(dates, 1):
        success, msg = download_pdf(bs_date, output_dir, force, session)
        
        if success:
            if "Already exists" in msg:
                results["skipped"] += 1
            else:
                results["success"] += 1
        else:
            results["failed"] += 1
        
        results["details"].append(msg)
        if success:
            logger.info("[%3d/%d] %s", i, total, msg)
        else:
            logger.warning("[%3d/%d] %s", i, total, msg)
        
        # Rate limiting (skip for already-existing files)
        if "Already exists" not in msg:
            time.sleep(DELAY_SECONDS)
    
    session.close()
    
    logger.info(
        "Download Summary: %d downloaded, %d skipped, %d failed (total: %d)",
        results["success"], results["skipped"], results["failed"], total,
    )
    
    return results


def download_recent_days(
    days: int = 30,
    output_dir: Path = OUTPUT_DIR,
    force: bool = False,
) -> dict:
    """Download NDOR PDFs for the last N days."""
    today_ad = datetime.date.today()
    start_ad = today_ad - datetime.timedelta(days=days)
    
    start_bs = ad_to_bs(start_ad)
    end_bs = ad_to_bs(today_ad)
    
    if not start_bs or not end_bs:
        logger.error("Could not convert dates to BS.")
        return {"success": 0, "failed": 0, "skipped": 0, "details": []}
    
    return download_date_range(start_bs, end_bs, output_dir, force)


# ─── CLI Entry Point ─────────────────────────────────────────
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Download NDOR PDFs from NEA website",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m extractor.download --date 2081/09/23
  python -m extractor.download --start 2081/09/01 --end 2081/09/30
  python -m extractor.download --days 30
  python -m extractor.download --days 365 --force
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", help="Single BS date to download (YYYY/MM/DD)")
    group.add_argument("--start", help="Start of BS date range (YYYY/MM/DD)")
    group.add_argument("--days", type=int, help="Download last N days")
    
    parser.add_argument("--end", help="End of BS date range (YYYY/MM/DD)")
    parser.add_argument("--output", default=str(OUTPUT_DIR), help="Output directory")
    parser.add_argument("--force", action="store_true", help="Re-download existing files")
    
    args = parser.parse_args()
    output_dir = Path(args.output)
    
    if args.date:
        success, msg = download_pdf(args.date, output_dir, args.force)
        print(msg)
    elif args.start:
        if not args.end:
            logger.error("--end is required with --start")
            sys.exit(1)
        download_date_range(args.start, args.end, output_dir, args.force)
    elif args.days:
        download_recent_days(args.days, output_dir, args.force)


if __name__ == "__main__":
    main()
