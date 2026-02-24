"""
NDOR PDF Extraction Engine — The Core IP.

Extracts structured data from NEA's Nepal Daily Operational Report PDFs
using pdfplumber. Each PDF contains a single landscape page with:

1. Daily Energy Values table (11 columns):
   NEA | NEA Subsidiary | IPP | Import | Total Energy Available |
   Energy Export | Net Energy Met | Interruption | Deficit | 
   Energy Requirement | Net exchange with India

2. Peak Time Generation/Demand table (10 columns):
   Peak Time | Generation | Import | Recorded Peak Availability |
   Export | Demand met at Peak Time | Interruption | Deficit |
   Peak Demand (Requirement) | Net exchange with India

Usage:
    # Extract a single PDF
    python -m extractor.extract --file data/NDOR_2081_09_23.pdf
    
    # Extract all PDFs in a directory
    python -m extractor.extract --dir data/raw_pdfs/
    
    # Extract from data/ (default)
    python -m extractor.extract
"""

import os
import sys
import csv
import json
import glob
import logging
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict

import pdfplumber

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extractor.utils import clean_numeric, clean_text, parse_time, extract_date_from_filename
from extractor.bs_calendar import bs_to_ad, parse_bs_date_from_text, get_bs_season, get_nepal_fiscal_year

logger = logging.getLogger(__name__)


# ─── Data Models ──────────────────────────────────────────────

@dataclass
class DailyEnergyReport:
    """Structured representation of one NDOR report."""
    
    # Date
    report_date_bs: str = ""
    report_date_ad: str = ""
    bs_year: int = 0
    bs_month: int = 0
    bs_day: int = 0
    
    # Daily Energy Values (MWh)
    nea_generation_mwh: Optional[float] = None
    nea_subsidiary_generation_mwh: Optional[float] = None
    ipp_generation_mwh: Optional[float] = None
    total_import_mwh: Optional[float] = None
    total_energy_available_mwh: Optional[float] = None
    energy_export_mwh: Optional[float] = None
    net_energy_met_mwh: Optional[float] = None
    energy_interruption_mwh: Optional[float] = None
    energy_deficit_mwh: Optional[float] = None
    energy_requirement_mwh: Optional[float] = None
    net_exchange_india_mwh: Optional[float] = None
    
    # Computed
    total_generation_mwh: Optional[float] = None
    
    # Peak Time Data (MW)
    peak_time: Optional[str] = None
    peak_generation_mw: Optional[float] = None
    peak_import_mw: Optional[float] = None
    peak_availability_mw: Optional[float] = None
    peak_export_mw: Optional[float] = None
    peak_demand_met_mw: Optional[float] = None
    peak_interruption_mw: Optional[float] = None
    peak_deficit_mw: Optional[float] = None
    peak_demand_requirement_mw: Optional[float] = None
    peak_net_exchange_india_mw: Optional[float] = None
    
    # Derived metrics
    season: str = ""
    fiscal_year: str = ""
    is_net_exporter: bool = False
    system_loss_mwh: Optional[float] = None
    system_loss_pct: Optional[float] = None
    
    # Metadata
    source_file: str = ""
    
    def compute_derived(self):
        """Calculate derived metrics after extraction."""
        # Total generation = NEA + Subsidiary + IPP
        gen_parts = [self.nea_generation_mwh, self.nea_subsidiary_generation_mwh, self.ipp_generation_mwh]
        if all(v is not None for v in gen_parts):
            self.total_generation_mwh = sum(gen_parts)
        
        # Net exporter if export > import
        if self.energy_export_mwh is not None and self.total_import_mwh is not None:
            self.is_net_exporter = self.energy_export_mwh > self.total_import_mwh
        
        # System loss ≈ Total Available - Net Energy Met - Export
        if all(v is not None for v in [self.total_energy_available_mwh, self.net_energy_met_mwh, self.energy_export_mwh]):
            self.system_loss_mwh = self.total_energy_available_mwh - self.net_energy_met_mwh - self.energy_export_mwh
            if self.total_energy_available_mwh > 0:
                self.system_loss_pct = (self.system_loss_mwh / self.total_energy_available_mwh) * 100
        
        # Season and fiscal year from BS date
        if self.bs_month > 0:
            self.season = get_bs_season(self.bs_month)
        if self.bs_year > 0 and self.bs_month > 0:
            self.fiscal_year = get_nepal_fiscal_year(self.bs_year, self.bs_month)
        
        # Cross-validate: computed total vs PDF total
        self._validate_energy_balance()
    
    def _validate_energy_balance(self):
        """Warn if extracted values don't add up — catches extraction drift."""
        if self.total_generation_mwh is not None and self.total_import_mwh is not None and self.total_energy_available_mwh is not None:
            computed_available = self.total_generation_mwh + self.total_import_mwh
            diff = abs(computed_available - self.total_energy_available_mwh)
            if diff > 10:  # Tolerance for rounding
                logger.warning(
                    "Energy balance mismatch on %s: gen(%s) + import(%s) = %s, but PDF says available = %s (diff=%.1f MWh)",
                    self.report_date_ad,
                    self.total_generation_mwh, self.total_import_mwh,
                    computed_available, self.total_energy_available_mwh, diff,
                )


# ─── Known Table Headers (for validation) ───────────────────

# Words expected in the Daily Energy table header rows
DAILY_ENERGY_HEADER_KEYWORDS = {"nea", "generation", "import", "export", "energy"}

# Words expected in the Peak Time table header rows
PEAK_TIME_HEADER_KEYWORDS = {"peak", "time", "generation", "demand"}


def _table_matches_keywords(table: List[List], keywords: set) -> bool:
    """Check if a table's header rows contain expected keywords."""
    header_text = ""
    for row in table[:3]:  # Check first 3 rows for headers
        for cell in row:
            if cell:
                header_text += " " + str(cell).lower()
    
    matched = sum(1 for kw in keywords if kw in header_text)
    return matched >= 2  # At least 2 keywords must match


# ─── Extraction Logic ────────────────────────────────────────

def extract_from_pdf(pdf_path: str) -> Optional[DailyEnergyReport]:
    """
    Extract all data from a single NDOR PDF.
    
    Args:
        pdf_path: Path to the NDOR PDF file
    
    Returns:
        DailyEnergyReport with all extracted and derived data,
        or None if extraction fails
    """
    pdf_path = str(pdf_path)
    filename = os.path.basename(pdf_path)
    
    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        logger.error("Could not open PDF: %s — %s", pdf_path, e)
        return None
    
    report = DailyEnergyReport(source_file=filename)
    
    try:
        if len(pdf.pages) == 0:
            logger.error("Empty PDF: %s", pdf_path)
            return None
        
        page = pdf.pages[0]  # NDOR is always a single page
        
        # ─── Extract Date ────────────────────────────
        text = page.extract_text() or ""
        
        # Try to get date from PDF text first
        bs_date = parse_bs_date_from_text(text)
        
        # Fallback: extract from filename
        if not bs_date:
            bs_date = extract_date_from_filename(filename)
        
        if bs_date:
            report.report_date_bs = bs_date
            parts = bs_date.split('/')
            report.bs_year = int(parts[0])
            report.bs_month = int(parts[1])
            report.bs_day = int(parts[2])
            
            ad_date = bs_to_ad(bs_date)
            if ad_date:
                report.report_date_ad = ad_date.isoformat()
        
        # ─── Extract Tables (with header validation) ──
        tables = page.extract_tables()
        
        daily_table = None
        peak_table = None
        
        # Identify tables by their header content, not by position
        for table in tables:
            if daily_table is None and _table_matches_keywords(table, DAILY_ENERGY_HEADER_KEYWORDS):
                daily_table = table
            elif peak_table is None and _table_matches_keywords(table, PEAK_TIME_HEADER_KEYWORDS):
                peak_table = table
        
        # Fallback to positional if header matching fails
        if daily_table is None and len(tables) >= 1:
            logger.warning("Header validation failed for %s — falling back to positional table matching", filename)
            daily_table = tables[0]
        if peak_table is None and len(tables) >= 2:
            peak_table = tables[1]
        
        if daily_table is not None:
            _parse_daily_energy_table(daily_table, report)
        
        if peak_table is not None:
            _parse_peak_time_table(peak_table, report)
        
        # If tables didn't work well, try text-based extraction
        if report.nea_generation_mwh is None:
            logger.info("Table extraction returned no data for %s — trying text fallback", filename)
            _parse_from_text(text, report)
        
        # Calculate derived fields
        report.compute_derived()
        
        return report
    
    except Exception as e:
        logger.error("Error extracting %s: %s", pdf_path, e, exc_info=True)
        return None
    finally:
        pdf.close()


def _parse_daily_energy_table(table: List[List], report: DailyEnergyReport):
    """
    Parse the Daily Energy Values table.
    
    Expected structure (after header rows):
    Row with data: [NEA, NEA_Sub, IPP, Import, Total, Export, NetMet, Interruption, Deficit, Requirement, NetExchange]
    """
    # Find the data row (last row with numeric content)
    for row in reversed(table):
        if row and any(clean_numeric(cell) is not None for cell in row if cell):
            values = [clean_numeric(cell) for cell in row]
            
            # Map values based on the 11-column structure
            if len(values) >= 11:
                report.nea_generation_mwh = values[0]
                report.nea_subsidiary_generation_mwh = values[1]
                report.ipp_generation_mwh = values[2]
                report.total_import_mwh = values[3]
                report.total_energy_available_mwh = values[4]
                report.energy_export_mwh = values[5]
                report.net_energy_met_mwh = values[6]
                report.energy_interruption_mwh = values[7]
                report.energy_deficit_mwh = values[8]
                report.energy_requirement_mwh = values[9]
                report.net_exchange_india_mwh = values[10]
            elif len(values) >= 5:
                # Partial extraction — log warning
                logger.warning(
                    "Partial daily energy extraction (%d/%d columns) for %s",
                    len(values), 11, report.source_file,
                )
                report.nea_generation_mwh = values[0]
                report.nea_subsidiary_generation_mwh = values[1] if len(values) > 1 else None
                report.ipp_generation_mwh = values[2] if len(values) > 2 else None
                report.total_import_mwh = values[3] if len(values) > 3 else None
                report.total_energy_available_mwh = values[4] if len(values) > 4 else None
            
            break  # Found data row


def _parse_peak_time_table(table: List[List], report: DailyEnergyReport):
    """
    Parse the Peak Time Generation and Demand table.
    
    Expected structure:
    Row with data: [Time, Gen, Import, Availability, Export, DemandMet, Interruption, Deficit, Requirement, NetExchange]
    """
    for row in reversed(table):
        if row and any(cell and ':' in str(cell) for cell in row[:2] if cell):
            # This row contains the time, so it's the data row
            report.peak_time = parse_time(row[0])
            
            values = [clean_numeric(cell) for cell in row[1:]]
            
            if len(values) >= 9:
                report.peak_generation_mw = values[0]
                report.peak_import_mw = values[1]
                report.peak_availability_mw = values[2]
                report.peak_export_mw = values[3]
                report.peak_demand_met_mw = values[4]
                report.peak_interruption_mw = values[5]
                report.peak_deficit_mw = values[6]
                report.peak_demand_requirement_mw = values[7]
                report.peak_net_exchange_india_mw = values[8]
            
            break


def _parse_from_text(text: str, report: DailyEnergyReport):
    """
    Fallback: extract data from raw text when table extraction fails.
    
    Looks for the data line pattern in the text (line of space-separated numbers).
    """
    import re
    
    lines = text.split('\n')
    
    for line in lines:
        # Look for lines that are primarily numbers (the data rows)
        nums = re.findall(r'[\d,]+\.?\d*', line)
        if len(nums) >= 10:
            values = [clean_numeric(n) for n in nums]
            
            # Check if this looks like the energy data row (values in thousands)
            if values[0] and values[0] > 100:
                if report.nea_generation_mwh is None:
                    report.nea_generation_mwh = values[0]
                    report.nea_subsidiary_generation_mwh = values[1]
                    report.ipp_generation_mwh = values[2]
                    report.total_import_mwh = values[3]
                    report.total_energy_available_mwh = values[4]
                    report.energy_export_mwh = values[5]
                    report.net_energy_met_mwh = values[6]
                    report.energy_interruption_mwh = values[7]
                    report.energy_deficit_mwh = values[8]
                    report.energy_requirement_mwh = values[9]
                    if len(values) > 10:
                        report.net_exchange_india_mwh = values[10]
    
    # Peak time data
    for line in lines:
        if ':' in line and any(c.isdigit() for c in line):
            time_match = re.match(r'(\d{1,2}:\d{2})', line.strip())
            if time_match and report.peak_time is None:
                report.peak_time = time_match.group(1)
                nums = re.findall(r'[\d,]+\.?\d*', line[time_match.end():])
                values = [clean_numeric(n) for n in nums]
                
                if len(values) >= 9:
                    report.peak_generation_mw = values[0]
                    report.peak_import_mw = values[1]
                    report.peak_availability_mw = values[2]
                    report.peak_export_mw = values[3]
                    report.peak_demand_met_mw = values[4]
                    report.peak_interruption_mw = values[5]
                    report.peak_deficit_mw = values[6]
                    report.peak_demand_requirement_mw = values[7]
                    report.peak_net_exchange_india_mw = values[8]


# ─── Batch Processing ────────────────────────────────────────

def extract_batch(
    pdf_dir: str,
    output_dir: str = "data/bronze",
    pattern: str = "*.pdf"
) -> List[DailyEnergyReport]:
    """
    Extract data from all PDFs in a directory.
    
    Args:
        pdf_dir: Directory containing NDOR PDFs
        output_dir: Where to write CSV output
        pattern: Glob pattern for PDF files
    
    Returns:
        List of extracted reports
    """
    pdf_dir = Path(pdf_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all PDFs
    pdf_files = sorted(glob.glob(str(pdf_dir / pattern)))
    
    # Also check for space-based naming (NEA uses spaces)
    if not pdf_files:
        pdf_files = sorted(glob.glob(str(pdf_dir / "NDOR*.pdf")))
    
    if not pdf_files:
        logger.warning("No PDFs found in %s", pdf_dir)
        return []
    
    logger.info("Extracting %d NDOR PDFs from %s → %s", len(pdf_files), pdf_dir.absolute(), output_dir.absolute())
    
    reports = []
    success = 0
    failed = 0
    
    for i, pdf_file in enumerate(pdf_files, 1):
        report = extract_from_pdf(pdf_file)
        
        if report:
            reports.append(report)
            success += 1
            logger.info(
                "[%3d/%d] %s → %s (%s): Gen=%s MWh, Import=%s MWh, Peak=%s MW",
                i, len(pdf_files), report.source_file,
                report.report_date_bs, report.report_date_ad,
                report.total_generation_mwh,
                report.total_import_mwh,
                report.peak_demand_requirement_mw,
            )
        else:
            failed += 1
            logger.error("[%3d/%d] Failed: %s", i, len(pdf_files), os.path.basename(pdf_file))
    
    # Write CSV
    if reports:
        csv_path = output_dir / "daily_grid_report.csv"
        _write_csv(reports, csv_path)
        
        # Also write detailed JSON for debugging
        json_path = output_dir / "daily_grid_report.json"
        _write_json(reports, json_path)
    
    logger.info(
        "Extraction Summary: %d extracted, %d failed, output → %s",
        success, failed, output_dir.absolute(),
    )
    
    return reports


def _write_csv(reports: List[DailyEnergyReport], csv_path: Path):
    """Write extracted reports to CSV."""
    if not reports:
        return
    
    # Define column order for CSV
    columns = [
        'report_date_bs', 'report_date_ad', 'bs_year', 'bs_month', 'bs_day',
        'nea_generation_mwh', 'nea_subsidiary_generation_mwh', 'ipp_generation_mwh',
        'total_generation_mwh', 'total_import_mwh', 'total_energy_available_mwh',
        'energy_export_mwh', 'net_energy_met_mwh', 'energy_interruption_mwh',
        'energy_deficit_mwh', 'energy_requirement_mwh', 'net_exchange_india_mwh',
        'peak_time', 'peak_generation_mw', 'peak_import_mw', 'peak_availability_mw',
        'peak_export_mw', 'peak_demand_met_mw', 'peak_interruption_mw',
        'peak_deficit_mw', 'peak_demand_requirement_mw', 'peak_net_exchange_india_mw',
        'season', 'fiscal_year', 'is_net_exporter',
        'system_loss_mwh', 'system_loss_pct',
        'source_file',
    ]
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        
        for report in sorted(reports, key=lambda r: r.report_date_ad):
            row = asdict(report)
            writer.writerow({k: row.get(k, '') for k in columns})
    
    logger.info("CSV written: %s (%d rows)", csv_path, len(reports))


def _write_json(reports: List[DailyEnergyReport], json_path: Path):
    """Write extracted reports to JSON for debugging/inspection."""
    data = [asdict(r) for r in sorted(reports, key=lambda r: r.report_date_ad)]
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)
    
    logger.info("JSON written: %s (%d records)", json_path, len(reports))


# ─── CLI Entry Point ─────────────────────────────────────────

def main():
    # Configure logging for CLI usage
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Extract data from NDOR PDFs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--file", help="Extract a single PDF file")
    group.add_argument("--dir", default="data", help="Directory of PDFs to extract (default: data/)")
    
    parser.add_argument("--output", default="data/bronze", help="Output directory for CSVs")
    
    args = parser.parse_args()
    
    if args.file:
        report = extract_from_pdf(args.file)
        if report:
            # Write single report
            output_dir = Path(args.output)
            output_dir.mkdir(parents=True, exist_ok=True)
            _write_csv([report], output_dir / "daily_grid_report.csv")
            _write_json([report], output_dir / "daily_grid_report.json")
            
            # Print summary
            logger.info(
                "Extracted: %s (%s) — Gen=%s MWh, Import=%s MWh, Peak=%s MW @ %s, Season=%s, FY=%s",
                report.report_date_bs, report.report_date_ad,
                report.total_generation_mwh, report.total_import_mwh,
                report.peak_demand_requirement_mw, report.peak_time,
                report.season, report.fiscal_year,
            )
        else:
            logger.error("Extraction failed for %s", args.file)
            sys.exit(1)
    else:
        extract_batch(args.dir, args.output)


if __name__ == "__main__":
    main()
