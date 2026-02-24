"""
Test Suite for NDOR PDF Extraction Engine.

Tests extraction accuracy, data cleaning, date conversion,
batch processing, and error handling against the sample PDFs in data/.

Run: python -m pytest tests/test_extract.py -v
"""

import os
import sys
import datetime
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extractor.extract import extract_from_pdf, extract_batch, DailyEnergyReport
from extractor.utils import clean_numeric, clean_text, parse_time, extract_date_from_filename
from extractor.bs_calendar import bs_to_ad, ad_to_bs, get_bs_season, get_nepal_fiscal_year


# ─── Test Data ────────────────────────────────────────────────

SAMPLE_DIR = Path("data")
SAMPLE_PDF = SAMPLE_DIR / "NDOR 2081_09_23.pdf"

# Known values from manual inspection of NDOR 2081_09_23.pdf
KNOWN_VALUES_2081_09_23 = {
    "report_date_bs": "2081/09/23",
    "report_date_ad": "2025-01-07",
    "nea_generation_mwh": 7376.0,
    "nea_subsidiary_generation_mwh": 5296.0,
    "ipp_generation_mwh": 15231.0,
    "total_generation_mwh": 27903.0,
    "total_import_mwh": 7438.0,
    "total_energy_available_mwh": 35340.0,
    "energy_export_mwh": 0.0,
    "net_energy_met_mwh": 35340.0,
    "peak_time": "18:00",
    "peak_demand_requirement_mw": 2036.0,
}


# ─── Utils Tests ──────────────────────────────────────────────

class TestCleanNumeric:
    """Test numeric string cleaning."""
    
    def test_integer_string(self):
        assert clean_numeric("7376") == 7376.0
    
    def test_comma_separated(self):
        assert clean_numeric("32,269") == 32269.0
    
    def test_with_whitespace(self):
        assert clean_numeric("  1,234 \n") == 1234.0
    
    def test_negative_parentheses(self):
        assert clean_numeric("(500)") == -500.0
    
    def test_negative_sign(self):
        assert clean_numeric("-123") == -123.0
    
    def test_none_input(self):
        assert clean_numeric(None) is None
    
    def test_empty_string(self):
        assert clean_numeric("") is None
    
    def test_dash(self):
        assert clean_numeric("-") is None
    
    def test_double_dash(self):
        assert clean_numeric("--") is None
    
    def test_zero(self):
        assert clean_numeric("0") == 0.0
    
    def test_float_string(self):
        assert clean_numeric("12.5") == 12.5
    
    def test_na_string(self):
        assert clean_numeric("N/A") is None
    
    def test_na_lowercase(self):
        assert clean_numeric("n/a") is None
    
    def test_null_string(self):
        assert clean_numeric("null") is None
    
    def test_none_string(self):
        assert clean_numeric("None") is None
    
    def test_large_number_with_commas(self):
        assert clean_numeric("1,234,567") == 1234567.0
    
    def test_negative_parentheses_with_commas(self):
        assert clean_numeric("(1,234)") == -1234.0


class TestCleanText:
    """Test text normalization."""
    
    def test_basic(self):
        assert clean_text("Hello World") == "Hello World"
    
    def test_newlines(self):
        assert clean_text("Hello\nWorld") == "Hello World"
    
    def test_extra_whitespace(self):
        assert clean_text("  Hello   World  ") == "Hello World"
    
    def test_none(self):
        assert clean_text(None) is None
    
    def test_empty(self):
        assert clean_text("   ") is None
    
    def test_tabs_and_newlines(self):
        assert clean_text("  Hello\t\n  World  ") == "Hello World"


class TestParseTime:
    """Test time string parsing."""
    
    def test_24h_format(self):
        assert parse_time("18:00") == "18:00"
    
    def test_dot_separator(self):
        assert parse_time("18.00") == "18:00"
    
    def test_12h_pm(self):
        assert parse_time("6:00 PM") == "18:00"
    
    def test_12h_am(self):
        assert parse_time("12:00 AM") == "00:00"
    
    def test_none(self):
        assert parse_time(None) is None


class TestExtractDateFromFilename:
    """Test BS date extraction from filenames."""
    
    def test_space_format(self):
        assert extract_date_from_filename("NDOR 2081_09_23.pdf") == "2081/09/23"
    
    def test_underscore_format(self):
        assert extract_date_from_filename("NDOR_2081_09_23.pdf") == "2081/09/23"
    
    def test_single_digit_month(self):
        assert extract_date_from_filename("NDOR 2081_9_3.pdf") == "2081/09/03"
    
    def test_invalid(self):
        assert extract_date_from_filename("random_file.pdf") is None
    
    def test_empty_string(self):
        assert extract_date_from_filename("") is None


# ─── Calendar Tests ───────────────────────────────────────────

class TestBSCalendar:
    """Test Bikram Sambat date operations."""
    
    def test_bs_to_ad_known_date(self):
        """2081/09/23 BS = January 7, 2025 AD"""
        result = bs_to_ad("2081/09/23")
        assert result == datetime.date(2025, 1, 7)
    
    def test_bs_to_ad_with_dashes(self):
        result = bs_to_ad("2081-09-23")
        assert result == datetime.date(2025, 1, 7)
    
    def test_ad_to_bs_known_date(self):
        result = ad_to_bs(datetime.date(2025, 1, 7))
        assert result == "2081/09/23"
    
    def test_roundtrip(self):
        """BS → AD → BS should return the same date."""
        original = "2081/09/23"
        ad = bs_to_ad(original)
        back = ad_to_bs(ad)
        assert back == original
    
    def test_bs_to_ad_invalid_date(self):
        """Invalid BS date should return None, not crash."""
        result = bs_to_ad("9999/99/99")
        assert result is None
    
    def test_bs_to_ad_malformed_string(self):
        """Completely malformed input should return None."""
        assert bs_to_ad("not-a-date") is None
        assert bs_to_ad("") is None
    
    def test_bs_to_ad_partial_date(self):
        """Incomplete date string should return None."""
        assert bs_to_ad("2081/09") is None
    
    def test_season_monsoon(self):
        assert get_bs_season(4) == "monsoon"  # Shrawan
        assert get_bs_season(5) == "monsoon"  # Bhadra
        assert get_bs_season(6) == "monsoon"  # Ashwin
    
    def test_season_dry(self):
        assert get_bs_season(9) == "dry"   # Poush
        assert get_bs_season(10) == "dry"  # Magh
    
    def test_season_pre_monsoon(self):
        assert get_bs_season(1) == "pre_monsoon"  # Baisakh
        assert get_bs_season(3) == "pre_monsoon"  # Ashadh
    
    def test_season_boundary_months(self):
        """Test every month maps to a valid season."""
        valid_seasons = {"monsoon", "dry", "pre_monsoon"}
        for month in range(1, 13):
            assert get_bs_season(month) in valid_seasons
    
    def test_fiscal_year(self):
        # Shrawan 2081 = FY 2081/82
        assert get_nepal_fiscal_year(2081, 4) == "2081/82"
        # Poush 2081 = still FY 2081/82
        assert get_nepal_fiscal_year(2081, 9) == "2081/82"
        # Baisakh 2081 = FY 2080/81
        assert get_nepal_fiscal_year(2081, 1) == "2080/81"
    
    def test_fiscal_year_boundary(self):
        """Month 3 (Ashadh) is last month of old FY, month 4 (Shrawan) is first of new FY."""
        assert get_nepal_fiscal_year(2081, 3) == "2080/81"
        assert get_nepal_fiscal_year(2081, 4) == "2081/82"


# ─── Extraction Tests ────────────────────────────────────────

class TestPDFExtraction:
    """Test PDF extraction against known sample data."""
    
    @pytest.fixture
    def sample_report(self):
        """Extract data from the known sample PDF."""
        if not SAMPLE_PDF.exists():
            pytest.skip(f"Sample PDF not found: {SAMPLE_PDF}")
        return extract_from_pdf(str(SAMPLE_PDF))
    
    def test_extraction_succeeds(self, sample_report):
        """Extraction should return a valid report."""
        assert sample_report is not None
        assert isinstance(sample_report, DailyEnergyReport)
    
    def test_date_extraction(self, sample_report):
        """Check BS and AD dates are correct."""
        assert sample_report.report_date_bs == KNOWN_VALUES_2081_09_23["report_date_bs"]
        assert sample_report.report_date_ad == KNOWN_VALUES_2081_09_23["report_date_ad"]
    
    def test_nea_generation(self, sample_report):
        """Check NEA generation value."""
        assert sample_report.nea_generation_mwh == KNOWN_VALUES_2081_09_23["nea_generation_mwh"]
    
    def test_ipp_generation(self, sample_report):
        """Check IPP generation value."""
        assert sample_report.ipp_generation_mwh == KNOWN_VALUES_2081_09_23["ipp_generation_mwh"]
    
    def test_total_generation_computed(self, sample_report):
        """Total generation should be NEA + Subsidiary + IPP."""
        assert sample_report.total_generation_mwh == KNOWN_VALUES_2081_09_23["total_generation_mwh"]
    
    def test_import_value(self, sample_report):
        """Check import value."""
        assert sample_report.total_import_mwh == KNOWN_VALUES_2081_09_23["total_import_mwh"]
    
    def test_net_energy_met(self, sample_report):
        """Check net energy met value."""
        assert sample_report.net_energy_met_mwh == KNOWN_VALUES_2081_09_23["net_energy_met_mwh"]
    
    def test_peak_time(self, sample_report):
        """Check peak time extraction."""
        assert sample_report.peak_time == KNOWN_VALUES_2081_09_23["peak_time"]
    
    def test_peak_demand(self, sample_report):
        """Check peak demand requirement."""
        assert sample_report.peak_demand_requirement_mw == KNOWN_VALUES_2081_09_23["peak_demand_requirement_mw"]
    
    def test_season_is_dry(self, sample_report):
        """January (Poush) should be dry season."""
        assert sample_report.season == "dry"
    
    def test_fiscal_year(self, sample_report):
        """Poush 2081 should be FY 2081/82."""
        assert sample_report.fiscal_year == "2081/82"
    
    def test_not_net_exporter(self, sample_report):
        """Nepal is a net importer in dry season."""
        assert sample_report.is_net_exporter == False
    
    def test_all_numeric_fields_are_numbers(self, sample_report):
        """All numeric fields should be float or None, never strings."""
        numeric_fields = [
            'nea_generation_mwh', 'ipp_generation_mwh', 'total_import_mwh',
            'total_energy_available_mwh', 'energy_export_mwh', 'net_energy_met_mwh',
            'peak_generation_mw', 'peak_demand_requirement_mw',
        ]
        for field_name in numeric_fields:
            value = getattr(sample_report, field_name)
            assert value is None or isinstance(value, (int, float)), \
                f"{field_name} should be numeric, got {type(value)}: {value}"
    
    def test_source_file_recorded(self, sample_report):
        """Source filename should be stored in report."""
        assert sample_report.source_file == "NDOR 2081_09_23.pdf"


# ─── Error Path Tests ────────────────────────────────────────

class TestErrorPaths:
    """Test graceful handling of bad inputs — corrupt files, missing data."""
    
    def test_nonexistent_pdf(self):
        """Extracting from a file that doesn't exist should return None."""
        result = extract_from_pdf("nonexistent_file.pdf")
        assert result is None
    
    def test_corrupt_pdf(self, tmp_path):
        """A file that isn't a valid PDF should return None, not crash."""
        fake_pdf = tmp_path / "corrupt.pdf"
        fake_pdf.write_text("This is not a PDF at all.")
        result = extract_from_pdf(str(fake_pdf))
        assert result is None
    
    def test_empty_file(self, tmp_path):
        """A zero-byte file should return None."""
        empty_file = tmp_path / "empty.pdf"
        empty_file.write_bytes(b"")
        result = extract_from_pdf(str(empty_file))
        assert result is None
    
    def test_extract_batch_empty_dir(self, tmp_path):
        """Batch extraction on an empty directory should return empty list."""
        output_dir = tmp_path / "output"
        reports = extract_batch(str(tmp_path), str(output_dir))
        assert reports == []
    
    def test_extract_batch_no_pdfs(self, tmp_path):
        """Directory with non-PDF files should return empty list."""
        (tmp_path / "readme.txt").write_text("not a pdf")
        (tmp_path / "data.csv").write_text("a,b,c")
        output_dir = tmp_path / "output"
        reports = extract_batch(str(tmp_path), str(output_dir))
        assert reports == []


# ─── Batch Extraction Tests ──────────────────────────────────

class TestBatchExtraction:
    """Test batch PDF extraction with proper temp directories."""
    
    def test_batch_extracts_all(self, tmp_path):
        """Batch extraction should process all 5 sample PDFs."""
        if not SAMPLE_DIR.exists():
            pytest.skip("Sample directory not found")
        
        output_dir = tmp_path / "bronze"
        reports = extract_batch(str(SAMPLE_DIR), str(output_dir))
        assert len(reports) == 5
    
    def test_batch_dates_are_sequential(self, tmp_path):
        """The 5 sample PDFs should have consecutive dates."""
        if not SAMPLE_DIR.exists():
            pytest.skip("Sample directory not found")
        
        output_dir = tmp_path / "bronze"
        reports = extract_batch(str(SAMPLE_DIR), str(output_dir))
        dates = sorted([r.report_date_ad for r in reports])
        
        assert dates == [
            "2025-01-07",
            "2025-01-08",
            "2025-01-09",
            "2025-01-10",
            "2025-01-11",
        ]
    
    def test_batch_csv_created(self, tmp_path):
        """Batch extraction should produce a CSV file."""
        if not SAMPLE_DIR.exists():
            pytest.skip("Sample directory not found")
        
        output_dir = tmp_path / "bronze"
        extract_batch(str(SAMPLE_DIR), str(output_dir))
        csv_path = output_dir / "daily_grid_report.csv"
        assert csv_path.exists()
        
        # Check CSV has header + 5 data rows
        with open(csv_path, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 6  # 1 header + 5 data
    
    def test_batch_json_created(self, tmp_path):
        """Batch extraction should also produce a JSON file."""
        if not SAMPLE_DIR.exists():
            pytest.skip("Sample directory not found")
        
        output_dir = tmp_path / "bronze"
        extract_batch(str(SAMPLE_DIR), str(output_dir))
        json_path = output_dir / "daily_grid_report.json"
        assert json_path.exists()


# ─── Dataclass Tests ──────────────────────────────────────────

class TestDailyEnergyReport:
    """Test the DailyEnergyReport dataclass computed fields."""
    
    def test_compute_total_generation(self):
        """Total generation = NEA + Subsidiary + IPP."""
        report = DailyEnergyReport(
            nea_generation_mwh=100.0,
            nea_subsidiary_generation_mwh=50.0,
            ipp_generation_mwh=200.0,
        )
        report.compute_derived()
        assert report.total_generation_mwh == 350.0
    
    def test_compute_total_generation_with_none(self):
        """If any component is None, total should be None."""
        report = DailyEnergyReport(
            nea_generation_mwh=100.0,
            nea_subsidiary_generation_mwh=None,
            ipp_generation_mwh=200.0,
        )
        report.compute_derived()
        assert report.total_generation_mwh is None
    
    def test_net_exporter_flag(self):
        """Net exporter when export > import."""
        report = DailyEnergyReport(
            energy_export_mwh=500.0,
            total_import_mwh=100.0,
        )
        report.compute_derived()
        assert report.is_net_exporter is True
    
    def test_net_importer_flag(self):
        """Net importer when import > export."""
        report = DailyEnergyReport(
            energy_export_mwh=0.0,
            total_import_mwh=7438.0,
        )
        report.compute_derived()
        assert report.is_net_exporter is False
    
    def test_system_loss_calculation(self):
        """System loss = Available - Net Met - Export."""
        report = DailyEnergyReport(
            total_energy_available_mwh=35000.0,
            net_energy_met_mwh=33000.0,
            energy_export_mwh=0.0,
        )
        report.compute_derived()
        assert report.system_loss_mwh == 2000.0
        assert abs(report.system_loss_pct - 5.714) < 0.01
    
    def test_season_from_bs_month(self):
        """Season computed from BS month."""
        report = DailyEnergyReport(bs_month=9)
        report.compute_derived()
        assert report.season == "dry"
    
    def test_fiscal_year_from_bs(self):
        """Fiscal year computed from BS year/month."""
        report = DailyEnergyReport(bs_year=2081, bs_month=9)
        report.compute_derived()
        assert report.fiscal_year == "2081/82"


# ─── Entry Point ──────────────────────────────────────────────

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
