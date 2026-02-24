"""
Database Loader — Load extracted CSVs into PostgreSQL.

Reads the bronze CSV files produced by the extraction engine and loads
them into the raw schema in PostgreSQL using psycopg2.

Supports upsert (ON CONFLICT UPDATE) to handle re-extraction of the
same dates without creating duplicates.

Usage:
    python db/load.py                          # Load from default CSV
    python db/load.py --csv data/bronze/daily_grid_report.csv
    python db/load.py --truncate               # Clear and reload
"""

import os
import sys
import csv
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# ─── Database Configuration ──────────────────────────────────

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "nea_warehouse"),
    "user": os.getenv("POSTGRES_USER", "nea_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "nea_secret_2024"),
}

DEFAULT_CSV = Path("data/bronze/daily_grid_report.csv")


# ─── Column Mappings ─────────────────────────────────────────

# CSV columns → PostgreSQL columns mapping
COLUMN_MAPPING = {
    "report_date_bs": ("report_date_bs", "VARCHAR"),
    "report_date_ad": ("report_date_ad", "DATE"),
    "nea_generation_mwh": ("nea_generation_mwh", "NUMERIC"),
    "nea_subsidiary_generation_mwh": ("nea_generation_mwh", "NUMERIC"),  # Combined into NEA
    "ipp_generation_mwh": ("ipp_generation_mwh", "NUMERIC"),
    "total_generation_mwh": ("total_generation_mwh", "NUMERIC"),  # This is in the table as computed
    "total_import_mwh": ("total_import_mwh", "NUMERIC"),
    "total_energy_available_mwh": ("total_energy_available_mwh", "NUMERIC"),
    "energy_export_mwh": ("total_export_mwh", "NUMERIC"),
    "net_energy_met_mwh": ("net_energy_met_mwh", "NUMERIC"),
    "energy_interruption_mwh": ("energy_interruption_mwh", "NUMERIC"),
    "energy_deficit_mwh": ("energy_deficit_mwh", "NUMERIC"),
    "energy_requirement_mwh": ("energy_requirement_mwh", "NUMERIC"),
    "net_exchange_india_mwh": ("india_import_mwh", "NUMERIC"),
    "peak_time": ("peak_time", "VARCHAR"),
    "peak_demand_requirement_mw": ("peak_demand_mw", "NUMERIC"),
    "system_loss_mwh": ("system_loss_mwh", "NUMERIC"),
    "system_loss_pct": ("system_loss_pct", "NUMERIC"),
    "source_file": ("source_file", "VARCHAR"),
}


def get_connection():
    """Create and return a PostgreSQL connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except psycopg2.OperationalError as e:
        logger.error("Could not connect to PostgreSQL: %s", e)
        logger.error("Config: %s:%s/%s", DB_CONFIG['host'], DB_CONFIG['port'], DB_CONFIG['dbname'])
        logger.error("Make sure Docker is running: docker-compose up -d")
        sys.exit(1)


def load_daily_report_csv(csv_path: Path, truncate: bool = False):
    """
    Load daily grid report CSV into PostgreSQL.
    
    Uses upsert (INSERT ... ON CONFLICT UPDATE) to handle duplicates.
    
    Args:
        csv_path: Path to the CSV file
        truncate: If True, clear table before loading
    """
    if not csv_path.exists():
        logger.error("CSV not found: %s — Run extraction first: python -m extractor.extract", csv_path)
        sys.exit(1)
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Optionally truncate
        if truncate:
            cur.execute("TRUNCATE TABLE raw.daily_grid_report RESTART IDENTITY CASCADE;")
            logger.info("Truncated raw.daily_grid_report")
        
        # Read CSV
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        if not rows:
            logger.warning("CSV is empty, nothing to load.")
            return
        
        logger.info("Loading %d rows from %s", len(rows), csv_path.name)
        
        # Prepare upsert query
        insert_sql = """
            INSERT INTO raw.daily_grid_report (
                report_date_bs, report_date_ad,
                total_generation_mwh, nea_generation_mwh, ipp_generation_mwh,
                total_import_mwh, india_import_mwh,
                total_export_mwh, india_export_mwh,
                peak_demand_mw, energy_demand_mwh,
                system_loss_mwh, system_loss_pct,
                net_energy_met_mwh,
                exchange_details, plant_generation,
                source_file
            ) VALUES (
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s, %s,
                %s
            )
            ON CONFLICT (report_date_ad) DO UPDATE SET
                total_generation_mwh = EXCLUDED.total_generation_mwh,
                nea_generation_mwh = EXCLUDED.nea_generation_mwh,
                ipp_generation_mwh = EXCLUDED.ipp_generation_mwh,
                total_import_mwh = EXCLUDED.total_import_mwh,
                india_import_mwh = EXCLUDED.india_import_mwh,
                total_export_mwh = EXCLUDED.total_export_mwh,
                peak_demand_mw = EXCLUDED.peak_demand_mw,
                system_loss_mwh = EXCLUDED.system_loss_mwh,
                system_loss_pct = EXCLUDED.system_loss_pct,
                net_energy_met_mwh = EXCLUDED.net_energy_met_mwh,
                source_file = EXCLUDED.source_file,
                extracted_at = CURRENT_TIMESTAMP;
        """
        
        loaded = 0
        errors = 0
        
        for row in rows:
            try:
                # Build values tuple
                def safe_numeric(key):
                    val = row.get(key, '').strip()
                    if val in ('', 'None', 'False', 'True'):
                        return None
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        return None
                
                # Build exchange details JSON
                exchange_details = json.dumps({
                    "net_exchange_india_mwh": safe_numeric("net_exchange_india_mwh"),
                    "peak_import_mw": safe_numeric("peak_import_mw"),
                    "peak_export_mw": safe_numeric("peak_export_mw"),
                    "peak_net_exchange_india_mw": safe_numeric("peak_net_exchange_india_mw"),
                })
                
                # Build peak/generation details JSON
                plant_details = json.dumps({
                    "nea_subsidiary_generation_mwh": safe_numeric("nea_subsidiary_generation_mwh"),
                    "total_energy_available_mwh": safe_numeric("total_energy_available_mwh"),
                    "energy_interruption_mwh": safe_numeric("energy_interruption_mwh"),
                    "energy_deficit_mwh": safe_numeric("energy_deficit_mwh"),
                    "energy_requirement_mwh": safe_numeric("energy_requirement_mwh"),
                    "peak_time": row.get("peak_time", ""),
                    "peak_generation_mw": safe_numeric("peak_generation_mw"),
                    "peak_availability_mw": safe_numeric("peak_availability_mw"),
                    "peak_demand_met_mw": safe_numeric("peak_demand_met_mw"),
                    "peak_interruption_mw": safe_numeric("peak_interruption_mw"),
                    "peak_deficit_mw": safe_numeric("peak_deficit_mw"),
                    "peak_demand_requirement_mw": safe_numeric("peak_demand_requirement_mw"),
                    "season": row.get("season", ""),
                    "fiscal_year": row.get("fiscal_year", ""),
                    "is_net_exporter": row.get("is_net_exporter", "False") == "True",
                })
                
                values = (
                    row.get("report_date_bs", ""),
                    row.get("report_date_ad", ""),
                    safe_numeric("total_generation_mwh"),
                    safe_numeric("nea_generation_mwh"),
                    safe_numeric("ipp_generation_mwh"),
                    safe_numeric("total_import_mwh"),
                    safe_numeric("net_exchange_india_mwh"),  # India import
                    safe_numeric("energy_export_mwh"),
                    safe_numeric("energy_export_mwh"),  # India export (same for now)
                    safe_numeric("peak_demand_requirement_mw"),
                    safe_numeric("energy_requirement_mwh"),
                    safe_numeric("system_loss_mwh"),
                    safe_numeric("system_loss_pct"),
                    safe_numeric("net_energy_met_mwh"),
                    exchange_details,
                    plant_details,
                    row.get("source_file", ""),
                )
                
                cur.execute(insert_sql, values)
                loaded += 1
                
            except Exception as e:
                errors += 1
                logger.error("Error loading row %s: %s", row.get('report_date_ad', '?'), e)
        
        conn.commit()
        
        # Verify
        cur.execute("SELECT COUNT(*) FROM raw.daily_grid_report;")
        total_count = cur.fetchone()[0]
        
        logger.info("Load Summary: %d loaded/updated, %d errors, %d total rows in DB", loaded, errors, total_count)
        
    except Exception as e:
        conn.rollback()
        logger.error("Database error: %s", e)
        raise
    finally:
        cur.close()
        conn.close()


def verify_data():
    """Print a summary of loaded data for verification."""
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Row count
        cur.execute("SELECT COUNT(*) FROM raw.daily_grid_report;")
        count = cur.fetchone()[0]
        
        # Date range
        cur.execute("""
            SELECT 
                MIN(report_date_ad) as min_date,
                MAX(report_date_ad) as max_date,
                AVG(total_generation_mwh) as avg_gen,
                AVG(peak_demand_mw) as avg_peak,
                AVG(total_import_mwh) as avg_import
            FROM raw.daily_grid_report;
        """)
        row = cur.fetchone()
        
        logger.info("Data Verification: %d records, %s → %s", count, row[0], row[1])
        logger.info("Avg generation: %.0f MWh, Avg peak: %.0f MW, Avg import: %.0f MWh", row[2], row[3], row[4])

        
        # Sample rows
        cur.execute("""
            SELECT report_date_bs, report_date_ad, total_generation_mwh, 
                   total_import_mwh, peak_demand_mw, net_energy_met_mwh
            FROM raw.daily_grid_report 
            ORDER BY report_date_ad DESC 
            LIMIT 5;
        """)
        
        print(f"\n   Latest 5 records:")
        print(f"   {'BS Date':>12} | {'AD Date':>12} | {'Gen (MWh)':>10} | {'Import':>8} | {'Peak (MW)':>9} | {'Net Met':>8}")
        print(f"   {'-'*12}-+-{'-'*12}-+-{'-'*10}-+-{'-'*8}-+-{'-'*9}-+-{'-'*8}")
        
        for r in cur.fetchall():
            print(f"   {r[0]:>12} | {str(r[1]):>12} | {r[2]:>10.0f} | {r[3]:>8.0f} | {r[4]:>9.0f} | {r[5]:>8.0f}")
        
    except Exception as e:
        logger.error("Verification error: %s", e)
    finally:
        cur.close()
        conn.close()


# ─── CLI ──────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Load NDOR CSVs into PostgreSQL")
    parser.add_argument("--csv", default=str(DEFAULT_CSV), help="CSV file to load")
    parser.add_argument("--truncate", action="store_true", help="Clear table before loading")
    parser.add_argument("--verify", action="store_true", help="Just verify existing data")
    
    args = parser.parse_args()
    
    if args.verify:
        verify_data()
    else:
        load_daily_report_csv(Path(args.csv), args.truncate)
        verify_data()


if __name__ == "__main__":
    main()
