-- =============================================================
-- NEA Data Warehouse — Database Initialization
-- Runs automatically on first 'docker-compose up'
-- =============================================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- =============================================================
-- RAW LANDING TABLE — daily grid report from NDOR PDFs
-- =============================================================
CREATE TABLE IF NOT EXISTS raw.daily_grid_report (
    id                      SERIAL PRIMARY KEY,
    
    -- Date fields
    report_date_bs          VARCHAR(20)     NOT NULL,   -- e.g. '2081-09-23'
    report_date_ad          DATE            NOT NULL,   -- Gregorian equivalent
    
    -- Daily Energy Summary (MWh)
    total_generation_mwh    NUMERIC(12,2),
    nea_generation_mwh      NUMERIC(12,2),
    ipp_generation_mwh      NUMERIC(12,2),
    total_import_mwh        NUMERIC(12,2),
    india_import_mwh        NUMERIC(12,2),
    total_export_mwh        NUMERIC(12,2),
    india_export_mwh        NUMERIC(12,2),
    
    -- Demand & Loss
    peak_demand_mw          NUMERIC(10,2),
    energy_demand_mwh       NUMERIC(12,2),
    system_loss_mwh         NUMERIC(12,2),
    system_loss_pct         NUMERIC(5,2),
    
    -- Net Energy
    net_energy_met_mwh      NUMERIC(12,2),
    
    -- Cross-border exchange details
    exchange_details        JSONB,
    
    -- Plant-wise breakdown
    plant_generation        JSONB,
    
    -- Metadata
    source_file             VARCHAR(255),
    extracted_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Prevent duplicate dates
    CONSTRAINT uq_report_date UNIQUE (report_date_ad)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_report_date_ad ON raw.daily_grid_report(report_date_ad);
CREATE INDEX IF NOT EXISTS idx_report_date_bs ON raw.daily_grid_report(report_date_bs);

-- =============================================================
-- METABASE DATABASE — separate DB for Metabase internal state
-- =============================================================
SELECT 'CREATE DATABASE metabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

-- Grant access
GRANT ALL PRIVILEGES ON SCHEMA raw TO nea_admin;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO nea_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO nea_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO nea_admin;

-- =============================================================
-- Plant-wise generation breakdown table (for detailed analysis)
-- =============================================================
CREATE TABLE IF NOT EXISTS raw.plant_generation (
    id                      SERIAL PRIMARY KEY,
    report_date_ad          DATE            NOT NULL,
    plant_name              VARCHAR(255)    NOT NULL,
    plant_type              VARCHAR(50),     -- 'Hydro RoR', 'Hydro Storage', 'Solar', 'Thermal'
    owner_type              VARCHAR(50),     -- 'NEA', 'IPP'
    installed_capacity_mw   NUMERIC(10,2),
    generation_mwh          NUMERIC(12,2),
    availability_pct        NUMERIC(5,2),
    source_file             VARCHAR(255),
    extracted_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_plant_date UNIQUE (report_date_ad, plant_name)
);

CREATE INDEX IF NOT EXISTS idx_plant_date ON raw.plant_generation(report_date_ad);

-- =============================================================
-- Cross-border exchange details table
-- =============================================================
CREATE TABLE IF NOT EXISTS raw.cross_border_exchange (
    id                      SERIAL PRIMARY KEY,
    report_date_ad          DATE            NOT NULL,
    interconnection_point   VARCHAR(255)    NOT NULL,
    direction               VARCHAR(10)     NOT NULL,  -- 'import' or 'export'
    energy_mwh              NUMERIC(12,2),
    max_power_mw            NUMERIC(10,2),
    source_file             VARCHAR(255),
    extracted_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_exchange_date UNIQUE (report_date_ad, interconnection_point, direction)
);

CREATE INDEX IF NOT EXISTS idx_exchange_date ON raw.cross_border_exchange(report_date_ad);

-- ✅ NEA Data Warehouse initialized successfully!
