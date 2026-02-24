/*
    Fact Table: fact_daily_generation
    
    Core fact table of the warehouse. One row per day, containing
    all generation, import/export, and demand metrics.
    
    Grain: One row per calendar day
    
    Joins:
    - stg_nea_daily (metrics)
    - dim_date (date attributes)
    
    Measures:
    - Generation (NEA, IPP, Total) in MWh
    - Cross-border (Import, Export, Net Exchange) in MWh
    - Demand (Peak MW, Energy Requirement MWh)
    - Loss (System Loss MWh, %)
    - Derived (Surplus/Deficit, Import Dependency)
*/

WITH staging AS (
    SELECT * FROM {{ ref('stg_nea_daily') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    -- ─── Keys ────────────────────────────────────────
    s.report_date_ad,
    s.report_date_bs,
    
    -- ─── Date Attributes (from dimension) ────────────
    d.bs_year,
    d.bs_month,
    d.bs_month_name,
    d.season,
    d.season_label,
    d.fiscal_year,
    d.is_weekend,
    d.day_name,
    d.year_ad,
    d.quarter_ad,
    
    -- ─── Generation Metrics (MWh) ────────────────────
    s.total_generation_mwh,
    s.nea_generation_mwh,
    s.ipp_generation_mwh,
    
    -- Generation share percentages
    CASE 
        WHEN s.total_generation_mwh > 0 
        THEN ROUND((s.nea_generation_mwh / s.total_generation_mwh) * 100, 2)
        ELSE 0 
    END AS nea_generation_pct,
    
    CASE 
        WHEN s.total_generation_mwh > 0 
        THEN ROUND((s.ipp_generation_mwh / s.total_generation_mwh) * 100, 2)
        ELSE 0 
    END AS ipp_generation_pct,
    
    -- ─── Cross-Border Exchange (MWh) ─────────────────
    s.total_import_mwh,
    s.india_import_mwh,
    s.total_export_mwh,
    s.india_export_mwh,
    
    -- Net trade (positive = net importer)
    s.total_import_mwh - s.total_export_mwh AS net_import_mwh,
    s.is_net_exporter,
    
    -- ─── Demand & Supply ─────────────────────────────
    s.peak_demand_mw,
    s.energy_demand_mwh,
    s.net_energy_met_mwh,
    
    -- ─── System Performance ──────────────────────────
    s.system_loss_mwh,
    s.system_loss_pct,
    s.import_dependency_pct,
    s.surplus_deficit_mwh,
    
    -- Total energy available = generation + import
    s.total_generation_mwh + s.total_import_mwh AS total_energy_available_mwh,
    
    -- ─── Period-over-Period Placeholders ──────────────
    -- (These would be filled via window functions for dashboards)
    
    -- ─── Metadata ────────────────────────────────────
    s.source_file,
    s.extracted_at

FROM staging s
LEFT JOIN dates d ON s.report_date_ad = d.date_key
WHERE s.report_date_ad IS NOT NULL
ORDER BY s.report_date_ad
