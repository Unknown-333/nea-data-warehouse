/*
    Staging Model: stg_nea_daily
    
    Source: raw.daily_grid_report
    Purpose: Clean, type-cast, and enrich the raw NDOR data
    
    This is the first transformation layer. It:
    1. Casts all columns to proper types
    2. Adds computed fields (season, fiscal year, net exporter flag)
    3. Renames to consistent snake_case
    4. Filters out any invalid records
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'daily_grid_report') }}
),

cleaned AS (
    SELECT
        -- ─── Date Fields ─────────────────────────────────
        report_date_ad,
        report_date_bs,
        EXTRACT(YEAR FROM report_date_ad)::INTEGER       AS year_ad,
        EXTRACT(MONTH FROM report_date_ad)::INTEGER      AS month_ad,
        EXTRACT(DOW FROM report_date_ad)::INTEGER        AS day_of_week,  -- 0=Sun
        
        -- BS date components (from the BS date string)
        SPLIT_PART(report_date_bs, '/', 1)::INTEGER      AS bs_year,
        SPLIT_PART(report_date_bs, '/', 2)::INTEGER      AS bs_month,
        SPLIT_PART(report_date_bs, '/', 3)::INTEGER      AS bs_day,
        
        -- ─── Generation (MWh) ────────────────────────────
        -- NULLs are preserved: a NULL means extraction failed,
        -- not zero generation. Use COALESCE only in downstream
        -- aggregations where 0 is a safe fallback.
        total_generation_mwh,
        nea_generation_mwh,
        ipp_generation_mwh,
        
        -- ─── Cross-Border Exchange (MWh) ─────────────────
        total_import_mwh,
        india_import_mwh,
        COALESCE(total_export_mwh, 0)                    AS total_export_mwh,
        COALESCE(india_export_mwh, 0)                    AS india_export_mwh,
        
        -- ─── Demand & Loss ───────────────────────────────
        peak_demand_mw,
        energy_demand_mwh,
        COALESCE(system_loss_mwh, 0)                     AS system_loss_mwh,
        COALESCE(system_loss_pct, 0)                     AS system_loss_pct,
        net_energy_met_mwh,
        
        -- ─── JSONB Detail Fields ─────────────────────────
        exchange_details,
        plant_generation,
        
        -- ─── Computed Fields ─────────────────────────────
        -- Season classification based on BS month
        CASE 
            WHEN SPLIT_PART(report_date_bs, '/', 2)::INTEGER IN (4, 5, 6) THEN 'monsoon'
            WHEN SPLIT_PART(report_date_bs, '/', 2)::INTEGER IN (7, 8, 9, 10, 11) THEN 'dry'
            ELSE 'pre_monsoon'
        END AS season,
        
        -- Fiscal year (Nepal FY starts mid-July / Shrawan)
        CASE 
            WHEN SPLIT_PART(report_date_bs, '/', 2)::INTEGER >= 4 
            THEN SPLIT_PART(report_date_bs, '/', 1)::INTEGER || '/' || 
                 LPAD(((SPLIT_PART(report_date_bs, '/', 1)::INTEGER + 1) % 100)::TEXT, 2, '0')
            ELSE (SPLIT_PART(report_date_bs, '/', 1)::INTEGER - 1) || '/' || 
                 LPAD((SPLIT_PART(report_date_bs, '/', 1)::INTEGER % 100)::TEXT, 2, '0')
        END AS fiscal_year,
        
        -- Net exporter flag
        CASE 
            WHEN COALESCE(total_export_mwh, 0) > COALESCE(total_import_mwh, 0) THEN TRUE
            ELSE FALSE
        END AS is_net_exporter,
        
        -- Import dependency ratio
        CASE 
            WHEN COALESCE(total_generation_mwh, 0) + COALESCE(total_import_mwh, 0) > 0
            THEN ROUND(
                (COALESCE(total_import_mwh, 0)::NUMERIC / 
                (COALESCE(total_generation_mwh, 0) + COALESCE(total_import_mwh, 0))) * 100, 
                2
            )
            ELSE 0
        END AS import_dependency_pct,
        
        -- Surplus/deficit (generation vs requirement)
        total_generation_mwh - energy_demand_mwh 
            AS surplus_deficit_mwh,
        
        -- Metadata
        source_file,
        extracted_at
        
    FROM source
    WHERE report_date_ad IS NOT NULL
)

SELECT * FROM cleaned
