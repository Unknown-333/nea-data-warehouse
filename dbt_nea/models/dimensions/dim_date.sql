/*
    Dimension: dim_date
    
    Date spine with both Bikram Sambat and Gregorian dates.
    Enriched with season, fiscal year, and calendar attributes.
    
    Uses the distinct dates from the staging model as the spine
    (we only have dates where reports exist).
*/

WITH date_spine AS (
    SELECT DISTINCT
        report_date_ad              AS date_key,
        report_date_bs,
        bs_year,
        bs_month,
        bs_day,
        year_ad,
        month_ad,
        day_of_week,
        season,
        fiscal_year
    FROM {{ ref('stg_nea_daily') }}
),

enriched AS (
    SELECT
        date_key,
        report_date_bs,
        
        -- Gregorian components
        year_ad,
        month_ad,
        EXTRACT(DAY FROM date_key)::INTEGER     AS day_ad,
        EXTRACT(QUARTER FROM date_key)::INTEGER AS quarter_ad,
        
        -- BS components
        bs_year,
        bs_month,
        bs_day,
        
        -- BS month names
        CASE bs_month
            WHEN 1  THEN 'Baisakh'
            WHEN 2  THEN 'Jestha'
            WHEN 3  THEN 'Ashadh'
            WHEN 4  THEN 'Shrawan'
            WHEN 5  THEN 'Bhadra'
            WHEN 6  THEN 'Ashwin'
            WHEN 7  THEN 'Kartik'
            WHEN 8  THEN 'Mangsir'
            WHEN 9  THEN 'Poush'
            WHEN 10 THEN 'Magh'
            WHEN 11 THEN 'Falgun'
            WHEN 12 THEN 'Chaitra'
        END AS bs_month_name,
        
        -- AD month names
        TO_CHAR(date_key, 'Month') AS ad_month_name,
        TO_CHAR(date_key, 'Day')   AS day_name,
        
        -- Weekend flag (Nepal uses Saturday as weekend)
        CASE 
            WHEN day_of_week = 6 THEN TRUE  -- Saturday
            ELSE FALSE
        END AS is_weekend,
        
        -- Season
        season,
        CASE season
            WHEN 'monsoon'     THEN '🌧️ Monsoon (High Hydro)'
            WHEN 'dry'         THEN '☀️ Dry (Low Generation)'
            WHEN 'pre_monsoon' THEN '🌤️ Pre-Monsoon (Transition)'
        END AS season_label,
        
        -- Fiscal year
        fiscal_year,
        
        -- Day of week (0=Sunday in PostgreSQL)
        day_of_week
        
    FROM date_spine
)

SELECT * FROM enriched
