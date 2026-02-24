/*
    Custom Test: Energy Balance Check
    
    Validates that the energy numbers are internally consistent:
    
      (total_generation + total_import) - (total_export + system_loss) ≈ net_energy_met
    
    We allow a tolerance of ±50 MWh because:
    - PDF values are often rounded
    - Some energy categories may not be fully captured
    - System loss calculation is approximate
    
    This test PASSES when it returns 0 rows (no violations).
    Rows returned = days where the balance is off by more than tolerance.
*/

WITH balance_check AS (
    SELECT
        report_date_ad,
        report_date_bs,
        total_generation_mwh,
        total_import_mwh,
        total_export_mwh,
        system_loss_mwh,
        net_energy_met_mwh,
        
        -- Expected net energy
        (total_generation_mwh + total_import_mwh) - (total_export_mwh + system_loss_mwh) 
            AS computed_net_energy,
        
        -- Difference from reported
        ABS(
            net_energy_met_mwh - 
            ((total_generation_mwh + total_import_mwh) - (total_export_mwh + system_loss_mwh))
        ) AS balance_difference_mwh
        
    FROM {{ ref('stg_nea_daily') }}
    WHERE net_energy_met_mwh > 0  -- Only check days with reported values
)

SELECT *
FROM balance_check
WHERE balance_difference_mwh > 50  -- Tolerance: 50 MWh
