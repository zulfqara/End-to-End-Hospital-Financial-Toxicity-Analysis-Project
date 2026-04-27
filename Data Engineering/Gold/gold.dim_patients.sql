CREATE OR ALTER VIEW gold.dim_patients AS 

-- ==============================================================================
-- CTE: Anchor to the Fact table to handle "Late Arriving Dimensions".
-- We start with admissions to guarantee every patient who visited the hospital 
-- gets a Dimension row, preventing missing data drops in Power BI.
-- ==============================================================================
WITH patients AS (
    SELECT
        a.patient_id,
        p.age,
        p.gender,
        p.state AS patient_state,
        p.bpl_card
    FROM silver.admissions AS a
    
    -- LEFT JOIN ensures the patient_id survives even if demographics are missing
    LEFT JOIN silver.patients AS p
        ON a.patient_id = p.patient_id
) 

-- ==============================================================================
-- Final Output: Deduplicate the inferred/matched records to strictly enforce 
-- the 1-to-Many (1:*) relationship rule required by the Power BI semantic model.
-- ==============================================================================
SELECT DISTINCT
    patient_id,
    age,
    gender,
    patient_state,
    bpl_card
FROM patients;