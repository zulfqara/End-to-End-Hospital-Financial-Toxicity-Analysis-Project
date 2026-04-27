CREATE OR ALTER VIEW gold.dim_admission_profile AS 

WITH unique_profiles AS (
	--Extracting only the unique combos
SELECT DISTINCT
	a.admit_type,
	a.ward_type,
	a.discharge_type
FROM silver.admissions as a
LEFT JOIN silver.billing as b
ON a.admission_id = b.admission_id
)
SELECT 
	--Assign the numbers to that tiny list
	ROW_NUMBER() OVER(ORDER BY admit_type, ward_type, discharge_type)  AS profile_key,
	admit_type,
	ward_type,
	discharge_type
FROM unique_profiles




