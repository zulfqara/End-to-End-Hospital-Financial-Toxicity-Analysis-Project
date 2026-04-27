CREATE OR ALTER VIEW gold.dim_diagnosis AS (
SELECT
	DISTINCT
	icd10_code,
	diag_category  AS category
FROM silver.diagnoses
)
