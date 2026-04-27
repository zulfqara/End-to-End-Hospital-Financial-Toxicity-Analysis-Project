CREATE OR ALTER VIEW  gold.bridge_admission_diagnosis AS (
SELECT 
	diag_id AS diagnosis_id,
	diag_desc AS diagnosis,
	admission_id,
	icd10_code	AS diagnosis_code,
	diag_rank AS diagnosis_rank
FROM silver.diagnoses

)