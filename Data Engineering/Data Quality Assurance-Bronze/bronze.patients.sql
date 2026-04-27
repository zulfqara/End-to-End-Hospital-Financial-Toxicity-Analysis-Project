WITH median_age AS (
SELECT DISTINCT 
	PERCENTILE_CONT(0.5) WITHIN GROUP( ORDER BY age ) OVER() AS median_age,
	PERCENTILE_CONT(0.5) WITHIN GROUP( ORDER BY comorbidity_count ) OVER() AS median_comorbidity_count,
	PERCENTILE_CONT(0.5) WITHIN GROUP( ORDER BY prev_admissions ) OVER() AS median_prev_admissions

FROM bronze.patients
)
, mainDQA AS (
SELECT 
	'bronze.patients' AS table_name,
	COUNT(*)		  AS total_rows,

	COUNT(patient_id) - COUNT(DISTINCT patient_id) AS dup_patient_ids,
	SUM( CASE WHEN patient_id IS NULL OR TRIM(patient_id) = '' THEN 1 ELSE 0 END ) AS missing_patient_ids,
	SUM( CASE WHEN patient_id != TRIM(patient_id) THEN 1 ELSE 0 END) AS whitespace_patient_id,

	SUM( CASE WHEN age is NULL OR TRIM(CAST(age AS VARCHAR)) = '' THEN 1 ELSE 0 END )	AS  missing_age,
	SUM( CASE WHEN age < 0 THEN 1 ELSE 0 END )										    AS negative_age,
	
	--SUMMARY STATISTICS (AGE):
	MIN(age)																			AS min_age,
	AVG(age)																		    AS average_age,
	MAX(age)																			AS max_age,

	SUM( CASE WHEN gender IS NULL OR TRIM(gender) = '' THEN 1 ELSE 0 END ) AS missing_gender,
	SUM( CASE WHEN gender != TRIM(gender) THEN 1 ELSE 0 END) AS whitespace_gender,

	SUM( CASE WHEN state IS NULL OR TRIM(state) = '' THEN 1 ELSE 0 END ) AS missing_state,
	SUM( CASE WHEN state != TRIM(state) THEN 1 ELSE 0 END) AS whitespace_state,

	SUM( CASE WHEN bpl_card IS NULL OR TRIM(bpl_card) = '' THEN 1 ELSE 0 END ) AS missing_bpl_card,
	SUM( CASE WHEN bpl_card != TRIM(bpl_card) THEN 1 ELSE 0 END) AS whitespace_bpl_card,

	SUM( CASE WHEN insurance_type IS NULL OR TRIM(insurance_type) = '' THEN 1 ELSE 0 END ) AS missing_insurance_type,
	SUM( CASE WHEN insurance_type != TRIM(insurance_type) THEN 1 ELSE 0 END) AS whitespace_insurance_type,

	SUM( CASE WHEN comorbidity_count is NULL OR TRIM(CAST(comorbidity_count AS VARCHAR)) = '' THEN 1 ELSE 0 END )	AS  missing_comorbidity_count,
	SUM( CASE WHEN comorbidity_count < 0 THEN 1 ELSE 0 END )										            AS negative_comorbidity_count,

		--SUMMARY STATISTICS (COMBORDITY):
	MIN(comorbidity_count)																			AS min_comorbidity_count,
	AVG(comorbidity_count)																		    AS average_comorbidity_count,
	MAX(comorbidity_count)																			AS max_combordity_count,

	SUM( CASE WHEN prev_admissions is NULL OR TRIM(CAST(prev_admissions AS VARCHAR)) = '' THEN 1 ELSE 0 END )	AS  missing_prev_admissions,
	SUM( CASE WHEN prev_admissions < 0 THEN 1 ELSE 0 END )										            AS negative_prev_admissions,

	--SUMMARY STATISTICS (Prev Admissions):
	MIN(prev_admissions)																			AS min_prev_admissions,
	AVG(prev_admissions)																		    AS average_prev_admissions,
	MAX(prev_admissions)																			AS max_prev_admissions

FROM bronze.patients
)

SELECT 
d.*,
ma.*
FROM mainDQA d
CROSS JOIN median_age as ma

