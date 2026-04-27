WITH AllMedians AS (
    SELECT DISTINCT 
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY los_days) OVER () AS median_los_days,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY num_procedures) OVER () AS median_num_procedures,
         PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY charlson_index) OVER () AS  median_charlson_index,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hba1c) OVER () AS median_hba1c,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY creatinine) OVER () AS median_creatinine,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY haemoglobin) OVER () AS median_haemoglobin,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY systolic_bp) OVER () AS median_systolic_bp
    FROM bronze.admissions
)
, MainDQA AS (
SELECT 
	'bronze.admissions' AS table_name,
	COUNT(*)			AS total_rows,

	COUNT(admission_id) - COUNT(DISTINCT admission_id) AS dup_admission_ids,
	SUM( CASE WHEN admission_id IS NULL OR TRIM(admission_id) = '' THEN 1 ELSE 0 END ) AS missing_admission_ids,
	SUM( CASE WHEN admission_id != TRIM(admission_id) THEN 1 ELSE 0 END) AS whitespace_admission_id,

	COUNT(patient_id) - COUNT(DISTINCT patient_id) AS dup_patient_ids,
	SUM( CASE WHEN patient_id IS NULL OR TRIM(patient_id) = '' THEN 1 ELSE 0 END ) AS missing_patient_ids,
	SUM( CASE WHEN patient_id != TRIM(patient_id) THEN 1 ELSE 0 END) AS whitespace_patient_id,

	SUM( CASE WHEN admit_date > discharge_date THEN 1 ELSE 0 END )									AS logical_error_admit_date,
	SUM( CASE WHEN admit_date IS NULL OR TRIM(CAST( admit_date AS VARCHAR)) = '' THEN 1 ELSE 0 END ) AS missing_admit_date,
	SUM( CASE WHEN admit_date < '2015-1-1' THEN 1 ELSE 0 END)											AS outliers_admit_date,

	SUM( CASE WHEN discharge_date < admit_date THEN 1 ELSE 0 END )										AS logical_error_discharge_date,
	SUM( CASE WHEN discharge_date IS NULL OR TRIM(CAST(discharge_date AS VARCHAR)) = '' THEN 1 ELSE 0 END ) AS missing_discharge_date,
	SUM( CASE WHEN discharge_date > '2024-12-31' THEN 1 ELSE 0 END)											AS outliers_discharge_date,

	--Check if LOS matches the actual dates
	SUM( CASE WHEN los_days != DATEDIFF(day, admit_date, discharge_date) THEN 1 ELSE 0 END ) AS mismatched_los_calc,

	SUM( CASE WHEN los_days is NULL OR TRIM(CAST(los_days AS VARCHAR)) = '' THEN 1 ELSE 0 END )			   AS  missing_los_days,
	SUM( CASE WHEN los_days <0 THEN 1 ELSE 0 END )										   AS negative_los_days,
	
	--SUMMARY STATISTICS (LOS)
	MIN( los_days)																			AS minimum_los_days,
	MAX( los_days)																			 AS maximum_los_days,
	AVG( los_days)																			AS average_los_days,


	SUM( CASE WHEN admit_type is NULL OR TRIM(admit_type)  = '' THEN 1 ELSE 0 END )        AS missing_admit_type,
	SUM( CASE WHEN admit_type != TRIM(admit_type) THEN 1 ELSE 0 END)					   AS whitespace_admit_type,

	SUM( CASE WHEN ward_type is NULL OR TRIM(ward_type)  = '' THEN 1 ELSE 0 END )          AS missing_ward_type,
	SUM( CASE WHEN ward_type != TRIM(ward_type) THEN 1 ELSE 0 END)					       AS whitespace_ward_type,

	SUM( CASE WHEN hospital_id is NULL OR TRIM(hospital_id)  = '' THEN 1 ELSE 0 END )      AS missing_hospital_id,
	SUM( CASE WHEN hospital_id != TRIM(hospital_id) THEN 1 ELSE 0 END)				       AS whitespace_hospital_id,
	COUNT(hospital_id) - COUNT(DISTINCT hospital_id) AS dup_hospital_ids,

	SUM( CASE WHEN discharge_type is NULL OR TRIM(discharge_type)  = '' THEN 1 ELSE 0 END ) AS missing_discharge_type,
	SUM( CASE WHEN discharge_type != TRIM(discharge_type) THEN 1 ELSE 0 END)				AS whitespace_discharge_type,

	SUM( CASE WHEN num_procedures is NULL OR TRIM(CAST(num_procedures AS VARCHAR)) = '' THEN 1 ELSE 0 END )	AS  missing_num_procedures,
	SUM( CASE WHEN num_procedures <0 THEN 1 ELSE 0 END )										           AS negative_num_procedures,

	--SUMMARY STATISTICS (num_procedures)
	MIN( num_procedures)																			AS min_num_procedures,
	MAX( num_procedures)																			 AS max_num_procedures,
	AVG( num_procedures)																			AS avg_num_procedures,



	SUM( CASE WHEN charlson_index is NULL OR TRIM(CAST(charlson_index AS VARCHAR)) = '' THEN 1 ELSE 0 END )	AS  missing_charlson_index,
	SUM( CASE WHEN charlson_index < 0 THEN 1 ELSE 0 END )								                   AS negative_charlson_index,

	MIN( charlson_index)																			AS min_charlson_index,
	MAX( charlson_index)																			 AS max_charlson_index,
	AVG( charlson_index)																	AS avg_charlson_index,

	SUM( CASE WHEN hba1c IS NULL OR TRIM(CAST(hba1c AS VARCHAR)) = '' THEN 1 ELSE 0 END ) AS missing_hba1c,
	SUM( CASE WHEN hba1c <= 0 THEN 1 ELSE 0 END ) AS negative_hba1c,

	MIN( hba1c)																			AS min_hba1c,
	MAX( hba1c)																			 AS max_hba1c,
	CAST( AVG( hba1c)	AS DECIMAL(10,2))																		AS avg_hba1c,

	SUM( CASE WHEN creatinine  is NULL OR TRIM(CAST(creatinine AS VARCHAR)) = '' THEN 1 ELSE 0 END )		AS  missing_creatinine,
	SUM( CASE WHEN creatinine <= 0 THEN 1 ELSE 0 END )									                   AS negative_creatinine,

	MIN( creatinine)																			AS min_creatinine,
	MAX( creatinine)																			 AS max_creatinine,
	CAST( AVG( creatinine) AS DECIMAL(10,2))																		AS avg_creatinine,

	SUM( CASE WHEN haemoglobin  is NULL OR TRIM(CAST(haemoglobin AS VARCHAR)) = '' THEN 1 ELSE 0 END )		AS  missing_haemoglobin,
	SUM( CASE WHEN haemoglobin <=0 THEN 1 ELSE 0 END )									                   AS negative_haemoglobin,

	MIN( haemoglobin)																			AS min_haemoglobin,
	MAX( haemoglobin)																			 AS max_haemoglobin,
	CAST(AVG( haemoglobin) AS DECIMAL(10,2)	)																		AS avg_haemoglobin,


	SUM( CASE WHEN systolic_bp is NULL OR TRIM(CAST(systolic_bp AS VARCHAR)) = '' THEN 1 ELSE 0 END )		AS  missing_systolic_bp,
	SUM( CASE WHEN systolic_bp <= 0 THEN 1 ELSE 0 END )								                       AS negative_systolic_bp,

	MIN( systolic_bp)																			AS min_systolic_bp,
	MAX( systolic_bp)																			 AS max_systolic_bp,
	CAST( AVG(systolic_bp) AS DECIMAL(10,2))																AS avg_systolic_bp,


	SUM( CASE WHEN readmitted_30d is NULL OR TRIM(CAST(readmitted_30d AS VARCHAR)) = '' THEN 1 ELSE 0 END )	AS  missing_readmitted_30d,
	SUM( CASE WHEN readmitted_30d < 0 THEN 1 ELSE 0 END )								                   AS negative_readmitted_30d,
	SUM( CASE WHEN readmitted_30d = 1 THEN 1 ELSE 0 END )                                                  AS [total_readmitted_30d_"1"],
	SUM( CASE WHEN readmitted_30d = 0 THEN 1 ELSE 0 END )                                                  AS [total_readmitted_30d_"0"] ,		

	SUM( CASE WHEN readmitted_7d is NULL OR TRIM(CAST(readmitted_7d AS VARCHAR)) = '' THEN 1 ELSE 0 END )	AS  missing_readmitted_7d,
	SUM( CASE WHEN readmitted_7d <0 THEN 1 ELSE 0 END )										           AS negative_readmitted_7d,
	SUM( CASE WHEN readmitted_7d = 1 AND readmitted_30d = 0 THEN 1 ELSE 0 END ) AS logical_error_readmissions,

	SUM( CASE WHEN readmitted_7d = 1 THEN 1 ELSE 0 END )                                                  AS [total_readmitted_7d_"1"],
	SUM( CASE WHEN readmitted_7d = 0 THEN 1 ELSE 0 END )                                                  AS [total_readmitted_7d_"0"] 		

FROM bronze.admissions
)

SELECT 
	d.table_name,
    d.total_rows,
    d.dup_admission_ids,
    d.missing_admission_ids,
    d.whitespace_admission_id,
    d.dup_patient_ids,
    d.missing_patient_ids,
    d.whitespace_patient_id,
    d.logical_error_admit_date,
    d.missing_admit_date,
	d.outliers_admit_date,
	d.outliers_discharge_date,
    d.logical_error_discharge_date,
    d.missing_discharge_date,
    d.mismatched_los_calc,
    d.missing_los_days,
    d.negative_los_days,
    d.minimum_los_days,
    d.maximum_los_days,
    d.average_los_days,
    m.median_los_days,
    d.missing_admit_type,
    d.whitespace_admit_type,
    d.missing_ward_type,
    d.whitespace_ward_type,
    d.missing_hospital_id,
    d.whitespace_hospital_id,
    d.dup_hospital_ids,
    d.missing_discharge_type,
    d.whitespace_discharge_type,
    d.missing_num_procedures,
    d.negative_num_procedures,
    d.min_num_procedures,
    d.max_num_procedures,
    d.avg_num_procedures,
    m.median_num_procedures,
    d.missing_charlson_index,
    d.negative_charlson_index,
    d.min_charlson_index,
    d.max_charlson_index,
    d.avg_charlson_index,
    m.median_charlson_index,
    d.missing_hba1c,
    d.negative_hba1c,
    d.min_hba1c,
    d.max_hba1c,
    d.avg_hba1c,
    m.median_hba1c,
    d.missing_creatinine,
    d.negative_creatinine,
    d.min_creatinine,
    d.max_creatinine,
    d.avg_creatinine,
    m.median_creatinine,
    d.missing_haemoglobin,
    d.negative_haemoglobin,
    d.min_haemoglobin,
    d.max_haemoglobin,
    d.avg_haemoglobin,
    m.median_haemoglobin,
    d.missing_systolic_bp,
    d.negative_systolic_bp,
    d.min_systolic_bp,
    d.max_systolic_bp,
    d.avg_systolic_bp,
    m.median_systolic_bp,
    d.missing_readmitted_30d,
    d.negative_readmitted_30d,
    d.[total_readmitted_30d_"1"],
    d.[total_readmitted_30d_"0"],
    d.missing_readmitted_7d,
    d.negative_readmitted_7d,
    d.logical_error_readmissions,
    d.[total_readmitted_7d_"1"],
    d.[total_readmitted_7d_"0"]
FROM mainDQA as d
CROSS JOIN AllMedians AS m

--Exploring the last month's admission and their readmissions:

--Checking if there are some patients that were admitted in the last days of the dataset.
--To ensure the discharge date has no outliers
select *
from(
SELECT
    -- 1. Correct visit_number: 1 = FIRST admission of the patient
    ROW_NUMBER() OVER(PARTITION BY patient_id ORDER BY admit_date ASC) AS visit_number,
    
    patient_id,
    admission_id,
    admit_date,
    discharge_date,
    discharge_type,
    -- 2. Correct previous_discharge_date (chronological history)
    LAG(discharge_date) OVER(PARTITION BY patient_id ORDER BY admit_date ASC) AS previous_discharge_date,
    
    los_days,
    readmitted_7d,
    readmitted_30d

FROM silver.ADMISSIONS



)t
where previous_discharge_date is null
-- 3. Sort the result so you can easily verify per patient
ORDER BY 
    patient_id, 
    admit_date ASC  -- ← Changed to ASC