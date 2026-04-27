WITH primary_diag_rank_check AS (

--Checks for duplicate Principal Diagnosis diagnosis rank "1"
SELECT 
	COUNT(admission_id) - COUNT( DISTINCT admission_id ) dup_principal_diag_rank
FROM ( 
SELECT 
	admission_id,
	diag_rank
FROM bronze.diagnoses
where diag_rank = 1
)t

)
, General_DQA AS (

SELECT 
	'bronze.diagnoses' AS table_name,
	COUNT(*)           AS total_rows,

	--Primary Key (diag_id):
	COUNT(diag_id) - COUNT(DISTINCT diag_id) AS dup_diag_id,
	SUM( CASE WHEN diag_id IS NULL OR TRIM(diag_id) = '' THEN 1 ELSE 0 END ) AS missing_diag_id,
	SUM( CASE WHEN diag_id != TRIM(diag_id) THEN 1 ELSE 0 END) AS whitespace_diag_id,
	
	--Foreign Key (admission_id):
	COUNT(admission_id) - COUNT(DISTINCT admission_id) AS dup_admission_ids,
	SUM( CASE WHEN admission_id IS NULL OR TRIM(admission_id) = '' THEN 1 ELSE 0 END ) AS missing_admission_ids,
	SUM( CASE WHEN admission_id != TRIM(admission_id) THEN 1 ELSE 0 END) AS whitespace_admission_id,

	--ICD10 Code:
	SUM( CASE WHEN icd10_code is NULL OR TRIM(icd10_code) = '' THEN 1 ELSE 0 END )	AS  missing_icd10_code,
	SUM( CASE WHEN icd10_code != TRIM(icd10_code) THEN 1 ELSE 0 END)					       AS whitespace_icd10_code,
	SUM( CASE WHEN LEN(TRIM(icd10_code)) < 3 OR LEN(TRIM(icd10_code)) > 7 THEN 1 ELSE 0 END ) AS invalid_length_icd10_code,
	SUM( CASE WHEN TRY_CAST( SUBSTRING(icd10_code, 1, 1) AS VARCHAR) IS NULL THEN 1 ELSE 0 END ) AS invalid_icd10_code, 

	--Diagnosis Description (diag_desc):
	SUM( CASE WHEN diag_desc is NULL OR TRIM(diag_desc) = '' THEN 1 ELSE 0 END )	AS  missing_diag_desc,
	SUM( CASE WHEN diag_desc != TRIM(diag_desc) THEN 1 ELSE 0 END)					       AS whitespace_diag_desc,

	--Diagnosis Rank (diag_rank):
	SUM( CASE WHEN diag_rank is NULL OR TRIM(CAST(diag_rank AS VARCHAR)) = '' THEN 1 ELSE 0 END )	AS  missing_diag_rank,
	SUM( CASE WHEN diag_rank <= 0 THEN 1 ELSE 0 END )								                AS negative_diag_rank,

	--Diagnosis Categor (diag_categor):
	SUM( CASE WHEN diag_category is NULL OR TRIM(diag_category) = '' THEN 1 ELSE 0 END )	AS  missing_diag_category,
	SUM( CASE WHEN diag_category != TRIM(diag_category) THEN 1 ELSE 0 END)					       AS whitespace_diag_category

FROM bronze.diagnoses
)

SELECT 
	g.table_name,
	g.total_rows,

	g.dup_diag_id,
	g.missing_diag_id,
	g.whitespace_diag_id,

	g.dup_admission_ids,
	g.missing_admission_ids,
	g.whitespace_admission_id,

	g.missing_icd10_code,
	g.whitespace_icd10_code,
	g.invalid_length_icd10_code,
	g.invalid_icd10_code,

	g.missing_diag_desc,
	g.whitespace_diag_desc,
	
	g.missing_diag_rank,
	g.negative_diag_rank,
	d.dup_principal_diag_rank,
	g.missing_diag_category,
	g.whitespace_diag_category

FROM General_DQA AS g
CROSS JOIN primary_diag_rank_check AS d



SELECT admission_id
from (

 select admission_id,
 count(diag_rank) as diag_count
 From bronze.diagnoses
 where diag_rank =1
 group by admission_id
 )t
 where diag_count >1

 select admission_id,
 count(diag_rank) as county
 from bronze.diagnoses
 where diag_rank = 1
group by admission_id
having  count(diag_rank)  > 1