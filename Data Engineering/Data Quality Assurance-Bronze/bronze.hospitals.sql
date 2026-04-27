WITH median_bed AS (
SELECT DISTINCT
	    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY beds) OVER () AS median_bed
FROM bronze.hospitals
)
, mainDQA AS (
SELECT 
	'bronze.hospitals' AS table_name,
	COUNT(*)		   AS total_rows,

	COUNT(hospital_id)																       AS total_hospitals,
	COUNT(hospital_id) - COUNT(DISTINCT hospital_id) AS dup_hospital_ids,
	SUM( CASE WHEN hospital_id is NULL OR TRIM(hospital_id)  = '' THEN 1 ELSE 0 END )      AS missing_hospital_id,
	SUM( CASE WHEN hospital_id != TRIM(hospital_id) THEN 1 ELSE 0 END)				       AS whitespace_hospital_id,
	
	SUM( CASE WHEN name is NULL OR TRIM(name)  = '' THEN 1 ELSE 0 END )        AS missing_name,
	SUM( CASE WHEN name != TRIM(name) THEN 1 ELSE 0 END)					   AS whitespace_name,

	SUM( CASE WHEN state is NULL OR TRIM(state)  = '' THEN 1 ELSE 0 END )        AS missing_state,
	SUM( CASE WHEN state != TRIM(state) THEN 1 ELSE 0 END)					   AS whitespace_state,

	SUM( CASE WHEN tier is NULL OR TRIM(tier)  = '' THEN 1 ELSE 0 END )        AS missing_tier,
	SUM( CASE WHEN tier != TRIM(tier) THEN 1 ELSE 0 END)					   AS whitespace_tier,

	SUM( CASE WHEN teaching is NULL OR TRIM(teaching)  = '' THEN 1 ELSE 0 END )        AS missing_teaching,
	SUM( CASE WHEN teaching != TRIM(teaching) THEN 1 ELSE 0 END)					   AS whitespace_teaching,
	SUM( CASE WHEN teaching = 'True' THEN 1 ELSE 0  END)								AS teaching_hospitals,
	SUM( CASE WHEN teaching = 'False' THEN 1 ELSE 0 END)								AS non_teacing_hospitals,

		SUM( CASE WHEN beds is NULL OR TRIM(CAST(beds AS VARCHAR)) = '' THEN 1 ELSE 0 END )	AS  missing_beds,
	SUM( CASE WHEN beds < =0 THEN 1 ELSE 0 END )										            AS negative_beds,

	--SUMMARY STATISTICS:
	MIN(beds)																						AS min_beds,
	MAX(beds)																						AS max_beds,
		AVG(beds)																						AS average_beds

FROM bronze.hospitals
)
select 
d.*,
mb.*
FROM mainDQA AS d
CROSS JOIN median_bed AS mb
