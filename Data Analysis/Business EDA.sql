--TOTAL PATIENTS WITH HIGH CHARLSON SCORE:
	
SELECT 
	
	SUM(CASE
		WHEN patient_complexity_tier = 'High Complexity' THEN 1
		ELSE 0
	END )total_high_complexity

FROM (

SELECT 

CASE 
    WHEN charlson_index = 0 THEN ' Baseline'
    WHEN charlson_index IN (1, 2) THEN 'Low Complexity'
    WHEN charlson_index IN (3, 4) THEN 'Moderate Complexity'
    WHEN charlson_index >= 5 THEN 'High Complexity'
    ELSE 'Unknown'
END AS patient_complexity_tier

FROM gold.fact_admissions


)T

--RESULT : 8432

--------------------------------------------------------------


-- How many patients are Below Poverty Line?
SELECT 
	COUNT(admission_id)	AS total_bpl_paitents
FROM gold.fact_admissions
WHERE is_bpl = 1

--RESULT: 86013

------------------------------------------------------------------------
--How many come back within 30 days?

SELECT 
	COUNT(admission_id)
FROM gold.fact_admissions
where readmitted_30d = 1

--RESULT: 623

--------------------------------------------------------------

--How much do BPL patients pay extra when cap is crossed?

SELECT 
	CAST( AVG(out_of_pocket_amount) AS DECIMAL(10,2) ) AS avg_oop,
	CAST( MAX(out_of_pocket_amount) AS DECIMAL(10,2) ) AS max_oop,

	(SELECT CAST( MIN(out_of_pocket_amount) AS DECIMAL(10,2) )
	FROM gold.fact_admissions 
	WHERE out_of_pocket_amount != 0 ) 	AS min_oop


FROM gold.fact_admissions
where  is_bpl = 1
---------------------------------

--ADMISSION DISTRIBUTION

SELECT 
	SUM( CASE WHEN is_emergency_admit = 1 THEN 1 ELSE 0 END ) AS total_emergency_admit,
	SUM( CASE WHEN is_elective_admit = 1 THEN 1 ELSE 0 END ) AS total_elective_admit,
	SUM( CASE WHEN is_opd_admit = 1 THEN 1 ELSE 0 END ) AS total_opd_admit
	
FROM gold.fact_admissions

-------------------------------------------------------------------------------------------------------------

--DISCHARGE DISTRIBUTION:

SELECT 

	COUNT( admission_id) AS total_admissions,
	SUM( CASE WHEN is_discharge_recovered = 1 THEN 1 ELSE 0 END ) AS total_recovered,
	SUM( CASE WHEN is_discharge_lama	  = 1 THEN 1 ELSE 0 END ) AS total_lama,
	SUM( CASE WHEN is_discharge_expired	  = 1 THEN 1 ELSE 0 END ) AS total_expired,
	SUM( CASE WHEN is_discharge_referred  = 1 THEN 1 ELSE 0 END ) AS total_referred

FROM gold.fact_admissions



---------------------------------------------------------------------------------------------------


-----------------------------------------

--Which group has the highest combined risk

--Total Patients
SELECT
	COUNT(admission_id)
FROM gold.fact_admissions
WHERE is_bpl = 1 AND ayushman_cap_exceeded = 1 AND charlson_index >= 5

--average length of stay in the highest-risk group
SELECT 
	AVG(length_of_stay) average_los
FROM gold.fact_admissions
WHERE is_bpl = 1 AND ayushman_cap_exceeded = 1 AND charlson_index >= 5

--Readmission rate of high risk patients:

select 
	COUNT(admission_id) AS total
from gold.fact_admissions
 WHERE readmitted_30d = 1 AND ayushman_cap_exceeded = 1 AND charlson_index >= 5 AND is_bpl=1

-- TOTAL PAID BY PATIENTS FROM THEIR OWN POCKET:

SELECT 
	SUM(out_of_pocket_amount) AS total_paid,
	CAST( AVG(out_of_pocket_amount) AS DECIMAL(10,2)  ) AS avg_oop,
	MIN(out_of_pocket_amount) AS min_oop,
	MAX(out_of_pocket_amount) AS max_oop
FROM gold.fact_admissions
WHERE is_bpl = 1 AND ayushman_cap_exceeded = 1 AND charlson_index >= 5

--What are the top 5 diagnoses in this group?

WITH diagnosis AS (
SELECT 
	b.diagnosis_rank,
	b.diagnosis,
	f.is_discharge_recovered,
	f.is_discharge_expired,
	f.is_discharge_lama,
	f.is_discharge_referred
FROM gold.fact_admissions as f
LEFT JOIN gold.bridge_admission_diagnosis b
	ON f.admission_id = b.admission_id
WHERE f.is_bpl = 1 AND f.ayushman_cap_exceeded = 1 AND f.charlson_index >= 5
)
, total  AS  (
SELECT 
	 diagnosis,
	is_discharge_recovered,
	is_discharge_expired,
	is_discharge_lama,
	is_discharge_referred,
	COUNT(diagnosis) AS total
FROM diagnosis
WHERE diagnosis_rank =1
GROUP BY diagnosis,  is_discharge_recovered,
	is_discharge_expired,
	is_discharge_lama,
	is_discharge_referred
)

SELECT TOP 5
	diagnosis,
	is_discharge_recovered,
	is_discharge_expired,
	is_discharge_lama,
	is_discharge_referred,
	total
FROM total
order by total desc

select*
from gold.fact_admissions
