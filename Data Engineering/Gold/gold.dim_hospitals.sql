CREATE OR ALTER VIEW gold.dim_hospitals AS (
SELECT
	ROW_NUMBER() OVER( ORDER BY hospital_id ) AS hospital_key,
	hospital_id,
	name		AS hospital_name,
	state,
	tier,
	CASE
		WHEN tier = 'Tier-1' THEN 'Private'
		WHEN tier = 'Tier-2' THEN 'Government'
		ELSE 'District'
	END AS hospital_type,
	beds,
	teaching AS medical_college_affiliation

FROM silver.hospitals
)