CREATE OR ALTER VIEW gold.fact_admissions AS 

SELECT
	p.profile_key,  
	a.admission_id,
	a.patient_id,    
	a.hospital_id,
	b.bill_id,

    --Convert the raw dates into YYYYMMDD integers
    CAST(CONVERT(VARCHAR(8), a.admit_date, 112) AS INT) AS admit_date_key,
    CAST(CONVERT(VARCHAR(8), a.discharge_date, 112) AS INT) AS discharge_date_key,

	CASE WHEN a.admit_type = 'Emergency' THEN 1 ELSE 0 END AS is_emergency_admit,
	CASE WHEN a.admit_type = 'Elective' THEN 1 ELSE 0 END AS is_elective_admit,
	CASE WHEN a.admit_type = 'OPD' THEN 1 ELSE 0 END AS is_opd_admit,

	CASE WHEN a.discharge_type = 'Recovered' THEN 1 ELSE 0 END AS is_discharge_recovered,
	CASE WHEN a.discharge_type = 'LAMA' THEN 1 ELSE 0 END AS is_discharge_lama,
	CASE WHEN a.discharge_type = 'Referred' THEN 1 ELSE 0 END AS is_discharge_referred,
	CASE WHEN a.discharge_type = 'Expired' THEN 1 ELSE 0 END AS is_discharge_expired,

	a.los_days AS length_of_stay,
	a.num_procedures,
	a.charlson_index,
	a.hba1c,
	a.creatinine,
	a.haemoglobin,
	a.systolic_bp,
	a.readmitted_7d,
	a.readmitted_30d,
	b.total_cost_inr AS total_cost,
	b.govt_subsidy_inr AS government_subsidy, 
	
    CASE WHEN b.govt_subsidy_inr > 0 THEN 1 ELSE 0 END AS is_bpl, 
	CASE WHEN b.total_cost_inr > 500000 THEN 1 ELSE 0 END AS ayushman_cap_exceeded,

	b.total_cost_inr / a.los_days AS cost_per_day,
	b.cost AS out_of_pocket_amount,

	CAST(b.cost / NULLIF(b.total_cost_inr, 0) AS DECIMAL(10,2)) AS out_of_pocket_ratio, 

	CAST(b.cost / NULLIF(a.los_days, 0) AS DECIMAL(10,2)) AS out_of_pocket_per_day, 

	CASE WHEN b.cost / NULLIF(b.total_cost_inr, 0) > 0.5 THEN 1 ELSE 0 END AS high_financial_toxicity 

FROM silver.admissions AS a

LEFT JOIN silver.billing AS b
	ON a.admission_id = b.admission_id

LEFT JOIN gold.dim_admission_profile AS p
    ON a.admit_type = p.admit_type
    AND a.ward_type = p.ward_type
    AND a.discharge_type = p.discharge_type;