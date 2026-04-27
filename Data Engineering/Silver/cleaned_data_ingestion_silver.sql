
--The admission table had no anomalies, missing, NULLs, negative or whitespaces and was already standardized

INSERT INTO silver.admissions (
	admission_id,
	patient_id,
	admit_date,
	discharge_date,
	los_days,
	admit_type,
	ward_type,
	hospital_id,
	discharge_type,
	num_procedures,
	charlson_index,
	hba1c,
	creatinine,
	haemoglobin,
	systolic_bp,
	readmitted_30d,
	readmitted_7d
)

SELECT 
	admission_id,
	patient_id,
	admit_date,
	discharge_date,
	los_days,
	admit_type,
	ward_type,
	hospital_id,
	discharge_type,
	num_procedures,
	charlson_index,
	hba1c,
	creatinine,
	haemoglobin,
	systolic_bp,
	CASE 
		WHEN DATEDIFF(day,discharge_date, LEAD(admit_date) OVER(PARTITION BY patient_id ORDER BY admit_date) ) <=7 THEN 1
		ELSE 0
	END AS readmitted_7d,
	CASE 
		WHEN DATEDIFF(day,discharge_date, LEAD(admit_date) OVER(PARTITION BY patient_id ORDER BY admit_date) ) <=30 THEN 1
		ELSE 0
	END AS readmitted_30d

FROM bronze.admissions

-----------------------------------------------------

INSERT INTO silver.billing (
    bill_id,
    admission_id,
    total_cost_inr,    
    govt_subsidy_inr,
    cost,
    category           
)
SELECT 
    bill_Id,
    admission_id,
    total_cost_inr,
    govt_subsidy_inr,
    
    -- COST CALCULATION
    CASE 
        -- If the first part is a number...
        WHEN TRY_CAST( TRIM( SUBSTRING( cost_category, 1, CHARINDEX(',', cost_category) - 1 ) ) AS FLOAT ) IS NOT NULL
        THEN CAST( TRIM( SUBSTRING( cost_category, 1, CHARINDEX(',', cost_category) - 1 ) ) AS FLOAT)
        
        -- Otherwise, grab the second part
        ELSE CAST( TRIM( SUBSTRING( cost_category, CHARINDEX(',', cost_category) + 1, LEN( cost_category) ) ) AS FLOAT )
    END AS cost,

    -- CATEGORY CALCULATION
    CASE 
        -- If the second part is NOT a number, it must be the category...
        WHEN TRY_CAST( TRIM( SUBSTRING( cost_category, CHARINDEX(',', cost_category) + 1, LEN( cost_category) )) AS FLOAT ) IS NULL
        THEN TRIM( SUBSTRING( cost_category, CHARINDEX(',', cost_category) + 1, LEN( cost_category) ) )
        
        -- Otherwise, the first part is the category
        ELSE TRIM( SUBSTRING( cost_category, 1, CHARINDEX(',', cost_category) - 1 ) )
    END AS category
        
FROM bronze.billing

---------------------------------------------------------------------

--The admission table had no missing, NULLs, negative or whitespaces and was already standardized

INSERT INTO silver.diagnoses (
diag_id ,		
admission_id,
icd10_code,
diag_desc,
diag_rank,
diag_category	
)

SELECT 
	diag_id ,		
	admission_id,
	icd10_code,
	 REPLACE(diag_desc, 'GÇÖ' , '-') AS diag_desc,
	diag_rank,
	diag_category	
FROM bronze.diagnoses

-----------------------------------------------------------------------------------------------------------

INSERT INTO silver.hospitals (
	hospital_id,
	name,
	state,
	tier,
	beds,
	teaching
)

SELECT 
	hospital_id,

	--Discards the second name
	--Replaces '(private)' with 'hospital'
	 REPLACE( REPLACE(name, '/Manipal' , ''), '(Private)', 'Hospitals') AS name, 
	state,

	CASE
		WHEN tier = 'tier1' THEN 'Tier-1'
		WHEN tier = 'tier2' THEN 'Tier-2'
		ELSE 'Tier-3'
	END AS tier,
	
	beds,

	CASE 
		WHEN teaching = 'True' THEN 'Yes'
		ELSE 'No'
	END AS teaching

FROM bronze.hospitals

-------------------------------------------------------------------------------------------

--The patients table had no anomalies, missing, NULLs, negative or whitespaces


WITH patients_data AS (
    SELECT
        -- 2. Added DESC to ensure '1' is the newest visit!
        ROW_NUMBER() OVER(PARTITION BY patient_id  ORDER BY prev_admissions DESC)  AS occurence,
        patient_id,
        age,
        CASE
            WHEN gender = 'M' THEN 'Male' 
            WHEN gender = 'F' THEN 'Female'
            ELSE 'Other'
        END AS gender,
        state,
        CASE
            WHEN bpl_card = 'True' THEN 'Yes'
            ELSE 'No'
        END AS bpl_card,
        insurance_type,
        comorbidity_count,
        prev_admissions
    FROM bronze.patients
)

INSERT INTO silver.patients (
    patient_id,
    age,
    gender,
    state,
    bpl_card,
    insurance_type,
    comorbidity_count,
    prev_admissions
)


SELECT 
    patient_id,
    age,
    gender,
    state,
    bpl_card,
    insurance_type,
    comorbidity_count,
    prev_admissions
FROM patients_data
WHERE occurence = 1;