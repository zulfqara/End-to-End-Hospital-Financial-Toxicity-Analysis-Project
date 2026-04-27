
CREATE TABLE bronze.admissions (
admission_id VARCHAR(50),
patient_id   VARCHAR(50),
admit_date	 DATE,
discharge_date DATE,
los_days	INT,
admit_type	VARCHAR(50),
ward_type	VARCHAR(50),
hospital_id	VARCHAR(50),
discharge_type	VARCHAR(50),
num_procedures	INT,
charlson_index	INT,
hba1c			 FLOAT,
creatinine		FLOAT,
haemoglobin		FLOAT,
systolic_bp		INT,
readmitted_30d	INT,
readmitted_7d	INT

)

GO

CREATE TABLE bronze.billing(
bill_id			  VARCHAR(50),	
admission_id	  VARCHAR(50),
total_cost_inr	  INT,
govt_subsidy_inr  INT,
cost_category	  VARCHAR(50),
)
 

CREATE  TABLE bronze.diagnoses (
diag_id VARCHAR(50),		
admission_id VARCHAR(50),
icd10_code	 VARCHAR(50),
diag_desc    VARCHAR(50),	
diag_rank	FLOAT,
diag_category VARCHAR(50),	

)

GO

CREATE TABLE bronze.hospitals (

hospital_id VARCHAR(50),
name VARCHAR(50),
state VARCHAR(50),
tier  VARCHAR(50),
beds	 INT,
teaching VARCHAR(50),
)

GO

CREATE TABLE bronze.patients (
patient_id VARCHAR(50),
age			INT,
gender	VARCHAR(50),
state	VARCHAR(50),
bpl_card	VARCHAR(50),
insurance_type	VARCHAR(50),
comorbidity_count	INT,
prev_admissions		INT,

)