# End-to-End Hospital Financial Toxicity Analysis

End-to-end Medallion pipeline (T-SQL) and diagnostic Power BI dashboard 
identifying financial toxicity drivers and LAMA risk among low-income 
patients facing government insurance cap breaches.

---

## Problem Statement

The initial stakeholder request focused on high 30-day readmission rates 
among patients with complex comorbidities. After performing exploratory 
analysis on 120,000+ admissions, the overall readmission rate was found 
to be just 0.52% — and 0% in the highest-risk cohort, making readmission 
prevention a statistically weak focus area.

The dominant pattern identified was severe financial toxicity: a 
concentrated group of 0.27% of admissions frequently breached the 
Ayushman cap, generating ₹12.39 crore in calculated out-of-pocket burden. 
The problem statement was accordingly refined to focus on identifying 
high-cost, high-risk patient cohorts — ensuring the analysis remained 
grounded in what the data actually revealed.

---

## Technology Stack

| Layer | Tools & Methods |
|---|---|
| **Source Data** | [India Hospital Readmission & Discharge Dataset](https://www.kaggle.com/datasets/digutlaranjithkumar/india-hospital-readmission-dataset-20152024)
| **Database & Language** | SQL Server Express, T-SQL, SSMS |
| **Data Engineering** | Medallion Architecture, Data Quality Assurance (DQA) |
| **Advanced SQL** | CTEs, Window Functions (ROW_NUMBER(), LAG(), SUM() OVER()), String Functions, CASE Statements |
| **Business Intelligence** | Power BI — DAX, Power Query, Custom Visualizations |

---

## Pipeline Architecture

Built a Bronze-Silver-Gold pipeline to ingest, cleanse, and model 
120,000+ raw admission records. Explicit binary flags were engineered 
for cap breaches and admission types to support future predictive 
machine learning models.


---


## Data Model:

<img width="815" height="606" alt="image" src="https://github.com/user-attachments/assets/fda3120d-101a-4576-89fa-71f3c01d0615" />

---

## Dashboard

<img width="986" height="565" alt="image" src="https://github.com/user-attachments/assets/9af42240-2815-41b2-ac71-c712a0ee9ee6" />

---

## Key Findings

- Emergency admissions average ₹70K+ in patient cost with a cap breach 
rate of 6.6%, representing the highest immediate financial shock point

- Severe and Moderate risk BPL patients face the highest LAMA rates when 
the government cap is breached — clinical severity combined with sudden 
loss of subsidy creates an unmanageable financial toxicity loop

- Diseases like Sepsis, Respiratory Failure, and Cerebral Infarction 
contribute the most to patient out-of-pocket costs in emergency cases

- Across 120,000 admissions, the BPL rate remained stable at 71% 
regardless of admission type — confirming financial status does not 
influence how patients access the hospital across any tier of care

---

## Strategic Recommendations

- Trigger mandatory financial counseling for any BPL patient admitted 
under a Severe Charlson Risk Classification before exhausting their 
₹5 Lakh cap

- Investigate internal micro-subsidy or payment plan options explicitly 
for non-BPL patients facing sudden Emergency intake

- Implement a cap breach early warning system to enable proactive 
intervention before patients abandon care

---

## How to Run

Execute SQL scripts in the following order:
1. `Bronze` — raw ingestion
2. `Silver` — cleansing and transformation
3. `Gold` — dimensional modeling and views
4. Open Power BI file and refresh data source connection

---

## Author

**Zulfiqar Ali**
[LinkedIn](linkedin.com/in/iamzulfiqarali) 
