WITH AllMedians AS (
    -- ============================================================================
    -- PHASE 1: THE HOLDING PEN (Medians)
    -- SQL cannot calculate medians and standard aggregations at the same time.
    -- We isolate them here. DISTINCT ensures we only output ONE single summary row.
    -- ============================================================================
    SELECT DISTINCT
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_cost_inr) OVER() AS median_total_cost_inr,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY govt_subsidy_inr) OVER() AS median_govt_subsidy_inr
    FROM bronze.billing
), 

mainDQA AS (
    -- ============================================================================
    -- PHASE 2: THE MAIN AUDIT
    -- This squashes the entire table to count errors, missing data, and anomalies.
    -- ============================================================================
    SELECT 
        'bronze.billing' AS table_name,
        COUNT(*)         AS total_rows,

        -- --- PRIMARY & FOREIGN KEY CHECKS ---
        COUNT(bill_id) - COUNT(DISTINCT bill_id) AS dup_bill_ids,
        SUM( CASE WHEN bill_id IS NULL OR TRIM(bill_id) = '' THEN 1 ELSE 0 END ) AS missing_bill_id,
        SUM( CASE WHEN bill_id != TRIM(bill_id) THEN 1 ELSE 0 END) AS whitespace_bill_id,

        COUNT(admission_id) - COUNT(DISTINCT admission_id) AS dup_admission_ids,
        SUM( CASE WHEN admission_id IS NULL OR TRIM(admission_id) = '' THEN 1 ELSE 0 END ) AS missing_admission_ids,
        SUM( CASE WHEN admission_id != TRIM(admission_id) THEN 1 ELSE 0 END) AS whitespace_admission_id,

        -- --- FINANCIAL COLUMN CHECKS (total_cost_inr) ---
        SUM( CASE WHEN total_cost_inr IS NULL OR TRIM(CAST(total_cost_inr AS VARCHAR)) = '' THEN 1 ELSE 0 END ) AS missing_total_cost_inr,
        SUM( CASE WHEN total_cost_inr <= 0 THEN 1 ELSE 0 END ) AS negative_zero_total_cost_inr,
        
        MIN(total_cost_inr) AS min_total_cost_inr,
        MAX(total_cost_inr) AS max_total_cost_inr,
        
        -- OVERFLOW FIX: Upgrading the INT to a FLOAT before doing the math prevents 
        -- the calculator from crashing when the total sum exceeds 2.14 billion INR.
        CAST(AVG(CAST(total_cost_inr AS FLOAT)) AS DECIMAL(15,2)) AS avg_cost_inr,

        -- --- FINANCIAL COLUMN CHECKS (govt_subsidy_inr) ---
        SUM( CASE WHEN govt_subsidy_inr IS NULL OR TRIM(CAST(govt_subsidy_inr AS VARCHAR)) = '' THEN 1 ELSE 0 END ) AS missing_govt_subsidy_inr,
        SUM( CASE WHEN govt_subsidy_inr < 0 THEN 1 ELSE 0 END ) AS negative_govt_subsidy_inr,

        MIN(govt_subsidy_inr) AS min_govt_subsidy_inr,
        MAX(govt_subsidy_inr) AS max_govt_subsidy_inr,
        
        -- OVERFLOW FIX: Same logic applied here for the subsidy average.
        CAST(AVG(CAST(govt_subsidy_inr AS FLOAT)) AS DECIMAL(15,2)) AS avg_govt_subsidy_inr,

        -- --- MESSY TEXT CHECKS (Standard delimiter checks on the raw string) ---
        SUM( CASE WHEN cost_category IS NULL OR TRIM(cost_category) = '' THEN 1 ELSE 0 END ) AS missing_cost_category,
        SUM( CASE WHEN cost_category IS NOT NULL AND CHARINDEX(',', cost_category) = 0 THEN 1 ELSE 0 END ) AS missing_delimiter,
        SUM( CASE WHEN LEN(cost_category) - LEN(REPLACE(cost_category, ',', '')) > 1 THEN 1 ELSE 0 END ) AS multiple_delimiters,

        -- --- MESSY TEXT CHECKS (Using the dynamically extracted variables from CROSS APPLY) ---
        
        -- Testing the Extracted Cost
        SUM( CASE WHEN LOWER(ext_cost) = 'null' OR ext_cost IS NULL OR TRIM(ext_cost) = '' THEN 1 ELSE 0 END ) AS missing_values_in_cost_category,
        SUM( CASE WHEN TRY_CAST(ext_cost AS FLOAT) < 0 THEN 1 ELSE 0 END ) AS negative_cost_in_cost_category,
        SUM( CASE WHEN ext_cost != TRIM(ext_cost) THEN 1 ELSE 0 END ) AS whitespaces_cost_in_cost_category,

        -- Testing the Extracted Category
        SUM( CASE WHEN LOWER(ext_category) = 'null' OR ext_category IS NULL OR TRIM(ext_category) = '' THEN 1 ELSE 0 END ) AS missing_category_in_cost_category,
        SUM( CASE WHEN ext_category != TRIM(ext_category) THEN 1 ELSE 0 END ) AS whitespaces_category_in_cost_category

    FROM bronze.billing
    
    -- ============================================================================
    -- THE DESK ASSISTANTS (Row-by-Row String Extraction)
    -- ============================================================================
    
    -- Assistant 1: The Blind Splitter
    -- Chops the string at the comma. We intentionally DO NOT use TRIM() here 
    -- so that our whitespace error checks at the top will actually catch rogue spaces.
    CROSS APPLY (
        SELECT 
            CASE WHEN CHARINDEX(',', cost_category) > 0 
                 THEN SUBSTRING(cost_category, 1, CHARINDEX(',', cost_category) - 1) 
                 ELSE NULL END AS left_part,
                 
            CASE WHEN CHARINDEX(',', cost_category) > 0 
                 THEN SUBSTRING(cost_category, CHARINDEX(',', cost_category) + 1, LEN(cost_category)) 
                 ELSE NULL END AS right_part
    ) AS RawSplit

    -- Assistant 2: The Smart Sorter (Data Type Evaluation)
    -- Figures out which side of the comma is the money, regardless of typing order.
    CROSS APPLY (
        SELECT 
            -- COST: Whichever part successfully casts as a number becomes the cost.
            CASE WHEN TRY_CAST(left_part AS FLOAT) IS NOT NULL THEN left_part
                 WHEN TRY_CAST(right_part AS FLOAT) IS NOT NULL THEN right_part
                 ELSE NULL END AS ext_cost,
            
            -- CATEGORY: Whichever part failed the number test becomes the category word.
            CASE WHEN TRY_CAST(left_part AS FLOAT) IS NOT NULL THEN right_part
                 WHEN TRY_CAST(right_part AS FLOAT) IS NOT NULL THEN left_part
                 ELSE NULL END AS ext_category
    ) AS CleanStrings
)

-- ============================================================================
-- PHASE 3: FINAL OUTPUT GENERATION
-- Glues the single row of Medians to the single row of Error Counts.
-- ============================================================================
SELECT 
    d.table_name,
    d.total_rows,
    d.dup_bill_ids,
    d.missing_bill_id,
    d.whitespace_bill_id,
    d.dup_admission_ids,
    d.missing_admission_ids,
    d.whitespace_admission_id,
    
    d.missing_total_cost_inr,
    d.negative_zero_total_cost_inr,
    d.min_total_cost_inr,
    d.max_total_cost_inr,
    d.avg_cost_inr,
    m.median_total_cost_inr,  -- Inserted from the Median Holding Pen
    
    d.missing_govt_subsidy_inr,
    d.negative_govt_subsidy_inr,
    d.min_govt_subsidy_inr,
    d.max_govt_subsidy_inr,
    d.avg_govt_subsidy_inr,
    m.median_govt_subsidy_inr, -- Inserted from the Median Holding Pen
    
    d.missing_cost_category,
    d.missing_delimiter,
    d.multiple_delimiters,
    d.missing_values_in_cost_category,
    d.negative_cost_in_cost_category,
    d.whitespaces_cost_in_cost_category,
    d.missing_category_in_cost_category,
    d.whitespaces_category_in_cost_category

FROM mainDQA AS d
CROSS JOIN AllMedians AS m;