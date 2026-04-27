-- ==============================================================================
-- STEP 1: Create the Physical Table Structure
-- ==============================================================================
CREATE TABLE Gold.DimDate (
    date_key INT PRIMARY KEY,      -- The crucial part: Setting this as the Primary Key
    full_date DATE NOT NULL,
    day_name VARCHAR(15),
    month_name VARCHAR(15),
    month_number INT,
    quarter INT,
    year INT,
    is_weekend BIT                 -- BIT is SQL Server's version of a boolean (1 or 0)
);
GO

-- ==============================================================================
-- STEP 2: Populate the Table
-- ==============================================================================
DECLARE @StartDate DATE = '2015-01-01';
DECLARE @EndDate DATE = '2025-12-31';

WITH DateCTE AS (
    -- Anchor: Start at the beginning
    SELECT @StartDate AS full_date
    UNION ALL
    -- Loop: Add one day at a time
    SELECT DATEADD(day, 1, full_date)
    FROM DateCTE
    WHERE full_date < @EndDate
)
-- Insert the calculated results directly into the new table
INSERT INTO Gold.DimDate (date_key, full_date, day_name, month_name, month_number, quarter, year, is_weekend)
SELECT 
    CAST(CONVERT(VARCHAR(8), full_date, 112) AS INT) AS date_key,
    full_date,
    DATENAME(weekday, full_date) AS day_name,
    DATENAME(month, full_date) AS month_name,
    DATEPART(month, full_date) AS month_number,
    DATEPART(quarter, full_date) AS quarter,
    YEAR(full_date) AS year,
    CASE 
        WHEN DATENAME(weekday, full_date) IN ('Saturday', 'Sunday') THEN 1 
        ELSE 0 
    END AS is_weekend
FROM DateCTE
-- This overrides SQL Server's 100-loop safety limit so it can generate all 20 years
OPTION (MAXRECURSION 0);


