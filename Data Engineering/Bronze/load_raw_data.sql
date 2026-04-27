-- NOTE: FORMAT = 'CSV' is required here to invoke the advanced parser. 
-- It prevents column-shifting errors (Msg 4864) by safely ignoring 
-- commas that are embedded within double-quoted text fields

BULK INSERT bronze.admissions
FROM 'C:\Users\zulfy\Downloads\Healthcare Readmission Analytics\Indian Hospital Readmission Dataset\admissions.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n'
);

GO

BULK INSERT bronze.billing
FROM 'C:\Users\zulfy\Downloads\Healthcare Readmission Analytics\Indian Hospital Readmission Dataset\billing.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n'
);

GO



BULK INSERT bronze.diagnoses
FROM 'C:\Users\zulfy\Downloads\Healthcare Readmission Analytics\Indian Hospital Readmission Dataset\diagnoses.csv'
WITH (
FORMAT = 'CSV',
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n'
);

GO


BULK INSERT bronze.hospitals
FROM 'C:\Users\zulfy\Downloads\Healthcare Readmission Analytics\Indian Hospital Readmission Dataset\hospitals.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n'
);

GO


BULK INSERT bronze.patients
FROM 'C:\Users\zulfy\Downloads\Healthcare Readmission Analytics\Indian Hospital Readmission Dataset\patients.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n'
);

