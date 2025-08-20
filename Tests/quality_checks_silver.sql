-- ====================================================================
-- Data Quality Checks for Silver Layer
-- ====================================================================
-- These queries are designed to be run on all silver tables in order to:
--   1. Detect duplicates
--   2. Identify unwanted spaces in text columns
--   3. Verify data standardization & consistency (e.g., country names)
--   4. Detect strange or non-standard characters
--   5. Ensure ID-to-country mapping consistency across tables
--
-- Run each block against the relevant silver tables to validate data quality.


-- Check Duplicates

SELECT
	Country,
	COUNT(*)
FROM silver.domestic_accommodation
GROUP BY Country
HAVING COUNT(*) > 1;

-- Check unwanted spaces

SELECT
	Country,
	Series_L1,
	Series_L2,
	Series_L3,
	Series_L4,
	Units
FROM silver.domestic_accommodation
WHERE Country != TRIM(Country) 
OR Series_L1 != TRIM(Series_L1)
OR Series_L2 != TRIM(Series_L2)
OR Series_L3 != TRIM(Series_L3)
OR Series_L4 != TRIM(Series_L4)
OR Units != TRIM(Units);

-- Data Standardization & Consistency

SELECT DISTINCT
	Country
FROM silver.domestic_trip;

-- Check strange characters

SELECT DISTINCT
	Country
FROM silver.sdg_891
WHERE Country COLLATE Latin1_General_BIN LIKE '%[^a-zA-Z0-9 _-]%';

-- Check ID mapping

SELECT DISTINCT
	*
FROM (
SELECT 
    COALESCE(acc.Geo_Area_Code, sdg1.Geo_Area_Code) AS ID,
    acc.Country AS Country_Acc,
    sdg1.Country AS Country_SDG,
    CASE
        WHEN acc.Geo_Area_Code IS NULL THEN 'Missing_in_Accommodation'
        WHEN sdg1.Geo_Area_Code IS NULL THEN 'Missing_in_SDG'
        WHEN acc.Country != sdg1.Country THEN 'Country_Conflict'
        ELSE 'OK'
    END AS Status
FROM silver.domestic_accommodation AS acc
FULL JOIN silver.sdg_12b1 AS sdg1
    ON acc.Geo_Area_Code = sdg1.Geo_Area_Code
 ) t
 WHERE Status = 'Country_Conflict' AND Country_Acc IS NOT NULL;
