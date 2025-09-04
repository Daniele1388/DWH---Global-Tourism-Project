/*===============================================================================
GOLD LAYER – FOREIGN KEY INTEGRITY CHECKS
---------------------------------------------------------------------------------
Purpose:
  Validate that the Gold layer fact views correctly reference their 
  related dimensions (Country, Indicator, Year, Unit of Measure).

How to use:
  - For Domestic / Inbound / Outbound / Industries: run the first query.
    Replace the fact table name in the FROM clause to test each one.
  - For SDG: use the second dedicated query.

Output:
  - Queries return ONLY rows where dimension keys are not resolved 
    (NULL values after LEFT JOIN). 
    Empty result set = integrity OK.

Notes:
  - Run after (re)creating dim_* and fact_* views.
  - Consider adding indexes on fact_* and dim_* keys to improve performance.
===============================================================================*/

-- ==================================================================================
-- Checking 'gold.fact_domestic_tourism'; 'gold.fact_inbound_tourism'; 
-- 'gold.fact_outbound_tourism';'gold.fact_tourism_industries'
-- ==================================================================================
--   Foreign Key Integrity (Dimensions) 
--   To use with Domestic/Inbound/Outbound/Industries, change the fact table in FROM.
  
	SELECT
	*
	FROM gold.fact_domestic_tourism f
	LEFT JOIN gold.dim_country c
	ON c.Country_key = f.Country_key
	LEFT JOIN gold.dim_indicator i
	ON i.Indicator_key = f.Indicator_key
	LEFT JOIN gold.dim_year y
	ON y.Year_key = f.Year_key
	LEFT JOIN gold.dim_unit_of_measure u
	ON u.Units_key = f.Units_key
	WHERE	c.Country_key IS NULL
			OR i.Indicator_key IS NULL
			OR y.Year_key IS NULL 
			OR u.Units_key IS NULL

-- ==================================================================================
-- Checking 'gold.fact_sdg'
-- ==================================================================================
--   Foreign Key Integrity (Dimensions) 
--   To use with SDG
  
  SELECT
	*
	FROM gold.fact_sdg f
	LEFT JOIN gold.dim_country c
	ON c.Country_key = f.Country_key
	LEFT JOIN gold.dim_year y
	ON y.Year_key = f.Year_key
	LEFT JOIN gold.dim_unit_of_measure u
	ON u.Units_key = f.Units_key
	WHERE	c.Country_key IS NULL
			OR y.Year_key IS NULL 
			OR u.Units_key IS NULL
