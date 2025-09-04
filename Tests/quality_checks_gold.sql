-- ==================================================================================
-- checking 'gold.fact_domestic_tourism'; 'gold.fact_inbound_tourism'; 
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
