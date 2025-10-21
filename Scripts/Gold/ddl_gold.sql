/*
===============================================================
GLOBAL TOURISM DATA WAREHOUSE - GOLD LAYER
===============================================================
Purpose:
  This script creates the GOLD layer views for the Tourism Data 
  Warehouse. 
  - Dimension views (dim_*) provide master data for countries, 
    indicators, years, and units of measure. 
  - Fact views (fact_*) provide cleaned and conformed measures 
    from the Silver layer, linked to the dimension keys.

Usage:

   - These views can be queried directly for analytics and reporting. 
===============================================================
CREATE GOLD DIMENSION VIEWS
===============================================================
*/


-- =============================================================
-- gold.dim_country : Dimension table with unique countries
-- Provides a surrogate key for each country
-- =============================================================

CREATE OR ALTER VIEW gold.dim_country AS
WITH cte_country AS
(
	SELECT
		t.Country_code AS Country_id,
		t.Country AS Country_name
	FROM		
	(
		SELECT Country_code, Country FROM silver.domestic_accommodation
		UNION ALL SELECT Country_code, Country FROM silver.domestic_trip
		UNION ALL SELECT Country_code, Country FROM silver.inbound_accommodation
		UNION ALL SELECT Country_code, Country FROM silver.inbound_arrivals
		UNION ALL SELECT Country_code, Country FROM silver.inbound_expenditure
		UNION ALL SELECT Country_code, Country FROM silver.inbound_purpose
		UNION ALL SELECT Country_code, Country FROM silver.inbound_regions
		UNION ALL SELECT Country_code, Country FROM silver.inbound_transport
		UNION ALL SELECT Country_code, Country FROM silver.outbound_departures
		UNION ALL SELECT Country_code, Country FROM silver.outbound_expenditure
		UNION ALL SELECT Country_code, Country FROM silver.sdg_891
		UNION ALL SELECT Country_code, Country FROM silver.sdg_892
		UNION ALL SELECT Country_code, Country FROM silver.sdg_12b1
		UNION ALL SELECT Country_code, Country FROM silver.tourism_industries
	) t 
)
SELECT
	ROW_NUMBER() OVER (ORDER BY country_id) AS Country_key,
	Country_id,
	Country_name
FROM cte_country
WHERE country_id IS NOT NULL AND country_name IS NOT NULL;


-- =============================================================
-- gold.dim_indicator : Dimension table with unique indicators
-- Normalizes indicator codes and names, with source table info
-- =============================================================

CREATE OR ALTER VIEW gold.dim_indicator AS
WITH cte_indicator AS
(
	SELECT DISTINCT
	t.Indicator_code AS Indicator_id,
		CASE
			WHEN t.Indicator_code = '2.19' THEN t.Indicator + ' (ACCOMMODATION)'
			WHEN t.Indicator_code = '2.20' THEN t.Indicator + ' (ACCOMMODATION)'
			WHEN t.Indicator_code = '2.21' THEN t.Indicator + ' (HOTELS AND SIMILAR ESTABLISHMENTS)'
			WHEN t.Indicator_code = '2.22' THEN t.Indicator + ' (HOTELS AND SIMILAR ESTABLISHMENTS)'
			WHEN t.Indicator_code = '1.29' THEN t.Indicator + ' (ACCOMMODATION)'
			WHEN t.Indicator_code = '1.30' THEN t.Indicator + ' (ACCOMMODATION)'
			WHEN t.Indicator_code = '1.31' THEN t.Indicator + ' (HOTELS AND SIMILAR ESTABLISHMENTS)'
			WHEN t.Indicator_code = '1.32' THEN t.Indicator + ' (HOTELS AND SIMILAR ESTABLISHMENTS)'
			WHEN t.Indicator_code = '1.14' THEN t.Indicator + ' PURPOSE'
			WHEN t.Indicator_code = '1.5'  THEN t.Indicator + ' REGIONS'
			WHEN t.Indicator_code = '1.19' THEN t.Indicator + ' TRANSPORT'
			WHEN t.Indicator_code = '1.4'  THEN 'CRUISE PASSENGERS'
			WHEN t.Indicator_code = '1.13' THEN 'NATIONALS RESIDING ABROAD'
			ELSE Indicator
		END Indicator_name,
		t.Source_Table
	FROM
	(
		SELECT Indicator_code, COALESCE(UPPER(Indicator_L1), UPPER(Indicator_L2), UPPER(Indicator_L3), UPPER(Indicator_L4)) AS Indicator, 'DOMESTIC_ACCOMMODATION' AS Source_Table FROM silver.domestic_accommodation 
		UNION ALL SELECT Indicator_code, COALESCE(UPPER(Indicator_L1), UPPER(Indicator_L2), UPPER(Indicator_L3), UPPER(Indicator_L4)) AS Indicator, 'DOMESTIC_TRIP' AS Source_Table FROM silver.domestic_trip 
		UNION ALL SELECT Indicator_code, COALESCE(UPPER(Indicator_L1), UPPER(Indicator_L2), UPPER(Indicator_L3), UPPER(Indicator_L4)) AS Indicator, 'INBOUND_ACCOMMODATION' AS Source_Table FROM silver.inbound_accommodation  
		UNION ALL SELECT Indicator_code, COALESCE(UPPER(Indicator_L1), UPPER(Indicator_L2), UPPER(Indicator_L3), UPPER(Indicator_L4)) AS Indicator, 'INBOUND_ARRIVALS' AS Source_Table FROM silver.inbound_arrivals 
		UNION ALL SELECT Indicator_code, COALESCE(UPPER(Indicator_L1), UPPER(Indicator_L2), UPPER(Indicator_L3), UPPER(Indicator_L4)) AS Indicator, 'INBOUND_EXPENDITURE' AS Source_Table FROM silver.inbound_expenditure 
		UNION ALL SELECT Indicator_code, COALESCE(UPPER(Indicator_L1), UPPER(Indicator_L2), UPPER(Indicator_L3), UPPER(Indicator_L4)) AS Indicator, 'INBOUND_PURPOSE' AS Source_Table FROM silver.inbound_purpose 
		UNION ALL SELECT Indicator_code, COALESCE(UPPER(Indicator_L1), UPPER(Indicator_L2), UPPER(Indicator_L3), UPPER(Indicator_L4)) AS Indicator, 'INBOUND_REGIONS' AS Source_Table FROM silver.inbound_regions 
		UNION ALL SELECT Indicator_code, COALESCE(UPPER(Indicator_L1), UPPER(Indicator_L2), UPPER(Indicator_L3), UPPER(Indicator_L4)) AS Indicator, 'INBOUND_TRANSPORT' AS Source_Table FROM silver.inbound_transport 
		UNION ALL SELECT Indicator_code, COALESCE(UPPER(Indicator_L1), UPPER(Indicator_L2), UPPER(Indicator_L3), UPPER(Indicator_L4)) AS Indicator, 'OUTBOUND_DEPARTURES' AS Source_Table FROM silver.outbound_departures 
		UNION ALL SELECT Indicator_code, COALESCE(UPPER(Indicator_L1), UPPER(Indicator_L2), UPPER(Indicator_L3), UPPER(Indicator_L4)) AS Indicator, 'OUTBOUND_EXPENDITURE' AS Source_Table FROM silver.outbound_expenditure 
		UNION ALL SELECT Indicator_code, COALESCE(UPPER(Indicator_L1), UPPER(Indicator_L2), UPPER(Indicator_L3), UPPER(Indicator_L4)) AS Indicator, 'TOURISM_INDUSTRIES' AS Source_Table FROM silver.tourism_industries
	) t
)
SELECT
	ROW_NUMBER() OVER (ORDER BY Indicator_id) AS Indicator_key,
	Indicator_id,
	Indicator_name,
	Source_Table
FROM cte_indicator
WHERE Indicator_id IS NOT NULL AND Indicator_name IS NOT NULL;


-- =============================================================
-- gold.dim_year : Dimension table with years
-- Extracts distinct years from SDG and tourism datasets
-- =============================================================

CREATE OR ALTER VIEW gold.dim_year AS
WITH cte_year AS
(
	SELECT
		DATEPART(year, t.Time_Period) AS Year
	FROM 
	(
		SELECT Time_Period FROM silver.sdg_891
		UNION ALL SELECT Time_Period FROM silver.sdg_892
		UNION ALL SELECT Time_Period FROM silver.sdg_12b1
		UNION ALL SELECT TRY_CONVERT(DATE, RIGHT(c.name, 4)) AS Year FROM sys.columns AS c WHERE c.object_id IN (OBJECT_ID('silver.domestic_accommodation'))
	) t
)
SELECT
	ROW_NUMBER() OVER (ORDER BY Year) AS Year_key,
	Year
FROM cte_year
WHERE Year IS NOT NULL


-- =============================================================
-- gold.dim_unit_of_measure : Dimension table with units of measure
-- Standardizes different measure units across datasets
-- =============================================================

CREATE OR ALTER VIEW gold.dim_unit_of_measure AS
WITH cte_units AS
(
	SELECT DISTINCT
		CASE
			WHEN t.Measure_Units = 'UNITS' THEN 'NUMBER'
			WHEN t.Measure_Units = 'NIGHTS' THEN 'AVG_NIGHTS'
			ELSE t.Measure_Units
		END Measure_Units
	FROM
	(
		SELECT UPPER(Units) AS Measure_Units FROM silver.inbound_accommodation
		UNION ALL SELECT UPPER(Units) AS Measure_Units FROM silver.inbound_arrivals
		UNION ALL SELECT UPPER(Units) AS Measure_Units FROM silver.inbound_expenditure
		UNION ALL SELECT UPPER(Units) AS Measure_Units FROM silver.inbound_purpose
		UNION ALL SELECT UPPER(Units) AS Measure_Units FROM silver.inbound_regions
		UNION ALL SELECT UPPER(Units) AS Measure_Units FROM silver.inbound_transport
		UNION ALL SELECT UPPER(Units) AS Measure_Units FROM silver.domestic_accommodation
		UNION ALL SELECT UPPER(Units) AS Measure_Units FROM silver.domestic_trip
		UNION ALL SELECT UPPER(Units) AS Measure_Units FROM silver.outbound_departures
		UNION ALL SELECT UPPER(Units) AS Measure_Units FROM silver.outbound_expenditure
		UNION ALL SELECT UPPER(Units) AS Measure_Units FROM silver.sdg_12b1
		UNION ALL SELECT UPPER(Units) AS Measure_Units FROM silver.sdg_891
		UNION ALL SELECT UPPER(Units) AS Measure_Units FROM silver.sdg_892
		UNION ALL SELECT UPPER(Units) AS Measure_Units FROM silver.tourism_industries
	) t
)
SELECT
	ROW_NUMBER() OVER(ORDER BY Measure_Units) AS Units_key,
	Measure_Units
FROM cte_units
WHERE Measure_Units IS NOT NULL


/*
===============================================================
CREATE GOLD FACT VIEWS
===============================================================
*/


-- =============================================================
-- gold.fact_domestic_tourism : Fact table for domestic tourism
-- Contains data from domestic accommodation and trips
-- =============================================================

CREATE OR ALTER VIEW gold.fact_domestic_tourism AS
WITH cte_domestic AS
(
	SELECT 
	*
	FROM
	(
		SELECT
			Country_code,
			Indicator_code,
			Units,
			v.Year,
			v.Value
		FROM silver.domestic_accommodation AS a
		CROSS APPLY (VALUES
					(1995, a.[year_1995]), (1996, a.[year_1996]), (1997, a.[year_1997]), (1998, a.[year_1998]), 
					(1999, a.[year_1999]), (2000, a.[year_2000]), (2001, a.[year_2001]), (2002, a.[year_2002]), 
					(2003, a.[year_2003]), (2004, a.[year_2004]), (2005, a.[year_2005]), (2006, a.[year_2006]), 
					(2007, a.[year_2007]), (2008, a.[year_2008]), (2009, a.[year_2009]), (2010, a.[year_2010]), 
					(2011, a.[year_2011]), (2012, a.[year_2012]), (2013, a.[year_2013]), (2014, a.[year_2014]), 
					(2015, a.[year_2015]), (2016, a.[year_2016]), (2017, a.[year_2017]), (2018, a.[year_2018]), 
					(2019, a.[year_2019]), (2020, a.[year_2020]), (2021, a.[year_2021]), (2022, a.[year_2022])
					) AS v(Year, Value)
	UNION ALL
		SELECT
			Country_code,
			Indicator_code,
			Units,
			v.Year,
			v.Value
		FROM silver.domestic_trip AS a
		CROSS APPLY (VALUES
					(1995, a.[year_1995]), (1996, a.[year_1996]), (1997, a.[year_1997]), (1998, a.[year_1998]), 
					(1999, a.[year_1999]), (2000, a.[year_2000]), (2001, a.[year_2001]), (2002, a.[year_2002]), 
					(2003, a.[year_2003]), (2004, a.[year_2004]), (2005, a.[year_2005]), (2006, a.[year_2006]), 
					(2007, a.[year_2007]), (2008, a.[year_2008]), (2009, a.[year_2009]), (2010, a.[year_2010]), 
					(2011, a.[year_2011]), (2012, a.[year_2012]), (2013, a.[year_2013]), (2014, a.[year_2014]), 
					(2015, a.[year_2015]), (2016, a.[year_2016]), (2017, a.[year_2017]), (2018, a.[year_2018]), 
					(2019, a.[year_2019]), (2020, a.[year_2020]), (2021, a.[year_2021]), (2022, a.[year_2022])
					) AS v(Year, Value)
	) t
)
SELECT
	co.Country_key,
	ind.Indicator_key,
	ye.Year_key,
	un.Units_key,
	cte.Value
FROM cte_domestic AS cte
LEFT JOIN gold.dim_country co
ON cte.Country_code = co.Country_id
LEFT JOIN gold.dim_indicator ind
ON cte.Indicator_code = ind.Indicator_id
LEFT JOIN gold.dim_year ye
ON cte.Year = ye.Year
LEFT JOIN gold.dim_unit_of_measure un
ON cte.Units = un.Measure_Units
WHERE cte.Value IS NOT NULL


-- =============================================================
-- gold.fact_inbound_tourism : Fact table for inbound tourism
-- Contains data from inbound accommodation, arrivals, expenditure, purpose, regions, and transport
-- =============================================================

CREATE OR ALTER VIEW gold.fact_inbound_tourism AS
WITH cte_inbound AS
(
	SELECT
	*
	FROM
	(
		SELECT
			Country_code,
			Indicator_code,
			Units,
			v.Year,
			v.Value
		FROM silver.inbound_accommodation AS a
		CROSS APPLY (VALUES
					(1995, a.[year_1995]), (1996, a.[year_1996]), (1997, a.[year_1997]), (1998, a.[year_1998]), 
					(1999, a.[year_1999]), (2000, a.[year_2000]), (2001, a.[year_2001]), (2002, a.[year_2002]), 
					(2003, a.[year_2003]), (2004, a.[year_2004]), (2005, a.[year_2005]), (2006, a.[year_2006]), 
					(2007, a.[year_2007]), (2008, a.[year_2008]), (2009, a.[year_2009]), (2010, a.[year_2010]), 
					(2011, a.[year_2011]), (2012, a.[year_2012]), (2013, a.[year_2013]), (2014, a.[year_2014]), 
					(2015, a.[year_2015]), (2016, a.[year_2016]), (2017, a.[year_2017]), (2018, a.[year_2018]), 
					(2019, a.[year_2019]), (2020, a.[year_2020]), (2021, a.[year_2021]), (2022, a.[year_2022])
					) AS v(Year, Value)
	UNION ALL
		SELECT
			Country_code,
			Indicator_code,
			Units,
			v.Year,
			v.Value
		FROM silver.inbound_arrivals AS a
		CROSS APPLY (VALUES
					(1995, a.[year_1995]), (1996, a.[year_1996]), (1997, a.[year_1997]), (1998, a.[year_1998]), 
					(1999, a.[year_1999]), (2000, a.[year_2000]), (2001, a.[year_2001]), (2002, a.[year_2002]), 
					(2003, a.[year_2003]), (2004, a.[year_2004]), (2005, a.[year_2005]), (2006, a.[year_2006]), 
					(2007, a.[year_2007]), (2008, a.[year_2008]), (2009, a.[year_2009]), (2010, a.[year_2010]), 
					(2011, a.[year_2011]), (2012, a.[year_2012]), (2013, a.[year_2013]), (2014, a.[year_2014]), 
					(2015, a.[year_2015]), (2016, a.[year_2016]), (2017, a.[year_2017]), (2018, a.[year_2018]), 
					(2019, a.[year_2019]), (2020, a.[year_2020]), (2021, a.[year_2021]), (2022, a.[year_2022])
					) AS v(Year, Value)
	UNION ALL
		SELECT
			Country_code,
			Indicator_code,
			Units,
			v.Year,
			v.Value
		FROM silver.inbound_expenditure AS a
		CROSS APPLY (VALUES
					(1995, a.[year_1995]), (1996, a.[year_1996]), (1997, a.[year_1997]), (1998, a.[year_1998]), 
					(1999, a.[year_1999]), (2000, a.[year_2000]), (2001, a.[year_2001]), (2002, a.[year_2002]), 
					(2003, a.[year_2003]), (2004, a.[year_2004]), (2005, a.[year_2005]), (2006, a.[year_2006]), 
					(2007, a.[year_2007]), (2008, a.[year_2008]), (2009, a.[year_2009]), (2010, a.[year_2010]), 
					(2011, a.[year_2011]), (2012, a.[year_2012]), (2013, a.[year_2013]), (2014, a.[year_2014]), 
					(2015, a.[year_2015]), (2016, a.[year_2016]), (2017, a.[year_2017]), (2018, a.[year_2018]), 
					(2019, a.[year_2019]), (2020, a.[year_2020]), (2021, a.[year_2021]), (2022, a.[year_2022])
					) AS v(Year, Value)
	UNION ALL
		SELECT
			Country_code,
			Indicator_code,
			Units,
			v.Year,
			v.Value
		FROM silver.inbound_purpose AS a
		CROSS APPLY (VALUES
					(1995, a.[year_1995]), (1996, a.[year_1996]), (1997, a.[year_1997]), (1998, a.[year_1998]), 
					(1999, a.[year_1999]), (2000, a.[year_2000]), (2001, a.[year_2001]), (2002, a.[year_2002]), 
					(2003, a.[year_2003]), (2004, a.[year_2004]), (2005, a.[year_2005]), (2006, a.[year_2006]), 
					(2007, a.[year_2007]), (2008, a.[year_2008]), (2009, a.[year_2009]), (2010, a.[year_2010]), 
					(2011, a.[year_2011]), (2012, a.[year_2012]), (2013, a.[year_2013]), (2014, a.[year_2014]), 
					(2015, a.[year_2015]), (2016, a.[year_2016]), (2017, a.[year_2017]), (2018, a.[year_2018]), 
					(2019, a.[year_2019]), (2020, a.[year_2020]), (2021, a.[year_2021]), (2022, a.[year_2022])
					) AS v(Year, Value)
	UNION ALL
		SELECT
			Country_code,
			Indicator_code,
			Units,
			v.Year,
			v.Value
		FROM silver.inbound_regions AS a
		CROSS APPLY (VALUES
					(1995, a.[year_1995]), (1996, a.[year_1996]), (1997, a.[year_1997]), (1998, a.[year_1998]), 
					(1999, a.[year_1999]), (2000, a.[year_2000]), (2001, a.[year_2001]), (2002, a.[year_2002]), 
					(2003, a.[year_2003]), (2004, a.[year_2004]), (2005, a.[year_2005]), (2006, a.[year_2006]), 
					(2007, a.[year_2007]), (2008, a.[year_2008]), (2009, a.[year_2009]), (2010, a.[year_2010]), 
					(2011, a.[year_2011]), (2012, a.[year_2012]), (2013, a.[year_2013]), (2014, a.[year_2014]), 
					(2015, a.[year_2015]), (2016, a.[year_2016]), (2017, a.[year_2017]), (2018, a.[year_2018]), 
					(2019, a.[year_2019]), (2020, a.[year_2020]), (2021, a.[year_2021]), (2022, a.[year_2022])
					) AS v(Year, Value)
	UNION ALL
		SELECT
			Country_code,
			Indicator_code,
			Units,
			v.Year,
			v.Value
		FROM silver.inbound_transport AS a
		CROSS APPLY (VALUES
					(1995, a.[year_1995]), (1996, a.[year_1996]), (1997, a.[year_1997]), (1998, a.[year_1998]), 
					(1999, a.[year_1999]), (2000, a.[year_2000]), (2001, a.[year_2001]), (2002, a.[year_2002]), 
					(2003, a.[year_2003]), (2004, a.[year_2004]), (2005, a.[year_2005]), (2006, a.[year_2006]), 
					(2007, a.[year_2007]), (2008, a.[year_2008]), (2009, a.[year_2009]), (2010, a.[year_2010]), 
					(2011, a.[year_2011]), (2012, a.[year_2012]), (2013, a.[year_2013]), (2014, a.[year_2014]), 
					(2015, a.[year_2015]), (2016, a.[year_2016]), (2017, a.[year_2017]), (2018, a.[year_2018]), 
					(2019, a.[year_2019]), (2020, a.[year_2020]), (2021, a.[year_2021]), (2022, a.[year_2022])
					) AS v(Year, Value)
	) t
)
SELECT
	co.Country_key,
	ind.Indicator_key,
	ye.Year_key,
	un.Units_key,
	cte.Value
FROM cte_inbound cte
LEFT JOIN gold.dim_country co
ON cte.Country_code = co.Country_id
LEFT JOIN gold.dim_indicator ind
ON cte.Indicator_code = ind.Indicator_id
LEFT JOIN gold.dim_year ye
ON cte.Year = ye.Year
LEFT JOIN gold.dim_unit_of_measure un
ON cte.Units = un.Measure_Units
WHERE cte.Value IS NOT NULL




-- =============================================================
-- gold.fact_outbound_tourism : Fact table for outbound tourism
-- Contains data from outbound departures and expenditure
-- =============================================================

CREATE OR ALTER VIEW gold.fact_outbound_tourism AS
WITH cte_outbound AS
(
	SELECT
	*
	FROM
	(
		SELECT
			Country_code,
			Indicator_code,
			Units,
			v.Year,
			v.Value
		FROM silver.outbound_departures AS a
		CROSS APPLY (VALUES
					(1995, a.[year_1995]), (1996, a.[year_1996]), (1997, a.[year_1997]), (1998, a.[year_1998]), 
					(1999, a.[year_1999]), (2000, a.[year_2000]), (2001, a.[year_2001]), (2002, a.[year_2002]), 
					(2003, a.[year_2003]), (2004, a.[year_2004]), (2005, a.[year_2005]), (2006, a.[year_2006]), 
					(2007, a.[year_2007]), (2008, a.[year_2008]), (2009, a.[year_2009]), (2010, a.[year_2010]), 
					(2011, a.[year_2011]), (2012, a.[year_2012]), (2013, a.[year_2013]), (2014, a.[year_2014]), 
					(2015, a.[year_2015]), (2016, a.[year_2016]), (2017, a.[year_2017]), (2018, a.[year_2018]), 
					(2019, a.[year_2019]), (2020, a.[year_2020]), (2021, a.[year_2021]), (2022, a.[year_2022])
					) AS v(Year, Value)
	UNION ALL
		SELECT
			Country_code,
			Indicator_code,
			Units,
			v.Year,
			v.Value
		FROM silver.outbound_expenditure AS a
		CROSS APPLY (VALUES
					(1995, a.[year_1995]), (1996, a.[year_1996]), (1997, a.[year_1997]), (1998, a.[year_1998]), 
					(1999, a.[year_1999]), (2000, a.[year_2000]), (2001, a.[year_2001]), (2002, a.[year_2002]), 
					(2003, a.[year_2003]), (2004, a.[year_2004]), (2005, a.[year_2005]), (2006, a.[year_2006]), 
					(2007, a.[year_2007]), (2008, a.[year_2008]), (2009, a.[year_2009]), (2010, a.[year_2010]), 
					(2011, a.[year_2011]), (2012, a.[year_2012]), (2013, a.[year_2013]), (2014, a.[year_2014]), 
					(2015, a.[year_2015]), (2016, a.[year_2016]), (2017, a.[year_2017]), (2018, a.[year_2018]), 
					(2019, a.[year_2019]), (2020, a.[year_2020]), (2021, a.[year_2021]), (2022, a.[year_2022])
					) AS v(Year, Value)
	) t
)
SELECT
	co.Country_key,
	ind.Indicator_key,
	ye.Year_key,
	un.Units_key,
	cte.Value
FROM cte_outbound cte
LEFT JOIN gold.dim_country co
ON cte.Country_code = co.Country_id
LEFT JOIN gold.dim_indicator ind
ON cte.Indicator_code = ind.Indicator_id
LEFT JOIN gold.dim_year ye
ON cte.Year = ye.Year
LEFT JOIN gold.dim_unit_of_measure un
ON cte.Units = un.Measure_Units
WHERE cte.Value IS NOT NULL


-- =============================================================
-- gold.fact_sdg : Fact table for SDG indicators
-- Contains data from SDG 8.9.1, 8.9.2, and 12.b.1 datasets
-- =============================================================

CREATE OR ALTER VIEW gold.fact_sdg AS
WITH cte_sdg AS
(
	SELECT 
	*
	FROM
	(
		SELECT Country_code, SeriesDescription, Units, DATEPART(year, Time_Period) AS Year, Value FROM silver.sdg_891
		UNION ALL SELECT Country_code, SeriesDescription, Units, DATEPART(year, Time_Period) AS Year, Value FROM silver.sdg_892	 
		UNION ALL SELECT Country_code, SeriesDescription, Units, DATEPART(year, Time_Period) AS Year, Value FROM silver.sdg_12b1		
	) t
)
SELECT
	co.Country_key,
	-- Add readable SDG indicator code
	CASE 
		WHEN SeriesDescription = 'Tourism direct GDP as a proportion of total GDP (%)' THEN 'SDG_8.9.1_GDP'
		WHEN SeriesDescription = 'Employed persons in the tourism industries (number)' THEN 'SDG_8.9.2_EMP'
		WHEN SeriesDescription = 'Implementation of standard accounting tools to monitor the economic and environmental aspects of tourism (SEEA tables)' THEN 'SDG_12.b.1_SEEA'
		ELSE SeriesDescription
	END AS Indicator,
	ye.Year_key,
	un.Units_key,
	cte.Value
FROM cte_sdg cte
LEFT JOIN gold.dim_country co
ON cte.Country_code = co.Country_id
LEFT JOIN gold.dim_year ye
ON cte.Year = ye.Year
LEFT JOIN gold.dim_unit_of_measure un
ON cte.Units = un.Measure_Units

  
-- =============================================================
-- gold.fact_tourism_industries : Fact table for tourism industries
-- Contains data on tourism industries indicators and values
-- =============================================================

CREATE OR ALTER VIEW gold.fact_tourism_industries AS
WITH cte_industries AS
(
	SELECT
		Country_code,
		Indicator_code,
		CASE
			WHEN Units = 'Units' THEN 'NUMBER'
			WHEN Units = 'Nights' THEN 'AVG_NIGHTS'
			ELSE Units
		END Units,
		v.Year,
		v.Value
	FROM silver.tourism_industries AS a
	CROSS APPLY (VALUES
				(1995, a.[year_1995]), (1996, a.[year_1996]), (1997, a.[year_1997]), (1998, a.[year_1998]), 
				(1999, a.[year_1999]), (2000, a.[year_2000]), (2001, a.[year_2001]), (2002, a.[year_2002]), 
				(2003, a.[year_2003]), (2004, a.[year_2004]), (2005, a.[year_2005]), (2006, a.[year_2006]), 
				(2007, a.[year_2007]), (2008, a.[year_2008]), (2009, a.[year_2009]), (2010, a.[year_2010]), 
				(2011, a.[year_2011]), (2012, a.[year_2012]), (2013, a.[year_2013]), (2014, a.[year_2014]), 
				(2015, a.[year_2015]), (2016, a.[year_2016]), (2017, a.[year_2017]), (2018, a.[year_2018]), 
				(2019, a.[year_2019]), (2020, a.[year_2020]), (2021, a.[year_2021]), (2022, a.[year_2022])
				) AS v(Year, Value)
)
SELECT
	co.Country_key,
	ind.Indicator_key,
	ye.Year_key,
	un.Units_key,
	cte.Value
FROM cte_industries cte
LEFT JOIN gold.dim_country co
ON cte.Country_code = co.Country_id
LEFT JOIN gold.dim_indicator ind
ON cte.Indicator_code = ind.Indicator_id
LEFT JOIN gold.dim_year ye
ON cte.Year = ye.Year
LEFT JOIN gold.dim_unit_of_measure un
ON cte.Units = un.Measure_Units
WHERE cte.Value IS NOT NULL
