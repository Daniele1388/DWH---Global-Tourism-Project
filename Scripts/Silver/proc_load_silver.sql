/*
=========================================================
Load Silver Layer from Bronze Raw Tables
=========================================================

Purpose
-------
Loads cleaned and standardized data from the 'bronze' schema (raw CSV ingest)
into the 'silver' schema (analysis-ready wide tables). The procedure is
idempotent: each target table is TRUNCATED and fully reloaded.

What this script DOES
---------------------
1) Table Groups Loaded
   - Domestic/Inbound/Outbound tourism wide tables:
     silver.domestic_accommodation, silver.domestic_trip,
     silver.inbound_accommodation, silver.outbound_departures,
     silver.tourism_industries, silver.inbound_arrivals,
     silver.inbound_expenditure, silver.inbound_purpose,
     silver.inbound_regions, silver.inbound_transport,
     silver.outbound_expenditure
   - SDG indicators (long format by year):
     silver.sdg_891, silver.sdg_892, silver.sdg_12b1

2) Column Mapping & Renaming
   - Bronze C      -> Silver Country_code (INT)
   - Bronze S      -> Silver Indicator_code (**NVARCHAR(20)**, normalized as text; preserves trailing zeros)
   - Bronze Basic_Data -> Silver Country (cleaned/standardized)
   - Bronze Unnamed_5..Unnamed_8 -> Silver Indicator_L1..Indicator_L4
   - Bronze Units  -> Silver Units
   - For SDG tables: GeoAreaCode -> Country_code; GeoAreaName -> Country;
     TimePeriod (YYYY) -> Time_Period (DATE = Jan 1 of that year);
     Value -> DECIMAL(18,2); SeriesDescription, Nature, Units carried over.

3) Data Cleaning Rules (applied consistently)
   - Whitespace: TRIM/LTRIM/RTRIM on all textual columns.
   - Placeholders: convert '..' and empty string '' to NULL (where appropriate).
   - Numbers from text (years/values):
       * Remove thousands separators (',') via REPLACE, preserve decimal points.
       * Cast to DECIMAL(18,2) for all year_* columns and SDG.Value.
   - Country: remove header/noise rows (“The information…”, “Source…” → NULL),
     plus normalize country names (COTE D'IVOIRE, SOUTH KOREA, HONG KONG, etc.).

4) Normalization of `Indicator_code` (from Bronze.S NVARCHAR)
   - **General rule** (applied to all tourism tables):
       * If `S` is NULL/empty → NULL.
       * If `S` already contains a '.' → leave as is (prevents double dots).
       * If `S` is only digits and LEN >= 2 → insert a dot after the first digit
         (e.g. '219' → '2.19', '22' → '2.2', '110' → '1.10').
       * If LEN = 1 → leave as is (e.g. '3' → '3').
     (Implemented with `STUFF(S, 2, 0, '.')` after checks; zero-padding
      is preserved because the column remains NVARCHAR.)
   - **Table-specific fixes** (to keep both variants with and without trailing zero, as required by the domain):
       * silver.domestic_accommodation: force '2.2' → **'2.20'**
       * silver.inbound_accommodation: force '1.3' → **'1.30'**
       * silver.inbound_transport:      force '1.2' → **'1.20'**
       * silver.inbound_regions:        force '1.1' → **'1.10'**
     These fixes are applied **before** the general rule so they take precedence.
   - Goal: allow coexistence of both '1.1' and '1.10', '2.2' and '2.20', '1.3' and '1.30', etc., exactly as in the desired reference list.

5) Special SDG Handling
   - Time_Period: DATEFROMPARTS(YYYY, 1, 1).
   - Country code remapping for consistency:
       * sdg_891 & sdg_892: 534→663, 535→658, 531→535, 276→280, 231→230
       * sdg_12b1:          534→663, 535→658, 531→535, 276→280, 231→288
         (plus name fix: ETHIOPIA → GHANA for alignment 231→288)
   - Constant SDG columns dropped: INDEX / SDG_Indicator / SeriesCode / Source.

6) Operational Behavior
   - Full idempotent reload: TRUNCATE + INSERT SELECT.
   - PRINT logging of section start/finish and elapsed times.
   - TRY…CATCH with detailed diagnostics (message/number/state/severity/line/proc).

======================================================================
*/


ALTER   PROCEDURE [silver].[load_data] AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================================';
		PRINT 'Load Silver (Group 1): domestic_accommodation, domestic_trip,';
		PRINT 'inbound_accommodation, outbound_departures, tourism_industries';
		PRINT 'Source: corresponding bronze raw tables (wide year_* format)';
		PRINT 'Cleaning: TRIM text; convert ''..''/'''' to NULL; remove thousands';
		PRINT 'separators (commas) preserving decimals; cast year_* to DECIMAL(18,2);';
		PRINT 'map C -> Country_code (INT) and S -> Indicator_code (NVARCHAR(20));';
		PRINT 'standardize Country names and ignore header/noise rows.';
		PRINT 'Dropped: C_and_S, Notes, and unused Unnamed_* columns.';
		PRINT '=========================================================';

-- silver.domestic_accommodation
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.domestic_accommodation;
		INSERT INTO silver.domestic_accommodation
		(
			Country_code,
			Indicator_code,
			Country,
			Indicator_L1,
			Indicator_L2,
			Indicator_L3,
			Indicator_L4,
			Units,
			year_1995,
			year_1996,
			year_1997,
			year_1998,
			year_1999,
			year_2000,
			year_2001,
			year_2002,
			year_2003,
			year_2004,
			year_2005,
			year_2006,
			year_2007,
			year_2008,
			year_2009,
			year_2010,
			year_2011,
			year_2012,
			year_2013,
			year_2014,
			year_2015,
			year_2016,
			year_2017,
			year_2018,
			year_2019,
			year_2020,
			year_2021,
			year_2022
		)
		SELECT
			TRY_CONVERT(INT, C) AS Country_code,
			CASE
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') IS NULL THEN NULL
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') = '2.2' THEN '2.20'
				WHEN CHARINDEX('.', TRIM(REPLACE(S, ',', '.'))) > 0 THEN TRIM(REPLACE(S, ',', '.'))
				WHEN LEN(TRIM(REPLACE(S, ',', '.'))) = 1 THEN TRIM(REPLACE(S, ',', '.'))
				ELSE STUFF(TRIM(REPLACE(S, ',', '.')), 2, 0, '.') 
			END AS Indicator_code,
			CASE 
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE '"The information%' THEN NULL
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'Source%' THEN NULL
			ELSE 
				CASE
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'COTE D' +  NCHAR(0x252C) + NCHAR(0x2524) + N'IVOIRE' THEN N'COTE D''IVOIRE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'BOLIVIA, PLURINATIONAL STATE OF' THEN 'BOLIVIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'CURA'+ NCHAR(0x251C) + N'çAO' THEN 'CURACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'HONG KONG, CHINA' THEN 'HONG KONG'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'KOREA, DEMOCRATIC PEOPLE%S REPUBLIC OF' THEN 'NORTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'KOREA, REPUBLIC OF' THEN 'SOUTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE  'LAO PEOPLE%S DEMOCRATIC REPUBLIC' THEN 'LAOS'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MACAO, CHINA' THEN 'MACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MICRONESIA, FEDERATED STATES OF' THEN 'MICRONESIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MOLDOVA, REPUBLIC OF' THEN 'MOLDOVA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SINT MAARTEN (DUTCH PART)' THEN 'SINT MAARTEN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TANZANIA, UNITED REPUBLIC OF' THEN 'TANZANIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TIMOR-LESTE' THEN 'TIMOR EST'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'VENEZUELA, BOLIVARIAN REPUBLIC OF' THEN 'VENEZUELA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'T%RKIYE' THEN 'TURKIYE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CZECH REPUBLIC (CZECHIA)' THEN 'CZECHIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CONGO, DEMOCRATIC REPUBLIC OF THE' THEN 'DEMOCRATIC REPUBLIC OF CONGO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'IRAN, ISLAMIC REPUBLIC OF' THEN 'IRAN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SYRIAN ARAB REPUBLIC' THEN 'SYRIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TAIWAN PROVINCE OF CHINA' THEN 'TAIWAN'
					ELSE NULLIF(TRIM(REPLACE(Basic_data,'..','')), '')
				END
			END AS Country,
			NULLIF(TRIM(Unnamed_5), '') AS Indicator_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Indicator_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Indicator_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Indicator_L4,
			NULLIF(TRIM(Units), '') AS Units,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1995, '..',''), ',', '')),'')) AS year_1995,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1996, '..',''), ',', '')),'')) AS year_1996,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1997, '..',''), ',', '')),'')) AS year_1997,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1998, '..',''), ',', '')),'')) AS year_1998,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1999, '..',''), ',', '')),'')) AS year_1999,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2000, '..',''), ',', '')),'')) AS year_2000,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2001, '..',''), ',', '')),'')) AS year_2001,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2002, '..',''), ',', '')),'')) AS year_2002,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2003, '..',''), ',', '')),'')) AS year_2003,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2004, '..',''), ',', '')),'')) AS year_2004,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2005, '..',''), ',', '')),'')) AS year_2005,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2006, '..',''), ',', '')),'')) AS year_2006,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2007, '..',''), ',', '')),'')) AS year_2007,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2008, '..',''), ',', '')),'')) AS year_2008,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2009, '..',''), ',', '')),'')) AS year_2009,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2010, '..',''), ',', '')),'')) AS year_2010,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2011, '..',''), ',', '')),'')) AS year_2011,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2012, '..',''), ',', '')),'')) AS year_2012,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2013, '..',''), ',', '')),'')) AS year_2013,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2014, '..',''), ',', '')),'')) AS year_2014,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2015, '..',''), ',', '')),'')) AS year_2015,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2016, '..',''), ',', '')),'')) AS year_2016,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2017, '..',''), ',', '')),'')) AS year_2017,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2018, '..',''), ',', '')),'')) AS year_2018,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2019, '..',''), ',', '')),'')) AS year_2019,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2020, '..',''), ',', '')),'')) AS year_2020,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2021, '..',''), ',', '')),'')) AS year_2021,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2022, '..',''), ',', '')),'')) AS year_2022
		FROM bronze.raw_domestic_accommodation;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

-- silver.domestic_trip
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.domestic_trip;
		INSERT INTO silver.domestic_trip
		(
			Country_code,
			Indicator_code,
			Country,
			Indicator_L1,
			Indicator_L2,
			Indicator_L3,
			Indicator_L4,
			Units,
			year_1995,
			year_1996,
			year_1997,
			year_1998,
			year_1999,
			year_2000,
			year_2001,
			year_2002,
			year_2003,
			year_2004,
			year_2005,
			year_2006,
			year_2007,
			year_2008,
			year_2009,
			year_2010,
			year_2011,
			year_2012,
			year_2013,
			year_2014,
			year_2015,
			year_2016,
			year_2017,
			year_2018,
			year_2019,
			year_2020,
			year_2021,
			year_2022
		)
		SELECT
			TRY_CONVERT(INT, C) AS Country_code,
			CASE
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') IS NULL THEN NULL
				WHEN CHARINDEX('.', TRIM(REPLACE(S, ',', '.'))) > 0 THEN TRIM(REPLACE(S, ',', '.'))
				WHEN LEN(TRIM(REPLACE(S, ',', '.'))) = 1 THEN TRIM(REPLACE(S, ',', '.'))
				ELSE STUFF(TRIM(REPLACE(S, ',', '.')), 2, 0, '.') 
			END AS Indicator_code,
			CASE 
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE '"The information%' THEN NULL
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'Source%' THEN NULL
			ELSE 
				CASE
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'COTE D' +  NCHAR(0x252C) + NCHAR(0x2524) + N'IVOIRE' THEN N'COTE D''IVOIRE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'BOLIVIA, PLURINATIONAL STATE OF' THEN 'BOLIVIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'CURA'+ NCHAR(0x251C) + N'çAO' THEN 'CURACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'HONG KONG, CHINA' THEN 'HONG KONG'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'KOREA, DEMOCRATIC PEOPLE%S REPUBLIC OF' THEN 'NORTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'KOREA, REPUBLIC OF' THEN 'SOUTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE  'LAO PEOPLE%S DEMOCRATIC REPUBLIC' THEN 'LAOS'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MACAO, CHINA' THEN 'MACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MICRONESIA, FEDERATED STATES OF' THEN 'MICRONESIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MOLDOVA, REPUBLIC OF' THEN 'MOLDOVA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SINT MAARTEN (DUTCH PART)' THEN 'SINT MAARTEN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TANZANIA, UNITED REPUBLIC OF' THEN 'TANZANIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TIMOR-LESTE' THEN 'TIMOR EST'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'VENEZUELA, BOLIVARIAN REPUBLIC OF' THEN 'VENEZUELA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'T%RKIYE' THEN 'TURKIYE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CZECH REPUBLIC (CZECHIA)' THEN 'CZECHIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CONGO, DEMOCRATIC REPUBLIC OF THE' THEN 'DEMOCRATIC REPUBLIC OF CONGO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'IRAN, ISLAMIC REPUBLIC OF' THEN 'IRAN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SYRIAN ARAB REPUBLIC' THEN 'SYRIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TAIWAN PROVINCE OF CHINA' THEN 'TAIWAN'
					ELSE NULLIF(TRIM(REPLACE(Basic_data,'..','')), '')
				END
			END AS Country,
			NULLIF(TRIM(Unnamed_5), '') AS Indicator_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Indicator_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Indicator_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Indicator_L4,
			NULLIF(TRIM(Units), '') AS Units,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1995, '..',''), ',', '')),'')) AS year_1995,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1996, '..',''), ',', '')),'')) AS year_1996,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1997, '..',''), ',', '')),'')) AS year_1997,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1998, '..',''), ',', '')),'')) AS year_1998,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1999, '..',''), ',', '')),'')) AS year_1999,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2000, '..',''), ',', '')),'')) AS year_2000,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2001, '..',''), ',', '')),'')) AS year_2001,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2002, '..',''), ',', '')),'')) AS year_2002,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2003, '..',''), ',', '')),'')) AS year_2003,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2004, '..',''), ',', '')),'')) AS year_2004,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2005, '..',''), ',', '')),'')) AS year_2005,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2006, '..',''), ',', '')),'')) AS year_2006,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2007, '..',''), ',', '')),'')) AS year_2007,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2008, '..',''), ',', '')),'')) AS year_2008,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2009, '..',''), ',', '')),'')) AS year_2009,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2010, '..',''), ',', '')),'')) AS year_2010,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2011, '..',''), ',', '')),'')) AS year_2011,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2012, '..',''), ',', '')),'')) AS year_2012,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2013, '..',''), ',', '')),'')) AS year_2013,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2014, '..',''), ',', '')),'')) AS year_2014,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2015, '..',''), ',', '')),'')) AS year_2015,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2016, '..',''), ',', '')),'')) AS year_2016,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2017, '..',''), ',', '')),'')) AS year_2017,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2018, '..',''), ',', '')),'')) AS year_2018,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2019, '..',''), ',', '')),'')) AS year_2019,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2020, '..',''), ',', '')),'')) AS year_2020,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2021, '..',''), ',', '')),'')) AS year_2021,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2022, '..',''), ',', '')),'')) AS year_2022
		FROM bronze.raw_domestic_trip;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
	
-- silver.inbound_accommodation
	
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.inbound_accommodation;
		INSERT INTO silver.inbound_accommodation
		(
			Country_code,
			Indicator_code,
			Country,
			Indicator_L1,
			Indicator_L2,
			Indicator_L3,
			Indicator_L4,
			Units,
			year_1995,
			year_1996,
			year_1997,
			year_1998,
			year_1999,
			year_2000,
			year_2001,
			year_2002,
			year_2003,
			year_2004,
			year_2005,
			year_2006,
			year_2007,
			year_2008,
			year_2009,
			year_2010,
			year_2011,
			year_2012,
			year_2013,
			year_2014,
			year_2015,
			year_2016,
			year_2017,
			year_2018,
			year_2019,
			year_2020,
			year_2021,
			year_2022
		)
		SELECT
			TRY_CONVERT(INT, C) AS Country_code,
			CASE
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') IS NULL THEN NULL
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') = '1.3' THEN '1.30'
				WHEN CHARINDEX('.', TRIM(REPLACE(S, ',', '.'))) > 0 THEN TRIM(REPLACE(S, ',', '.'))
				WHEN LEN(TRIM(REPLACE(S, ',', '.'))) = 1 THEN TRIM(REPLACE(S, ',', '.'))
				ELSE STUFF(TRIM(REPLACE(S, ',', '.')), 2, 0, '.') 
			END AS Indicator_code,
			CASE 
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE '"The information%' THEN NULL
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'Source%' THEN NULL
			ELSE 
				CASE
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'COTE D' +  NCHAR(0x252C) + NCHAR(0x2524) + N'IVOIRE' THEN N'COTE D''IVOIRE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'BOLIVIA, PLURINATIONAL STATE OF' THEN 'BOLIVIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'CURA'+ NCHAR(0x251C) + N'çAO' THEN 'CURACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'HONG KONG, CHINA' THEN 'HONG KONG'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'KOREA, DEMOCRATIC PEOPLE%S REPUBLIC OF' THEN 'NORTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'KOREA, REPUBLIC OF' THEN 'SOUTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE  'LAO PEOPLE%S DEMOCRATIC REPUBLIC' THEN 'LAOS'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MACAO, CHINA' THEN 'MACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MICRONESIA, FEDERATED STATES OF' THEN 'MICRONESIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MOLDOVA, REPUBLIC OF' THEN 'MOLDOVA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SINT MAARTEN (DUTCH PART)' THEN 'SINT MAARTEN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TANZANIA, UNITED REPUBLIC OF' THEN 'TANZANIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TIMOR-LESTE' THEN 'TIMOR EST'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'VENEZUELA, BOLIVARIAN REPUBLIC OF' THEN 'VENEZUELA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'T%RKIYE' THEN 'TURKIYE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CZECH REPUBLIC (CZECHIA)' THEN 'CZECHIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CONGO, DEMOCRATIC REPUBLIC OF THE' THEN 'DEMOCRATIC REPUBLIC OF CONGO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'IRAN, ISLAMIC REPUBLIC OF' THEN 'IRAN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SYRIAN ARAB REPUBLIC' THEN 'SYRIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TAIWAN PROVINCE OF CHINA' THEN 'TAIWAN'
					ELSE NULLIF(TRIM(REPLACE(Basic_data,'..','')), '')
				END
			END AS Country,
			NULLIF(TRIM(Unnamed_5), '') AS Indicator_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Indicator_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Indicator_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Indicator_L4,
			NULLIF(TRIM(Units), '') AS Units,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1995, '..',''), ',', '')),'')) AS year_1995,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1996, '..',''), ',', '')),'')) AS year_1996,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1997, '..',''), ',', '')),'')) AS year_1997,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1998, '..',''), ',', '')),'')) AS year_1998,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1999, '..',''), ',', '')),'')) AS year_1999,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2000, '..',''), ',', '')),'')) AS year_2000,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2001, '..',''), ',', '')),'')) AS year_2001,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2002, '..',''), ',', '')),'')) AS year_2002,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2003, '..',''), ',', '')),'')) AS year_2003,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2004, '..',''), ',', '')),'')) AS year_2004,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2005, '..',''), ',', '')),'')) AS year_2005,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2006, '..',''), ',', '')),'')) AS year_2006,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2007, '..',''), ',', '')),'')) AS year_2007,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2008, '..',''), ',', '')),'')) AS year_2008,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2009, '..',''), ',', '')),'')) AS year_2009,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2010, '..',''), ',', '')),'')) AS year_2010,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2011, '..',''), ',', '')),'')) AS year_2011,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2012, '..',''), ',', '')),'')) AS year_2012,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2013, '..',''), ',', '')),'')) AS year_2013,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2014, '..',''), ',', '')),'')) AS year_2014,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2015, '..',''), ',', '')),'')) AS year_2015,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2016, '..',''), ',', '')),'')) AS year_2016,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2017, '..',''), ',', '')),'')) AS year_2017,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2018, '..',''), ',', '')),'')) AS year_2018,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2019, '..',''), ',', '')),'')) AS year_2019,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2020, '..',''), ',', '')),'')) AS year_2020,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2021, '..',''), ',', '')),'')) AS year_2021,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2022, '..',''), ',', '')),'')) AS year_2022
		FROM bronze.raw_inbound_accommodation;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

-- silver.outbound_departures
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.outbound_departures;
		INSERT INTO silver.outbound_departures
		(
			Country_code,
			Indicator_code,
			Country,
			Indicator_L1,
			Indicator_L2,
			Indicator_L3,
			Indicator_L4,
			Units,
			year_1995,
			year_1996,
			year_1997,
			year_1998,
			year_1999,
			year_2000,
			year_2001,
			year_2002,
			year_2003,
			year_2004,
			year_2005,
			year_2006,
			year_2007,
			year_2008,
			year_2009,
			year_2010,
			year_2011,
			year_2012,
			year_2013,
			year_2014,
			year_2015,
			year_2016,
			year_2017,
			year_2018,
			year_2019,
			year_2020,
			year_2021,
			year_2022
		)
		SELECT
			TRY_CONVERT(INT, C) AS Country_code,
			CASE
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') IS NULL THEN NULL
				WHEN CHARINDEX('.', TRIM(REPLACE(S, ',', '.'))) > 0 THEN TRIM(REPLACE(S, ',', '.'))
				WHEN LEN(TRIM(REPLACE(S, ',', '.'))) = 1 THEN TRIM(REPLACE(S, ',', '.'))
				ELSE STUFF(TRIM(REPLACE(S, ',', '.')), 2, 0, '.') 
			END AS Indicator_code,
			CASE 
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE '"The information%' THEN NULL
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'Source%' THEN NULL
			ELSE 
				CASE
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'COTE D' +  NCHAR(0x252C) + NCHAR(0x2524) + N'IVOIRE' THEN N'COTE D''IVOIRE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'BOLIVIA, PLURINATIONAL STATE OF' THEN 'BOLIVIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'CURA'+ NCHAR(0x251C) + N'çAO' THEN 'CURACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'HONG KONG, CHINA' THEN 'HONG KONG'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'KOREA, DEMOCRATIC PEOPLE%S REPUBLIC OF' THEN 'NORTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'KOREA, REPUBLIC OF' THEN 'SOUTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE  'LAO PEOPLE%S DEMOCRATIC REPUBLIC' THEN 'LAOS'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MACAO, CHINA' THEN 'MACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MICRONESIA, FEDERATED STATES OF' THEN 'MICRONESIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MOLDOVA, REPUBLIC OF' THEN 'MOLDOVA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SINT MAARTEN (DUTCH PART)' THEN 'SINT MAARTEN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TANZANIA, UNITED REPUBLIC OF' THEN 'TANZANIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TIMOR-LESTE' THEN 'TIMOR EST'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'VENEZUELA, BOLIVARIAN REPUBLIC OF' THEN 'VENEZUELA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'T%RKIYE' THEN 'TURKIYE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CZECH REPUBLIC (CZECHIA)' THEN 'CZECHIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CONGO, DEMOCRATIC REPUBLIC OF THE' THEN 'DEMOCRATIC REPUBLIC OF CONGO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'IRAN, ISLAMIC REPUBLIC OF' THEN 'IRAN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SYRIAN ARAB REPUBLIC' THEN 'SYRIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TAIWAN PROVINCE OF CHINA' THEN 'TAIWAN'
					ELSE NULLIF(TRIM(REPLACE(Basic_data,'..','')), '')
				END
			END AS Country,
			NULLIF(TRIM(Unnamed_5), '') AS Indicator_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Indicator_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Indicator_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Indicator_L4,
			NULLIF(TRIM(Units), '') AS Units,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1995, '..',''), ',', '')),'')) AS year_1995,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1996, '..',''), ',', '')),'')) AS year_1996,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1997, '..',''), ',', '')),'')) AS year_1997,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1998, '..',''), ',', '')),'')) AS year_1998,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1999, '..',''), ',', '')),'')) AS year_1999,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2000, '..',''), ',', '')),'')) AS year_2000,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2001, '..',''), ',', '')),'')) AS year_2001,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2002, '..',''), ',', '')),'')) AS year_2002,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2003, '..',''), ',', '')),'')) AS year_2003,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2004, '..',''), ',', '')),'')) AS year_2004,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2005, '..',''), ',', '')),'')) AS year_2005,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2006, '..',''), ',', '')),'')) AS year_2006,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2007, '..',''), ',', '')),'')) AS year_2007,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2008, '..',''), ',', '')),'')) AS year_2008,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2009, '..',''), ',', '')),'')) AS year_2009,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2010, '..',''), ',', '')),'')) AS year_2010,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2011, '..',''), ',', '')),'')) AS year_2011,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2012, '..',''), ',', '')),'')) AS year_2012,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2013, '..',''), ',', '')),'')) AS year_2013,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2014, '..',''), ',', '')),'')) AS year_2014,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2015, '..',''), ',', '')),'')) AS year_2015,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2016, '..',''), ',', '')),'')) AS year_2016,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2017, '..',''), ',', '')),'')) AS year_2017,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2018, '..',''), ',', '')),'')) AS year_2018,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2019, '..',''), ',', '')),'')) AS year_2019,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2020, '..',''), ',', '')),'')) AS year_2020,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2021, '..',''), ',', '')),'')) AS year_2021,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2022, '..',''), ',', '')),'')) AS year_2022
		FROM bronze.raw_outbound_departures;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

-- silver.tourism_industries
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.tourism_industries;
		INSERT INTO silver.tourism_industries
		(
			Country_code,
			Indicator_code,
			Country,
			Indicator_L1,
			Indicator_L2,
			Indicator_L3,
			Indicator_L4,
			Units,
			year_1995,
			year_1996,
			year_1997,
			year_1998,
			year_1999,
			year_2000,
			year_2001,
			year_2002,
			year_2003,
			year_2004,
			year_2005,
			year_2006,
			year_2007,
			year_2008,
			year_2009,
			year_2010,
			year_2011,
			year_2012,
			year_2013,
			year_2014,
			year_2015,
			year_2016,
			year_2017,
			year_2018,
			year_2019,
			year_2020,
			year_2021,
			year_2022
		)
		SELECT
			TRY_CONVERT(INT, C) AS Country_code,
			CASE
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') IS NULL THEN NULL
				WHEN CHARINDEX('.', TRIM(REPLACE(S, ',', '.'))) > 0 THEN TRIM(REPLACE(S, ',', '.'))
				WHEN LEN(TRIM(REPLACE(S, ',', '.'))) = 1 THEN TRIM(REPLACE(S, ',', '.'))
				ELSE STUFF(TRIM(REPLACE(S, ',', '.')), 2, 0, '.') 
			END AS Indicator_code,
			CASE 
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE '"The information%' THEN NULL
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'Source%' THEN NULL
			ELSE 
				CASE
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'COTE D' +  NCHAR(0x252C) + NCHAR(0x2524) + N'IVOIRE' THEN N'COTE D''IVOIRE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'BOLIVIA, PLURINATIONAL STATE OF' THEN 'BOLIVIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'CURA'+ NCHAR(0x251C) + N'çAO' THEN 'CURACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'HONG KONG, CHINA' THEN 'HONG KONG'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'KOREA, DEMOCRATIC PEOPLE%S REPUBLIC OF' THEN 'NORTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'KOREA, REPUBLIC OF' THEN 'SOUTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE  'LAO PEOPLE%S DEMOCRATIC REPUBLIC' THEN 'LAOS'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MACAO, CHINA' THEN 'MACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MICRONESIA, FEDERATED STATES OF' THEN 'MICRONESIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MOLDOVA, REPUBLIC OF' THEN 'MOLDOVA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SINT MAARTEN (DUTCH PART)' THEN 'SINT MAARTEN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TANZANIA, UNITED REPUBLIC OF' THEN 'TANZANIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TIMOR-LESTE' THEN 'TIMOR EST'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'VENEZUELA, BOLIVARIAN REPUBLIC OF' THEN 'VENEZUELA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'T%RKIYE' THEN 'TURKIYE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CZECH REPUBLIC (CZECHIA)' THEN 'CZECHIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CONGO, DEMOCRATIC REPUBLIC OF THE' THEN 'DEMOCRATIC REPUBLIC OF CONGO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'IRAN, ISLAMIC REPUBLIC OF' THEN 'IRAN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SYRIAN ARAB REPUBLIC' THEN 'SYRIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TAIWAN PROVINCE OF CHINA' THEN 'TAIWAN'
					ELSE NULLIF(TRIM(REPLACE(Basic_data,'..','')), '')
				END
			END AS Country,
			NULLIF(TRIM(Unnamed_5), '') AS Indicator_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Indicator_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Indicator_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Indicator_L4,
			NULLIF(TRIM(Units), '') AS Units,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1995, '..',''), ',', '')),'')) AS year_1995,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1996, '..',''), ',', '')),'')) AS year_1996,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1997, '..',''), ',', '')),'')) AS year_1997,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1998, '..',''), ',', '')),'')) AS year_1998,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1999, '..',''), ',', '')),'')) AS year_1999,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2000, '..',''), ',', '')),'')) AS year_2000,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2001, '..',''), ',', '')),'')) AS year_2001,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2002, '..',''), ',', '')),'')) AS year_2002,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2003, '..',''), ',', '')),'')) AS year_2003,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2004, '..',''), ',', '')),'')) AS year_2004,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2005, '..',''), ',', '')),'')) AS year_2005,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2006, '..',''), ',', '')),'')) AS year_2006,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2007, '..',''), ',', '')),'')) AS year_2007,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2008, '..',''), ',', '')),'')) AS year_2008,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2009, '..',''), ',', '')),'')) AS year_2009,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2010, '..',''), ',', '')),'')) AS year_2010,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2011, '..',''), ',', '')),'')) AS year_2011,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2012, '..',''), ',', '')),'')) AS year_2012,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2013, '..',''), ',', '')),'')) AS year_2013,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2014, '..',''), ',', '')),'')) AS year_2014,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2015, '..',''), ',', '')),'')) AS year_2015,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2016, '..',''), ',', '')),'')) AS year_2016,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2017, '..',''), ',', '')),'')) AS year_2017,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2018, '..',''), ',', '')),'')) AS year_2018,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2019, '..',''), ',', '')),'')) AS year_2019,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2020, '..',''), ',', '')),'')) AS year_2020,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2021, '..',''), ',', '')),'')) AS year_2021,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2022, '..',''), ',', '')),'')) AS year_2022
		FROM bronze.raw_tourism_industries;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '=========================================================';
		PRINT 'Load Silver (Group 2): inbound_arrivals, inbound_expenditure,';
		PRINT 'inbound_purpose, inbound_regions, inbound_transport, outbound_expenditure';
		PRINT 'Source: corresponding bronze raw tables (wide year_* format)';
		PRINT 'Cleaning: TRIM text; convert ''..''/'''' to NULL; remove thousands';
		PRINT 'separators (commas) preserving decimals; cast year_* to DECIMAL(18,2);';
		PRINT 'map C -> Country_code (INT), S -> Indicator_code (NVARCHAR(20));';
		PRINT 'extract/clean Series -> Series_method; standardize Country names.';
		PRINT 'Dropped: C_and_S, Notes, and unused Unnamed_* columns.';
		PRINT '=========================================================';


-- silver.inbound_arrivals
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.inbound_arrivals;
		INSERT INTO silver.inbound_arrivals
		(
			Country_code,
			Indicator_code,
			Country,
			Indicator_L1,
			Indicator_L2,
			Indicator_L3,
			Indicator_L4,
			Units,
			Series_method,
			year_1995,
			year_1996,
			year_1997,
			year_1998,
			year_1999,
			year_2000,
			year_2001,
			year_2002,
			year_2003,
			year_2004,
			year_2005,
			year_2006,
			year_2007,
			year_2008,
			year_2009,
			year_2010,
			year_2011,
			year_2012,
			year_2013,
			year_2014,
			year_2015,
			year_2016,
			year_2017,
			year_2018,
			year_2019,
			year_2020,
			year_2021,
			year_2022
		)
		SELECT
			TRY_CONVERT(INT, C) AS Country_code,
			CASE
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') IS NULL THEN NULL
				WHEN CHARINDEX('.', TRIM(REPLACE(S, ',', '.'))) > 0 THEN TRIM(REPLACE(S, ',', '.'))
				WHEN LEN(TRIM(REPLACE(S, ',', '.'))) = 1 THEN TRIM(REPLACE(S, ',', '.'))
				ELSE STUFF(TRIM(REPLACE(S, ',', '.')), 2, 0, '.') 
			END AS Indicator_code,
			CASE
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE '"The information%' THEN NULL
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'Source%' THEN NULL
			ELSE 
				CASE
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'COTE D' +  NCHAR(0x252C) + NCHAR(0x2524) + N'IVOIRE' THEN N'COTE D''IVOIRE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'BOLIVIA, PLURINATIONAL STATE OF' THEN 'BOLIVIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'CURA'+ NCHAR(0x251C) + N'çAO' THEN 'CURACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'HONG KONG, CHINA' THEN 'HONG KONG'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'KOREA, DEMOCRATIC PEOPLE%S REPUBLIC OF' THEN 'NORTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'KOREA, REPUBLIC OF' THEN 'SOUTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE  'LAO PEOPLE%S DEMOCRATIC REPUBLIC' THEN 'LAOS'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MACAO, CHINA' THEN 'MACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MICRONESIA, FEDERATED STATES OF' THEN 'MICRONESIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MOLDOVA, REPUBLIC OF' THEN 'MOLDOVA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SINT MAARTEN (DUTCH PART)' THEN 'SINT MAARTEN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TANZANIA, UNITED REPUBLIC OF' THEN 'TANZANIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TIMOR-LESTE' THEN 'TIMOR EST'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'VENEZUELA, BOLIVARIAN REPUBLIC OF' THEN 'VENEZUELA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'T%RKIYE' THEN 'TURKIYE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CZECH REPUBLIC (CZECHIA)' THEN 'CZECHIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CONGO, DEMOCRATIC REPUBLIC OF THE' THEN 'DEMOCRATIC REPUBLIC OF CONGO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'IRAN, ISLAMIC REPUBLIC OF' THEN 'IRAN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SYRIAN ARAB REPUBLIC' THEN 'SYRIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TAIWAN PROVINCE OF CHINA' THEN 'TAIWAN'
					ELSE NULLIF(TRIM(REPLACE(Basic_data,'..','')), '')
				END
			END AS Country,
			NULLIF(TRIM(Unnamed_5), '') AS Indicator_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Indicator_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Indicator_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Indicator_L4,
			NULLIF(TRIM(Units), '') AS Units,
			NULLIF(TRIM(REPLACE(Series, '..','')), '') AS Series_method,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1995, '..',''), ',', '')),'')) AS year_1995,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1996, '..',''), ',', '')),'')) AS year_1996,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1997, '..',''), ',', '')),'')) AS year_1997,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1998, '..',''), ',', '')),'')) AS year_1998,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1999, '..',''), ',', '')),'')) AS year_1999,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2000, '..',''), ',', '')),'')) AS year_2000,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2001, '..',''), ',', '')),'')) AS year_2001,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2002, '..',''), ',', '')),'')) AS year_2002,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2003, '..',''), ',', '')),'')) AS year_2003,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2004, '..',''), ',', '')),'')) AS year_2004,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2005, '..',''), ',', '')),'')) AS year_2005,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2006, '..',''), ',', '')),'')) AS year_2006,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2007, '..',''), ',', '')),'')) AS year_2007,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2008, '..',''), ',', '')),'')) AS year_2008,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2009, '..',''), ',', '')),'')) AS year_2009,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2010, '..',''), ',', '')),'')) AS year_2010,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2011, '..',''), ',', '')),'')) AS year_2011,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2012, '..',''), ',', '')),'')) AS year_2012,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2013, '..',''), ',', '')),'')) AS year_2013,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2014, '..',''), ',', '')),'')) AS year_2014,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2015, '..',''), ',', '')),'')) AS year_2015,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2016, '..',''), ',', '')),'')) AS year_2016,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2017, '..',''), ',', '')),'')) AS year_2017,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2018, '..',''), ',', '')),'')) AS year_2018,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2019, '..',''), ',', '')),'')) AS year_2019,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2020, '..',''), ',', '')),'')) AS year_2020,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2021, '..',''), ',', '')),'')) AS year_2021,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2022, '..',''), ',', '')),'')) AS year_2022
		FROM bronze.raw_inbound_arrivals;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

-- silver.inbound_expenditure

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.inbound_expenditure;
		INSERT INTO silver.inbound_expenditure
		(
			Country_code,
			Indicator_code,
			Country,
			Indicator_L1,
			Indicator_L2,
			Indicator_L3,
			Indicator_L4,
			Units,
			Series_method,
			year_1995,
			year_1996,
			year_1997,
			year_1998,
			year_1999,
			year_2000,
			year_2001,
			year_2002,
			year_2003,
			year_2004,
			year_2005,
			year_2006,
			year_2007,
			year_2008,
			year_2009,
			year_2010,
			year_2011,
			year_2012,
			year_2013,
			year_2014,
			year_2015,
			year_2016,
			year_2017,
			year_2018,
			year_2019,
			year_2020,
			year_2021,
			year_2022
		)
		SELECT
			TRY_CONVERT(INT, C) AS Country_code,
			CASE
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') IS NULL THEN NULL
				WHEN CHARINDEX('.', TRIM(REPLACE(S, ',', '.'))) > 0 THEN TRIM(REPLACE(S, ',', '.'))
				WHEN LEN(TRIM(REPLACE(S, ',', '.'))) = 1 THEN TRIM(REPLACE(S, ',', '.'))
				ELSE STUFF(TRIM(REPLACE(S, ',', '.')), 2, 0, '.') 
			END AS Indicator_code,
			CASE 
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE '"The information%' THEN NULL
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'Source%' THEN NULL
			ELSE 
				CASE
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'COTE D' +  NCHAR(0x252C) + NCHAR(0x2524) + N'IVOIRE' THEN N'COTE D''IVOIRE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'BOLIVIA, PLURINATIONAL STATE OF' THEN 'BOLIVIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'CURA'+ NCHAR(0x251C) + N'çAO' THEN 'CURACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'HONG KONG, CHINA' THEN 'HONG KONG'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'KOREA, DEMOCRATIC PEOPLE%S REPUBLIC OF' THEN 'NORTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'KOREA, REPUBLIC OF' THEN 'SOUTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE  'LAO PEOPLE%S DEMOCRATIC REPUBLIC' THEN 'LAOS'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MACAO, CHINA' THEN 'MACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MICRONESIA, FEDERATED STATES OF' THEN 'MICRONESIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MOLDOVA, REPUBLIC OF' THEN 'MOLDOVA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SINT MAARTEN (DUTCH PART)' THEN 'SINT MAARTEN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TANZANIA, UNITED REPUBLIC OF' THEN 'TANZANIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TIMOR-LESTE' THEN 'TIMOR EST'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'VENEZUELA, BOLIVARIAN REPUBLIC OF' THEN 'VENEZUELA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'T%RKIYE' THEN 'TURKIYE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CZECH REPUBLIC (CZECHIA)' THEN 'CZECHIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CONGO, DEMOCRATIC REPUBLIC OF THE' THEN 'DEMOCRATIC REPUBLIC OF CONGO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'IRAN, ISLAMIC REPUBLIC OF' THEN 'IRAN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SYRIAN ARAB REPUBLIC' THEN 'SYRIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TAIWAN PROVINCE OF CHINA' THEN 'TAIWAN'
					ELSE NULLIF(TRIM(REPLACE(Basic_data,'..','')), '')
				END
			END AS Country,
			NULLIF(TRIM(Unnamed_5), '') AS Indicator_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Indicator_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Indicator_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Indicator_L4,
			NULLIF(TRIM(Units), '') AS Units,
			NULLIF(TRIM(REPLACE(Series, '..','')), '') AS Series_method,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1995, '..',''), ',', '')),'')) AS year_1995,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1996, '..',''), ',', '')),'')) AS year_1996,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1997, '..',''), ',', '')),'')) AS year_1997,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1998, '..',''), ',', '')),'')) AS year_1998,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1999, '..',''), ',', '')),'')) AS year_1999,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2000, '..',''), ',', '')),'')) AS year_2000,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2001, '..',''), ',', '')),'')) AS year_2001,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2002, '..',''), ',', '')),'')) AS year_2002,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2003, '..',''), ',', '')),'')) AS year_2003,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2004, '..',''), ',', '')),'')) AS year_2004,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2005, '..',''), ',', '')),'')) AS year_2005,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2006, '..',''), ',', '')),'')) AS year_2006,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2007, '..',''), ',', '')),'')) AS year_2007,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2008, '..',''), ',', '')),'')) AS year_2008,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2009, '..',''), ',', '')),'')) AS year_2009,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2010, '..',''), ',', '')),'')) AS year_2010,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2011, '..',''), ',', '')),'')) AS year_2011,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2012, '..',''), ',', '')),'')) AS year_2012,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2013, '..',''), ',', '')),'')) AS year_2013,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2014, '..',''), ',', '')),'')) AS year_2014,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2015, '..',''), ',', '')),'')) AS year_2015,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2016, '..',''), ',', '')),'')) AS year_2016,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2017, '..',''), ',', '')),'')) AS year_2017,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2018, '..',''), ',', '')),'')) AS year_2018,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2019, '..',''), ',', '')),'')) AS year_2019,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2020, '..',''), ',', '')),'')) AS year_2020,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2021, '..',''), ',', '')),'')) AS year_2021,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2022, '..',''), ',', '')),'')) AS year_2022
		FROM bronze.raw_inbound_expenditure;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

-- silver.inbound_purpose

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.inbound_purpose;
		INSERT INTO silver.inbound_purpose
		(
			Country_code,
			Indicator_code,
			Country,
			Indicator_L1,
			Indicator_L2,
			Indicator_L3,
			Indicator_L4,
			Units,
			Series_method,
			year_1995,
			year_1996,
			year_1997,
			year_1998,
			year_1999,
			year_2000,
			year_2001,
			year_2002,
			year_2003,
			year_2004,
			year_2005,
			year_2006,
			year_2007,
			year_2008,
			year_2009,
			year_2010,
			year_2011,
			year_2012,
			year_2013,
			year_2014,
			year_2015,
			year_2016,
			year_2017,
			year_2018,
			year_2019,
			year_2020,
			year_2021,
			year_2022
		)
		SELECT
			TRY_CONVERT(INT, C) AS Country_code,
			CASE
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') IS NULL THEN NULL
				WHEN CHARINDEX('.', TRIM(REPLACE(S, ',', '.'))) > 0 THEN TRIM(REPLACE(S, ',', '.'))
				WHEN LEN(TRIM(REPLACE(S, ',', '.'))) = 1 THEN TRIM(REPLACE(S, ',', '.'))
				ELSE STUFF(TRIM(REPLACE(S, ',', '.')), 2, 0, '.') 
			END AS Indicator_code,
			CASE 
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE '"The information%' THEN NULL
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'Source%' THEN NULL
			ELSE 
				CASE
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'COTE D' +  NCHAR(0x252C) + NCHAR(0x2524) + N'IVOIRE' THEN N'COTE D''IVOIRE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'BOLIVIA, PLURINATIONAL STATE OF' THEN 'BOLIVIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'CURA'+ NCHAR(0x251C) + N'çAO' THEN 'CURACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'HONG KONG, CHINA' THEN 'HONG KONG'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'KOREA, DEMOCRATIC PEOPLE%S REPUBLIC OF' THEN 'NORTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'KOREA, REPUBLIC OF' THEN 'SOUTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE  'LAO PEOPLE%S DEMOCRATIC REPUBLIC' THEN 'LAOS'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MACAO, CHINA' THEN 'MACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MICRONESIA, FEDERATED STATES OF' THEN 'MICRONESIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MOLDOVA, REPUBLIC OF' THEN 'MOLDOVA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SINT MAARTEN (DUTCH PART)' THEN 'SINT MAARTEN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TANZANIA, UNITED REPUBLIC OF' THEN 'TANZANIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TIMOR-LESTE' THEN 'TIMOR EST'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'VENEZUELA, BOLIVARIAN REPUBLIC OF' THEN 'VENEZUELA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'T%RKIYE' THEN 'TURKIYE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CZECH REPUBLIC (CZECHIA)' THEN 'CZECHIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CONGO, DEMOCRATIC REPUBLIC OF THE' THEN 'DEMOCRATIC REPUBLIC OF CONGO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'IRAN, ISLAMIC REPUBLIC OF' THEN 'IRAN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SYRIAN ARAB REPUBLIC' THEN 'SYRIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TAIWAN PROVINCE OF CHINA' THEN 'TAIWAN'
					ELSE NULLIF(TRIM(REPLACE(Basic_data,'..','')), '')
				END
			END AS Country,
			NULLIF(TRIM(Unnamed_5), '') AS Indicator_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Indicator_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Indicator_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Indicator_L4,
			NULLIF(TRIM(Units), '') AS Units,
			NULLIF(TRIM(REPLACE(Series, '..','')), '') AS Series_method,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1995, '..',''), ',', '')),'')) AS year_1995,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1996, '..',''), ',', '')),'')) AS year_1996,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1997, '..',''), ',', '')),'')) AS year_1997,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1998, '..',''), ',', '')),'')) AS year_1998,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1999, '..',''), ',', '')),'')) AS year_1999,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2000, '..',''), ',', '')),'')) AS year_2000,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2001, '..',''), ',', '')),'')) AS year_2001,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2002, '..',''), ',', '')),'')) AS year_2002,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2003, '..',''), ',', '')),'')) AS year_2003,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2004, '..',''), ',', '')),'')) AS year_2004,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2005, '..',''), ',', '')),'')) AS year_2005,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2006, '..',''), ',', '')),'')) AS year_2006,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2007, '..',''), ',', '')),'')) AS year_2007,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2008, '..',''), ',', '')),'')) AS year_2008,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2009, '..',''), ',', '')),'')) AS year_2009,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2010, '..',''), ',', '')),'')) AS year_2010,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2011, '..',''), ',', '')),'')) AS year_2011,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2012, '..',''), ',', '')),'')) AS year_2012,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2013, '..',''), ',', '')),'')) AS year_2013,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2014, '..',''), ',', '')),'')) AS year_2014,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2015, '..',''), ',', '')),'')) AS year_2015,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2016, '..',''), ',', '')),'')) AS year_2016,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2017, '..',''), ',', '')),'')) AS year_2017,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2018, '..',''), ',', '')),'')) AS year_2018,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2019, '..',''), ',', '')),'')) AS year_2019,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2020, '..',''), ',', '')),'')) AS year_2020,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2021, '..',''), ',', '')),'')) AS year_2021,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2022, '..',''), ',', '')),'')) AS year_2022
		FROM bronze.raw_inbound_purpose;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

-- silver.inbound_regions

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.inbound_regions;
		INSERT INTO silver.inbound_regions
		(
			Country_code,
			Indicator_code,
			Country,
			Indicator_L1,
			Indicator_L2,
			Indicator_L3,
			Indicator_L4,
			Units,
			Series_method,
			year_1995,
			year_1996,
			year_1997,
			year_1998,
			year_1999,
			year_2000,
			year_2001,
			year_2002,
			year_2003,
			year_2004,
			year_2005,
			year_2006,
			year_2007,
			year_2008,
			year_2009,
			year_2010,
			year_2011,
			year_2012,
			year_2013,
			year_2014,
			year_2015,
			year_2016,
			year_2017,
			year_2018,
			year_2019,
			year_2020,
			year_2021,
			year_2022
		)
		SELECT
			TRY_CONVERT(INT, C) AS Country_code,
			CASE
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') IS NULL THEN NULL
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') = '1.1' THEN '1.10'
				WHEN CHARINDEX('.', TRIM(REPLACE(S, ',', '.'))) > 0 THEN TRIM(REPLACE(S, ',', '.'))
				WHEN LEN(TRIM(REPLACE(S, ',', '.'))) = 1 THEN TRIM(REPLACE(S, ',', '.'))
				ELSE STUFF(TRIM(REPLACE(S, ',', '.')), 2, 0, '.') 
			END AS Indicator_code,
			CASE 
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE '"The information%' THEN NULL
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'Source%' THEN NULL
			ELSE 
				CASE
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'COTE D' +  NCHAR(0x252C) + NCHAR(0x2524) + N'IVOIRE' THEN N'COTE D''IVOIRE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'BOLIVIA, PLURINATIONAL STATE OF' THEN 'BOLIVIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'CURA'+ NCHAR(0x251C) + N'çAO' THEN 'CURACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'HONG KONG, CHINA' THEN 'HONG KONG'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'KOREA, DEMOCRATIC PEOPLE%S REPUBLIC OF' THEN 'NORTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'KOREA, REPUBLIC OF' THEN 'SOUTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE  'LAO PEOPLE%S DEMOCRATIC REPUBLIC' THEN 'LAOS'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MACAO, CHINA' THEN 'MACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MICRONESIA, FEDERATED STATES OF' THEN 'MICRONESIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MOLDOVA, REPUBLIC OF' THEN 'MOLDOVA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SINT MAARTEN (DUTCH PART)' THEN 'SINT MAARTEN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TANZANIA, UNITED REPUBLIC OF' THEN 'TANZANIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TIMOR-LESTE' THEN 'TIMOR EST'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'VENEZUELA, BOLIVARIAN REPUBLIC OF' THEN 'VENEZUELA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'T%RKIYE' THEN 'TURKIYE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CZECH REPUBLIC (CZECHIA)' THEN 'CZECHIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CONGO, DEMOCRATIC REPUBLIC OF THE' THEN 'DEMOCRATIC REPUBLIC OF CONGO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'IRAN, ISLAMIC REPUBLIC OF' THEN 'IRAN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SYRIAN ARAB REPUBLIC' THEN 'SYRIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TAIWAN PROVINCE OF CHINA' THEN 'TAIWAN'
					ELSE NULLIF(TRIM(REPLACE(Basic_data,'..','')), '')
				END
			END AS Country,
			NULLIF(TRIM(Unnamed_5), '') AS Indicator_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Indicator_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Indicator_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Indicator_L4,
			NULLIF(TRIM(Units), '') AS Units,
			NULLIF(TRIM(REPLACE(Series, '..','')), '') AS Series_method,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1995, '..',''), ',', '')),'')) AS year_1995,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1996, '..',''), ',', '')),'')) AS year_1996,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1997, '..',''), ',', '')),'')) AS year_1997,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1998, '..',''), ',', '')),'')) AS year_1998,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1999, '..',''), ',', '')),'')) AS year_1999,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2000, '..',''), ',', '')),'')) AS year_2000,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2001, '..',''), ',', '')),'')) AS year_2001,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2002, '..',''), ',', '')),'')) AS year_2002,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2003, '..',''), ',', '')),'')) AS year_2003,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2004, '..',''), ',', '')),'')) AS year_2004,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2005, '..',''), ',', '')),'')) AS year_2005,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2006, '..',''), ',', '')),'')) AS year_2006,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2007, '..',''), ',', '')),'')) AS year_2007,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2008, '..',''), ',', '')),'')) AS year_2008,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2009, '..',''), ',', '')),'')) AS year_2009,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2010, '..',''), ',', '')),'')) AS year_2010,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2011, '..',''), ',', '')),'')) AS year_2011,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2012, '..',''), ',', '')),'')) AS year_2012,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2013, '..',''), ',', '')),'')) AS year_2013,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2014, '..',''), ',', '')),'')) AS year_2014,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2015, '..',''), ',', '')),'')) AS year_2015,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2016, '..',''), ',', '')),'')) AS year_2016,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2017, '..',''), ',', '')),'')) AS year_2017,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2018, '..',''), ',', '')),'')) AS year_2018,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2019, '..',''), ',', '')),'')) AS year_2019,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2020, '..',''), ',', '')),'')) AS year_2020,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2021, '..',''), ',', '')),'')) AS year_2021,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2022, '..',''), ',', '')),'')) AS year_2022
		FROM bronze.raw_inbound_regions;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

-- silver.inbound_transport

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.inbound_transport;
		INSERT INTO silver.inbound_transport
		(
			Country_code,
			Indicator_code,
			Country,
			Indicator_L1,
			Indicator_L2,
			Indicator_L3,
			Indicator_L4,
			Units,
			Series_method,
			year_1995,
			year_1996,
			year_1997,
			year_1998,
			year_1999,
			year_2000,
			year_2001,
			year_2002,
			year_2003,
			year_2004,
			year_2005,
			year_2006,
			year_2007,
			year_2008,
			year_2009,
			year_2010,
			year_2011,
			year_2012,
			year_2013,
			year_2014,
			year_2015,
			year_2016,
			year_2017,
			year_2018,
			year_2019,
			year_2020,
			year_2021,
			year_2022
		)
		SELECT
			TRY_CONVERT(INT, C) AS Country_code,
			CASE
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') IS NULL THEN NULL
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') = '12' THEN '1.20'
				WHEN CHARINDEX('.', TRIM(REPLACE(S, ',', '.'))) > 0 THEN TRIM(REPLACE(S, ',', '.'))
				WHEN LEN(TRIM(REPLACE(S, ',', '.'))) = 1 THEN TRIM(REPLACE(S, ',', '.'))
				ELSE STUFF(TRIM(REPLACE(S, ',', '.')), 2, 0, '.') 
			END AS Indicator_code,
			CASE 
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE '"The information%' THEN NULL
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'Source%' THEN NULL
			ELSE 
				CASE
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'COTE D' +  NCHAR(0x252C) + NCHAR(0x2524) + N'IVOIRE' THEN N'COTE D''IVOIRE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'BOLIVIA, PLURINATIONAL STATE OF' THEN 'BOLIVIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'CURA'+ NCHAR(0x251C) + N'çAO' THEN 'CURACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'HONG KONG, CHINA' THEN 'HONG KONG'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'KOREA, DEMOCRATIC PEOPLE%S REPUBLIC OF' THEN 'NORTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'KOREA, REPUBLIC OF' THEN 'SOUTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE  'LAO PEOPLE%S DEMOCRATIC REPUBLIC' THEN 'LAOS'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MACAO, CHINA' THEN 'MACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MICRONESIA, FEDERATED STATES OF' THEN 'MICRONESIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MOLDOVA, REPUBLIC OF' THEN 'MOLDOVA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SINT MAARTEN (DUTCH PART)' THEN 'SINT MAARTEN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TANZANIA, UNITED REPUBLIC OF' THEN 'TANZANIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TIMOR-LESTE' THEN 'TIMOR EST'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'VENEZUELA, BOLIVARIAN REPUBLIC OF' THEN 'VENEZUELA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'T%RKIYE' THEN 'TURKIYE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CZECH REPUBLIC (CZECHIA)' THEN 'CZECHIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CONGO, DEMOCRATIC REPUBLIC OF THE' THEN 'DEMOCRATIC REPUBLIC OF CONGO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'IRAN, ISLAMIC REPUBLIC OF' THEN 'IRAN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SYRIAN ARAB REPUBLIC' THEN 'SYRIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TAIWAN PROVINCE OF CHINA' THEN 'TAIWAN'
					ELSE NULLIF(TRIM(REPLACE(Basic_data,'..','')), '')
				END
			END AS Country,
			NULLIF(TRIM(Unnamed_5), '') AS Indicator_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Indicator_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Indicator_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Indicator_L4,
			NULLIF(TRIM(Units), '') AS Units,
			NULLIF(TRIM(REPLACE(Series, '..','')), '') AS Series_method,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1995, '..',''), ',', '')),'')) AS year_1995,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1996, '..',''), ',', '')),'')) AS year_1996,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1997, '..',''), ',', '')),'')) AS year_1997,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1998, '..',''), ',', '')),'')) AS year_1998,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1999, '..',''), ',', '')),'')) AS year_1999,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2000, '..',''), ',', '')),'')) AS year_2000,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2001, '..',''), ',', '')),'')) AS year_2001,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2002, '..',''), ',', '')),'')) AS year_2002,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2003, '..',''), ',', '')),'')) AS year_2003,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2004, '..',''), ',', '')),'')) AS year_2004,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2005, '..',''), ',', '')),'')) AS year_2005,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2006, '..',''), ',', '')),'')) AS year_2006,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2007, '..',''), ',', '')),'')) AS year_2007,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2008, '..',''), ',', '')),'')) AS year_2008,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2009, '..',''), ',', '')),'')) AS year_2009,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2010, '..',''), ',', '')),'')) AS year_2010,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2011, '..',''), ',', '')),'')) AS year_2011,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2012, '..',''), ',', '')),'')) AS year_2012,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2013, '..',''), ',', '')),'')) AS year_2013,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2014, '..',''), ',', '')),'')) AS year_2014,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2015, '..',''), ',', '')),'')) AS year_2015,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2016, '..',''), ',', '')),'')) AS year_2016,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2017, '..',''), ',', '')),'')) AS year_2017,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2018, '..',''), ',', '')),'')) AS year_2018,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2019, '..',''), ',', '')),'')) AS year_2019,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2020, '..',''), ',', '')),'')) AS year_2020,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2021, '..',''), ',', '')),'')) AS year_2021,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2022, '..',''), ',', '')),'')) AS year_2022
		FROM bronze.raw_inbound_transport;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
 
 -- silver.outbound_expenditure
 
 	    SET @start_time = GETDATE();
		TRUNCATE TABLE silver.outbound_expenditure;
		INSERT INTO silver.outbound_expenditure
		(
			Country_code,
			Indicator_code,
			Country,
			Indicator_L1,
			Indicator_L2,
			Indicator_L3,
			Indicator_L4,
			Units,
			Series_method,
			year_1995,
			year_1996,
			year_1997,
			year_1998,
			year_1999,
			year_2000,
			year_2001,
			year_2002,
			year_2003,
			year_2004,
			year_2005,
			year_2006,
			year_2007,
			year_2008,
			year_2009,
			year_2010,
			year_2011,
			year_2012,
			year_2013,
			year_2014,
			year_2015,
			year_2016,
			year_2017,
			year_2018,
			year_2019,
			year_2020,
			year_2021,
			year_2022
		)
		SELECT
			TRY_CONVERT(INT, C) AS Country_code,
			CASE
				WHEN NULLIF(TRIM(REPLACE(S, ',', '.')), '') IS NULL THEN NULL
				WHEN CHARINDEX('.', TRIM(REPLACE(S, ',', '.'))) > 0 THEN TRIM(REPLACE(S, ',', '.'))
				WHEN LEN(TRIM(REPLACE(S, ',', '.'))) = 1 THEN TRIM(REPLACE(S, ',', '.'))
				ELSE STUFF(TRIM(REPLACE(S, ',', '.')), 2, 0, '.') 
			END AS Indicator_code,
			CASE 
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE '"The information%' THEN NULL
				WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'Source%' THEN NULL
			ELSE 
				CASE
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'COTE D' +  NCHAR(0x252C) + NCHAR(0x2524) + N'IVOIRE' THEN N'COTE D''IVOIRE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'BOLIVIA, PLURINATIONAL STATE OF' THEN 'BOLIVIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = N'CURA'+ NCHAR(0x251C) + N'çAO' THEN 'CURACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'HONG KONG, CHINA' THEN 'HONG KONG'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'KOREA, DEMOCRATIC PEOPLE%S REPUBLIC OF' THEN 'NORTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'KOREA, REPUBLIC OF' THEN 'SOUTH KOREA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE  'LAO PEOPLE%S DEMOCRATIC REPUBLIC' THEN 'LAOS'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MACAO, CHINA' THEN 'MACAO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MICRONESIA, FEDERATED STATES OF' THEN 'MICRONESIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'MOLDOVA, REPUBLIC OF' THEN 'MOLDOVA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SINT MAARTEN (DUTCH PART)' THEN 'SINT MAARTEN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TANZANIA, UNITED REPUBLIC OF' THEN 'TANZANIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TIMOR-LESTE' THEN 'TIMOR EST'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'VENEZUELA, BOLIVARIAN REPUBLIC OF' THEN 'VENEZUELA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') LIKE 'T%RKIYE' THEN 'TURKIYE'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CZECH REPUBLIC (CZECHIA)' THEN 'CZECHIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'CONGO, DEMOCRATIC REPUBLIC OF THE' THEN 'DEMOCRATIC REPUBLIC OF CONGO'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'IRAN, ISLAMIC REPUBLIC OF' THEN 'IRAN'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'SYRIAN ARAB REPUBLIC' THEN 'SYRIA'
					WHEN NULLIF(TRIM(REPLACE(Basic_data,'..','')), '') = 'TAIWAN PROVINCE OF CHINA' THEN 'TAIWAN'
					ELSE NULLIF(TRIM(REPLACE(Basic_data,'..','')), '')
				END
			END AS Country,
			NULLIF(TRIM(Unnamed_5), '') AS Indicator_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Indicator_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Indicator_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Indicator_L4,
			NULLIF(TRIM(Units), '') AS Units,
			NULLIF(TRIM(REPLACE(Series, '..','')), '') AS Series_method,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1995, '..',''), ',', '')),'')) AS year_1995,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1996, '..',''), ',', '')),'')) AS year_1996,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1997, '..',''), ',', '')),'')) AS year_1997,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1998, '..',''), ',', '')),'')) AS year_1998,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_1999, '..',''), ',', '')),'')) AS year_1999,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2000, '..',''), ',', '')),'')) AS year_2000,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2001, '..',''), ',', '')),'')) AS year_2001,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2002, '..',''), ',', '')),'')) AS year_2002,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2003, '..',''), ',', '')),'')) AS year_2003,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2004, '..',''), ',', '')),'')) AS year_2004,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2005, '..',''), ',', '')),'')) AS year_2005,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2006, '..',''), ',', '')),'')) AS year_2006,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2007, '..',''), ',', '')),'')) AS year_2007,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2008, '..',''), ',', '')),'')) AS year_2008,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2009, '..',''), ',', '')),'')) AS year_2009,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2010, '..',''), ',', '')),'')) AS year_2010,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2011, '..',''), ',', '')),'')) AS year_2011,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2012, '..',''), ',', '')),'')) AS year_2012,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2013, '..',''), ',', '')),'')) AS year_2013,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2014, '..',''), ',', '')),'')) AS year_2014,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2015, '..',''), ',', '')),'')) AS year_2015,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2016, '..',''), ',', '')),'')) AS year_2016,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2017, '..',''), ',', '')),'')) AS year_2017,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2018, '..',''), ',', '')),'')) AS year_2018,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2019, '..',''), ',', '')),'')) AS year_2019,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2020, '..',''), ',', '')),'')) AS year_2020,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2021, '..',''), ',', '')),'')) AS year_2021,
			TRY_CONVERT(DECIMAL(18, 2), NULLIF(TRIM(REPLACE(REPLACE(year_2022, '..',''), ',', '')),'')) AS year_2022
		FROM bronze.raw_outbound_expenditure;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		PRINT '=========================================================';
		PRINT 'Load Silver (Group 3): sdg_891, sdg_892, sdg_12b1';
		PRINT 'Source: corresponding bronze raw tables';
		PRINT 'Cleaning: TRIM text; convert TimePeriod (YYYY) -> DATE (YYYY-01-01);';
		PRINT 'cast Value to DECIMAL(18,2); handle NULLs and placeholder values;';
		PRINT 'standardize Country_code (INT) and Geo_Area_Name.';
		PRINT 'SPECIAL NOTE: Remap conflicting Country_code values across SDG tables';
		PRINT '    * sdg_891 & sdg_892: 534→663, 535→658, 531→535, 276→280, 231→230';
		PRINT '    * sdg_12b1:          534→663, 535→658, 531→535, 276→280, 231→288';
		PRINT '      (plus name fix: ETHIOPIA → GHANA to align with 231→288 remap)';
		PRINT 'Dropped: INDEX, SDG_Indicator, SeriesCode, Source';
		PRINT '=========================================================';

-- silver.sdg_891

        SET @start_time = GETDATE();
		TRUNCATE TABLE silver.sdg_891;
		INSERT INTO silver.sdg_891
		(
			Country_code,
			Country,
			Time_Period,
			Value,
			SeriesDescription,
			Nature,
			Units
		)
		SELECT
		CASE
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 534 THEN 663
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 535 THEN 658
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 531 THEN 535
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 276 THEN 280
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 231 THEN 230
			ELSE TRY_CONVERT(INT, GeoAreaCode)
		END Country_code,
		CASE
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'C%TE D''IVOIRE' THEN 'COTE D''IVOIRE'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'CHINA, HONG KONG SPECIAL ADMINISTRATIVE REGION' THEN 'HONG KONG'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'CHINA, MACAO SPECIAL ADMINISTRATIVE REGION' THEN 'MACAO'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'MICRONESIA (FEDERATED STATES OF)' THEN 'MICRONESIA'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'NETHERLANDS (KINGDOM OF THE)' THEN 'NETHERLANDS'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'R%UNION' THEN 'REUNION'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'T%RKIYE' THEN 'TURKIYE'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND' THEN 'UNITED KINGDOM'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'REPUBLIC OF KOREA' THEN 'SOUTH KOREA'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'DEMOCRATIC REPUBLIC OF THE CONGO' THEN 'DEMOCRATIC REPUBLIC OF CONGO'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'BOLIVIA (PLURINATIONAL STATE OF)' THEN 'BOLIVIA'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'BONAIRE, SINT EUSTATIUS AND SABA' THEN 'SINT EUSTATIUS'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'CURA%AO' THEN 'CURACAO'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'IRAN (ISLAMIC REPUBLIC OF)' THEN 'IRAN'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'LAO PEOPLE%S DEMOCRATIC REPUBLIC' THEN 'LAOS'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'SINT MAARTEN (DUTCH PART)' THEN 'SINT MAARTEN'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'TIMOR-LESTE' THEN 'TIMOR EST'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'SYRIAN ARAB REPUBLIC' THEN 'SYRIA'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'REPUBLIC OF MOLDOVA' THEN 'MOLDOVA'
			ELSE NULLIF(TRIM(UPPER(GeoAreaName)), '')
		END AS Country,	
			DATEFROMPARTS(TRY_CONVERT(INT, TimePeriod), 1,1) AS Time_Period,
			TRY_CONVERT(DECIMAL(18,2), Value) AS Value,
			NULLIF(TRIM(SeriesDescription), '') AS SeriesDescription,
			NULLIF(TRIM(Nature), '') AS Nature,
			NULLIF(TRIM(Units), '') AS Units
		FROM bronze.raw_sdg_891;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

-- silver.sdg_892
     
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.sdg_892;
		INSERT INTO silver.sdg_892
		(
			Country_code,
			Country,
			Time_Period,
			Value,
			SeriesDescription,
			Nature,
			Units
		)
		SELECT
		CASE
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 534 THEN 663
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 535 THEN 658
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 531 THEN 535
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 276 THEN 280
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 231 THEN 230
			ELSE TRY_CONVERT(INT, GeoAreaCode)
		END Country_code,
		CASE
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'C%TE D''IVOIRE' THEN 'COTE D''IVOIRE'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'CHINA, HONG KONG SPECIAL ADMINISTRATIVE REGION' THEN 'HONG KONG'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'CHINA, MACAO SPECIAL ADMINISTRATIVE REGION' THEN 'MACAO'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'MICRONESIA (FEDERATED STATES OF)' THEN 'MICRONESIA'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'NETHERLANDS (KINGDOM OF THE)' THEN 'NETHERLANDS'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'R%UNION' THEN 'REUNION'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'T%RKIYE' THEN 'TURKIYE'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND' THEN 'UNITED KINGDOM'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'REPUBLIC OF KOREA' THEN 'SOUTH KOREA'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'DEMOCRATIC REPUBLIC OF THE CONGO' THEN 'DEMOCRATIC REPUBLIC OF CONGO'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'BOLIVIA (PLURINATIONAL STATE OF)' THEN 'BOLIVIA'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'BONAIRE, SINT EUSTATIUS AND SABA' THEN 'SINT EUSTATIUS'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'CURA%AO' THEN 'CURACAO'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'IRAN (ISLAMIC REPUBLIC OF)' THEN 'IRAN'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'LAO PEOPLE%S DEMOCRATIC REPUBLIC' THEN 'LAOS'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'SINT MAARTEN (DUTCH PART)' THEN 'SINT MAARTEN'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'TIMOR-LESTE' THEN 'TIMOR EST'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'SYRIAN ARAB REPUBLIC' THEN 'SYRIA'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'REPUBLIC OF MOLDOVA' THEN 'MOLDOVA'
			ELSE NULLIF(TRIM(UPPER(GeoAreaName)), '')
		END AS Country,			
			DATEFROMPARTS(TRY_CONVERT(INT, TimePeriod), 1,1) AS Time_Period,
			TRY_CONVERT(DECIMAL(18,2), Value) AS Value,
			NULLIF(TRIM(SeriesDescription), '') AS SeriesDescription,
			NULLIF(TRIM(Nature), '') AS Nature,
			NULLIF(TRIM(Units), '') AS Units
		FROM bronze.raw_sdg_892;

-- silver.sdg_12b1

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
        
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.sdg_12b1;
		INSERT INTO silver.sdg_12b1
		(
			Country_code,
			Country,
			Time_Period,
			Value,
			SeriesDescription,
			Nature,
			Units
		)
		SELECT
		CASE
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 534 THEN 663
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 535 THEN 658
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 531 THEN 535
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 276 THEN 280
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 231 THEN 288
			ELSE TRY_CONVERT(INT, GeoAreaCode)
		END Country_code,
		CASE
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'C%TE D''IVOIRE' THEN 'COTE D''IVOIRE'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'CHINA, HONG KONG SPECIAL ADMINISTRATIVE REGION' THEN 'HONG KONG'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'CHINA, MACAO SPECIAL ADMINISTRATIVE REGION' THEN 'MACAO'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'MICRONESIA (FEDERATED STATES OF)' THEN 'MICRONESIA'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'NETHERLANDS (KINGDOM OF THE)' THEN 'NETHERLANDS'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'R%UNION' THEN 'REUNION'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'T%RKIYE' THEN 'TURKIYE'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND' THEN 'UNITED KINGDOM'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'REPUBLIC OF KOREA' THEN 'SOUTH KOREA'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'DEMOCRATIC REPUBLIC OF THE CONGO' THEN 'DEMOCRATIC REPUBLIC OF CONGO'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'BOLIVIA (PLURINATIONAL STATE OF)' THEN 'BOLIVIA'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'BONAIRE, SINT EUSTATIUS AND SABA' THEN 'SINT EUSTATIUS'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'CURA%AO' THEN 'CURACAO'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'IRAN (ISLAMIC REPUBLIC OF)' THEN 'IRAN'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') LIKE 'LAO PEOPLE%S DEMOCRATIC REPUBLIC' THEN 'LAOS'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'SINT MAARTEN (DUTCH PART)' THEN 'SINT MAARTEN'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'TIMOR-LESTE' THEN 'TIMOR EST'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'SYRIAN ARAB REPUBLIC' THEN 'SYRIA'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'REPUBLIC OF MOLDOVA' THEN 'MOLDOVA'
			WHEN NULLIF(TRIM(UPPER(GeoAreaName)), '') = 'ETHIOPIA' THEN 'GHANA'
			ELSE NULLIF(TRIM(UPPER(GeoAreaName)), '')
		END AS Country,	
			DATEFROMPARTS(TRY_CONVERT(INT, TimePeriod), 1,1) AS Time_Period,
			TRY_CONVERT(DECIMAL(18,2), Value) AS Value,
			NULLIF(TRIM(SeriesDescription), '') AS SeriesDescription,
			NULLIF(TRIM(Nature), '') AS Nature,
			NULLIF(TRIM(Units), '') AS Units
		FROM bronze.raw_sdg_12b1;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		
		SET @batch_end_time = GETDATE();
		PRINT '=========================================================';
		PRINT 'Loading Silver Layer is Completed';
		PRINT '		- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================================';
	END TRY
	BEGIN CATCH
		PRINT '=========================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message :' + ERROR_MESSAGE();
		PRINT 'Error Number :' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State :' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT 'Error Severity :' + CAST(ERROR_SEVERITY() AS NVARCHAR);
		PRINT 'Error Line :' + CAST(ERROR_LINE() AS NVARCHAR);
		PRINT 'Error Procedure :' + ISNULL(ERROR_PROCEDURE(), '-');
		PRINT '=========================================================';
	END CATCH 
END



