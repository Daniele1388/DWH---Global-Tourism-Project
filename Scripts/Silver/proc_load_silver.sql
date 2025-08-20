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
   - Bronze C      -> Silver Geo_Area_Code (INT)
   - Bronze S      -> Silver Series_Code (DECIMAL(5,2))
   - Bronze Basic_Data -> Silver Country (cleaned/standardized)
   - Bronze Unnamed_5..Unnamed_8 -> Silver Series_L1..Series_L4
   - Bronze Units  -> Silver Units
   - For SDG tables: GeoAreaCode -> Geo_Area_Code; GeoAreaName -> Country;
     TimePeriod (YYYY) -> Time_Period (DATE = 1 Jan of that year);
     Value -> DECIMAL(18,2); Source, Nature, Units carried over.

3) Data Cleaning Rules (applied consistently)
   - Whitespace: TRIM/LTRIM/RTRIM on text columns.
   - Placeholders: convert '..' and empty strings '' to NULL.
   - Numbers from text:
       * Remove thousands separators (commas) with REPLACE(…, ',', '')
       * Preserve decimal points (e.g., '0.3' stays 0.3)
       * Cast to DECIMAL(18,2) for all year_* columns and SDG Value.
   - Header/noise rows in Country:
       * Rows where Basic_Data starts with '"The information' or 'Source'
         are set to NULL Country and still loaded (so you can detect them),
         i.e., they won’t masquerade as a real country.
   - Series method:
       * Where present in bronze 'Series', cleaned to Series_method
         (trim + placeholder→NULL).

4) Country Name Normalization
   - Applies a curated CASE mapping to standardize variants
     (e.g., "COTE D’IVOIRE" → "COTE D'IVOIRE", "KOREA, REPUBLIC OF" → "SOUTH KOREA",
     "CHINA, HONG KONG SPECIAL ADMINISTRATIVE REGION" → "HONG KONG", etc.).
   - SDG also uppercases GeoAreaName before mapping to increase hit rate.

5) Special SDG Handling
   - Time_Period: DATEFROMPARTS(YYYY, 1, 1).
   - Geo_Area_Code remap: 534→663, 535→658 (legacy → current codes).
   - SeriesCode / SeriesDescription / SDG_Indicator dropped because constant.

6) Operational Behavior
   - Idempotent full reload: TRUNCATE target, then INSERT SELECT from bronze.
   - Basic runtime logging with PRINT:
       * Section banners
       * Per-block elapsed seconds
       * Total elapsed time
   - TRY…CATCH block prints detailed error diagnostics
     (message, number, state, severity, line, procedure).

What this script does NOT do (by design)
----------------------------------------
- No index creation/maintenance (handled later).
- No PK/FK enforcement (to be added when modeling Gold/star schema).
- No deduplication/deletion logic (only load & light standardization).
- No NULL imputation (e.g., year_* NULL→0); such semantics will be handled in Gold.

Assumptions
-----------
- Bronze stores all numeric fields as NVARCHAR.
- Year columns are wide (year_1995 … year_2022) and remain wide in Silver.
- SDG Value may be a percentage; Units conveys the semantics (no rescaling here).
- Thousands separators in CSV may appear as commas; decimals use '.' in Silver.

Performance Notes
-----------------
- TRUNCATE+INSERT favors full refresh semantics and minimal logging.
- TRY_CONVERT safeguards type coercion; non-parsable values yield NULL.
- CASE country normalization happens inline; consider external reference table
  in future for maintainability/performance.

Next Steps (outside this procedure)
-----------------------------------
- Add columnstore/B-tree indexes after loads.
- Run Silver QC queries (NULLs, duplicates, units consistency).
- Build Gold layer (Kimball star): conformed dims (Geo, Series, Units, Time),
  unpivot year_* measures into long form for fact tables.
- Optional: wrap in explicit transaction with SET XACT_ABORT ON if you want
  all-or-nothing across the full batch.

======================================================================
*/

CREATE OR ALTER PROCEDURE [silver].[load_data] AS
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
		PRINT 'map C -> Geo_Area_Code (INT) and S -> Series_Code (DECIMAL(5,2));';
		PRINT 'standardize Country names and ignore header/noise rows.';
		PRINT 'Dropped: C_and_S, Notes, and unused Unnamed_* columns.';
		PRINT '=========================================================';

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.domestic_accommodation;
		INSERT INTO silver.domestic_accommodation
		(
			Geo_Area_Code,
			Series_Code,
			Country,
			Series_L1,
			Series_L2,
			Series_L3,
			Series_L4,
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
			TRY_CONVERT(INT, C) AS Geo_Area_Code,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS Series_Code,
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
			NULLIF(TRIM(Unnamed_5), '') AS Series_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Series_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Series_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Series_L4,
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
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.domestic_trip;
		INSERT INTO silver.domestic_trip
		(
			Geo_Area_Code,
			Series_Code,
			Country,
			Series_L1,
			Series_L2,
			Series_L3,
			Series_L4,
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
			TRY_CONVERT(INT, C) AS Geo_Area_Code,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS Series_Code,
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
			NULLIF(TRIM(Unnamed_5), '') AS Series_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Series_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Series_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Series_L4,
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
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.inbound_accommodation;
		INSERT INTO silver.inbound_accommodation
		(
			Geo_Area_Code,
			Series_Code,
			Country,
			Series_L1,
			Series_L2,
			Series_L3,
			Series_L4,
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
			TRY_CONVERT(INT, C) AS Geo_Area_Code,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS Series_Code,
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
			NULLIF(TRIM(Unnamed_5), '') AS Series_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Series_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Series_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Series_L4,
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
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.outbound_departures;
		INSERT INTO silver.outbound_departures
		(
			Geo_Area_Code,
			Series_Code,
			Country,
			Series_L1,
			Series_L2,
			Series_L3,
			Series_L4,
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
			TRY_CONVERT(INT, C) AS Geo_Area_Code,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS Series_Code,
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
			NULLIF(TRIM(Unnamed_5), '') AS Series_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Series_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Series_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Series_L4,
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
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.tourism_industries;
		INSERT INTO silver.tourism_industries
		(
			Geo_Area_Code,
			Series_Code,
			Country,
			Series_L1,
			Series_L2,
			Series_L3,
			Series_L4,
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
			TRY_CONVERT(INT, C) AS Geo_Area_Code,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS Series_Code,
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
			NULLIF(TRIM(Unnamed_5), '') AS Series_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Series_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Series_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Series_L4,
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
		PRINT 'map C -> Geo_Area_Code (INT), S -> Series_Code (DECIMAL(5,2));';
		PRINT 'extract/clean Series -> Series_method; standardize Country names.';
		PRINT 'Dropped: C_and_S, Notes, and unused Unnamed_* columns.';
		PRINT '=========================================================';
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.inbound_arrivals;
		INSERT INTO silver.inbound_arrivals
		(
			Geo_Area_Code,
			Series_Code,
			Country,
			Series_L1,
			Series_L2,
			Series_L3,
			Series_L4,
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
			TRY_CONVERT(INT, C) AS Geo_Area_Code,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS Series_Code,
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
			NULLIF(TRIM(Unnamed_5), '') AS Series_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Series_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Series_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Series_L4,
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

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.inbound_expenditure;
		INSERT INTO silver.inbound_expenditure
		(
			Geo_Area_Code,
			Series_Code,
			Country,
			Series_L1,
			Series_L2,
			Series_L3,
			Series_L4,
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
			TRY_CONVERT(INT, C) AS Geo_Area_Code,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS Series_Code,
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
			NULLIF(TRIM(Unnamed_5), '') AS Series_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Series_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Series_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Series_L4,
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

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.inbound_purpose;
		INSERT INTO silver.inbound_purpose
		(
			Geo_Area_Code,
			Series_Code,
			Country,
			Series_L1,
			Series_L2,
			Series_L3,
			Series_L4,
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
			TRY_CONVERT(INT, C) AS Geo_Area_Code,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS Series_Code,
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
			NULLIF(TRIM(Unnamed_5), '') AS Series_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Series_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Series_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Series_L4,
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

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.inbound_regions;
		INSERT INTO silver.inbound_regions
		(
			Geo_Area_Code,
			Series_Code,
			Country,
			Series_L1,
			Series_L2,
			Series_L3,
			Series_L4,
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
			TRY_CONVERT(INT, C) AS Geo_Area_Code,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS Series_Code,
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
			NULLIF(TRIM(Unnamed_5), '') AS Series_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Series_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Series_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Series_L4,
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

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.inbound_transport;
		INSERT INTO silver.inbound_transport
		(
			Geo_Area_Code,
			Series_Code,
			Country,
			Series_L1,
			Series_L2,
			Series_L3,
			Series_L4,
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
			TRY_CONVERT(INT, C) AS Geo_Area_Code,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS Series_Code,
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
			NULLIF(TRIM(Unnamed_5), '') AS Series_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Series_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Series_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Series_L4,
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
       
 	    SET @start_time = GETDATE();
		TRUNCATE TABLE silver.outbound_expenditure;
		INSERT INTO silver.outbound_expenditure
		(
			Geo_Area_Code,
			Series_Code,
			Country,
			Series_L1,
			Series_L2,
			Series_L3,
			Series_L4,
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
			TRY_CONVERT(INT, C) AS Geo_Area_Code,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS Series_Code,
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
			NULLIF(TRIM(Unnamed_5), '') AS Series_L1,
			NULLIF(TRIM(Unnamed_6), '') AS Series_L2,
			NULLIF(TRIM(Unnamed_7), '') AS Series_L3,
			NULLIF(TRIM(Unnamed_8), '') AS Series_L4,
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
		PRINT 'standardize Geo_Area_Code (INT) and Geo_Area_Name.';
		PRINT 'SPECIAL NOTE: Corrected Geo_Area_Code mapping conflicts in sdg_12b1:';
		PRINT '  - ID 534 (Bonaire in other tables) → 663 (Sint Maarten) for TEZVT source';
		PRINT '  - ID 535 (Curacao in other tables) → 658 (Sint Eustatius) for Eustatius source';
		PRINT 'Dropped: INDEX, SDG_Indicator, SeriesCode, SeriesDescription (constant values).';
		PRINT '=========================================================';

        SET @start_time = GETDATE();
		TRUNCATE TABLE silver.sdg_891;
		INSERT INTO silver.sdg_891
		(
			Geo_Area_Code,
			Country,
			Time_Period,
			Value,
			Source,
			Nature,
			Units
		)
		SELECT
		CASE
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 534 THEN 663
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 535 THEN 658
			ELSE TRY_CONVERT(INT, GeoAreaCode)
		END Geo_Area_Code,
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
			NULLIF(TRIM(Source), '') AS Source,
			NULLIF(TRIM(Nature), '') AS Nature,
			NULLIF(TRIM(Units), '') AS Units
		FROM bronze.raw_sdg_891;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
        
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.sdg_892;
		INSERT INTO silver.sdg_892
		(
			Geo_Area_Code,
			Country,
			Time_Period,
			Value,
			Source,
			Nature,
			Units
		)
		SELECT
		CASE
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 534 THEN 663
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 535 THEN 658
			ELSE TRY_CONVERT(INT, GeoAreaCode)
		END Geo_Area_Code,
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
			NULLIF(TRIM(Source), '') AS Source,
			NULLIF(TRIM(Nature), '') AS Nature,
			NULLIF(TRIM(Units), '') AS Units
		FROM bronze.raw_sdg_892;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
        
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.sdg_12b1;
		INSERT INTO silver.sdg_12b1
		(
			Geo_Area_Code,
			Country,
			Time_Period,
			Value,
			Source,
			Nature,
			Units
		)
		SELECT
		CASE
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 534 THEN 663
			WHEN TRY_CONVERT(INT, GeoAreaCode) = 535 THEN 658
			ELSE TRY_CONVERT(INT, GeoAreaCode)
		END Geo_Area_Code,
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
			NULLIF(TRIM(Source), '') AS Source,
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
