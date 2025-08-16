/*
=========================================================
Load Silver Layer from Bronze Raw Tables
=========================================================
Purpose:
  This script loads data from the 'bronze' schema (raw imported CSV data)
  into the 'silver' schema (cleaned and standardized data) in the Data Warehouse.

General Rules:
  - Use TRUNCATE before INSERT to ensure idempotent reloads.
  - Apply data cleaning:
      * TRIM spaces from all string fields.
      * Convert empty strings ('') or placeholder values ('..') to NULL.
      * Remove thousands separators (commas) from numeric fields.
      * Preserve decimal points for fractional numbers (e.g., 0.3).
  - Convert data types from NVARCHAR (bronze) to appropriate formats (silver):
      * DECIMAL for numeric values.
      * INT for codes.
      * DATE for year-based time periods.
  - Drop unnecessary or constant-value columns from bronze tables.
  - Keep only analysis-ready fields in silver tables.

Assumptions:
  - All numeric values in bronze are stored as NVARCHAR due to raw CSV import.
  - Time series columns in wide format (year_1995...year_2022) remain unchanged.
  - Certain SDG tables contain percentage values (Units = 'PERCENT').

Note:
  Index creation and additional optimizations are handled in a later step.
=========================================================
*/

CREATE OR ALTER PROCEDURE Silver.load_data AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================================';
		PRINT 'Load silver tables (domestic_accommodation, domestic_trip, inbound_accommodation, outbound_departures and tourism_industries)';
		PRINT 'from their equivalent bronze raw tables';
		PRINT 'Cleaning: Remove commas, convert to DECIMAL, trim text, map series codes';
		PRINT 'Dropped columns: C_and_S, Notes, unused unnamed columns';
		PRINT '=========================================================';

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.domestic_accommodation;
		INSERT INTO silver.domestic_accommodation
		(
			C,
			S,
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
			TRY_CONVERT(INT, C) AS C,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS S,
			NULLIF(TRIM(Basic_data), '') AS Country,
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
			C,
			S,
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
			TRY_CONVERT(INT, C) AS C,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS S,
			NULLIF(TRIM(Basic_data), '') AS Country,
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
			C,
			S,
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
			TRY_CONVERT(INT, C) AS C,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS S,
			NULLIF(TRIM(Basic_data), '') AS Country,
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
			C,
			S,
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
			TRY_CONVERT(INT, C) AS C,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS S,
			NULLIF(TRIM(Basic_data), '') AS Country,
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
			C,
			S,
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
			TRY_CONVERT(INT, C) AS C,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS S,
			NULLIF(TRIM(Basic_data), '') AS Country,
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
		PRINT 'Load silver tables (inbound_arrivals, inbound_expenditure, inbound_purpose, inbound_regions, inbound_transport and outbound_expenditure)';
		PRINT 'from their equivalent bronze raw tables';
		PRINT 'Cleaning: Remove commas, convert to DECIMAL, trim text, handle series_method';
		PRINT 'Dropped columns: C_and_S, Notes, unused unnamed columns';
		PRINT '=========================================================';
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.inbound_arrivals;
		INSERT INTO silver.inbound_arrivals
		(
			C,
			S,
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
			TRY_CONVERT(INT, C) AS C,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS S,
			NULLIF(TRIM(Basic_data), '') AS Country,
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
			C,
			S,
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
			TRY_CONVERT(INT, C) AS C,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS S,
			NULLIF(TRIM(Basic_data), '') AS Country,
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
		TRUNCATE TABLE silver.inbound_regions;
		INSERT INTO silver.inbound_regions
		(
			C,
			S,
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
			TRY_CONVERT(INT, C) AS C,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS S,
			NULLIF(TRIM(Basic_data), '') AS Country,
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
			C,
			S,
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
			TRY_CONVERT(INT, C) AS C,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS S,
			NULLIF(TRIM(Basic_data), '') AS Country,
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
			C,
			S,
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
			TRY_CONVERT(INT, C) AS C,
			TRY_CONVERT(DECIMAL(5,2), REPLACE(S, ',','.')) AS S,
			NULLIF(TRIM(Basic_data), '') AS Country,
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
		PRINT 'Load silver tables (sdg_891, sdg_892 and sdg_12b1) from their equivalent bronze raw tables';
		PRINT 'Cleaning: Convert TimePeriod (YYYY) to DATE, cast Value to DECIMAL, trim text';
		PRINT 'Dropped columns: INDEX, SDG_Indicator, SeriesCode, SeriesDescription (constant values)';
		PRINT '=========================================================';

        SET @start_time = GETDATE();
		TRUNCATE TABLE silver.sdg_891;
		INSERT INTO silver.sdg_891
		(
			Geo_Area_Code,
			Geo_Area_Name,
			Time_Period,
			Value,
			Source,
			Nature,
			Units
		)
		SELECT
			TRY_CONVERT(INT, GeoAreaCode) AS Geo_Area_Code,
			NULLIF(TRIM(GeoAreaName), '') AS Geo_Area_Name,
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
			Geo_Area_Name,
			Time_Period,
			Value,
			Source,
			Nature,
			Units
		)
		SELECT
			TRY_CONVERT(INT, GeoAreaCode) AS Geo_Area_Code,
			NULLIF(TRIM(GeoAreaName), '') AS Geo_Area_Name,
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
			Geo_Area_Name,
			Time_Period,
			Value,
			Source,
			Nature,
			Units
		)
		SELECT
			TRY_CONVERT(INT, GeoAreaCode) AS Geo_Area_Code,
			NULLIF(TRIM(GeoAreaName), '') AS Geo_Area_Name,
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
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message :' + ERROR_MESSAGE();
		PRINT 'Error Number :' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State :' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT 'Error Severity :' + CAST(ERROR_SEVERITY() AS NVARCHAR);
		PRINT 'Error Line :' + CAST(ERROR_LINE() AS NVARCHAR);
		PRINT 'Error Procedure :' + ISNULL(ERROR_PROCEDURE(), '-');
		PRINT '=========================================================';
	END CATCH 
END
