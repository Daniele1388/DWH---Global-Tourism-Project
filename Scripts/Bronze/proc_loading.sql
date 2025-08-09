/*
=========================================================
Stored Procedure: Load Bronze Layer
=========================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.

/*
NOTE:
  All raw CSVs — except 'raw_sdg_891', 'raw_sdg_892', and 'raw_sdg_12b1' —
  are pre-cleaned with Power Query (Excel) and saved as UTF-8 CSV
  with semicolon (;) as field delimiter and " as text qualifier.
  This avoids column shifts caused by commas inside text and normalizes numeric fields
  (thousand separators, NBSP/spaces). BULK INSERT therefore uses FIELDTERMINATOR=';'.
  
PATH:
  Replace <path_to_dataset> with the folder where you downloaded and cleaned 
  the UNWTO CSV files.

Example:
  'C:/Users/YourName/data/UN_TourismCSV_PQ/Domestic Tourism-Accommodation.csv'
*/
=========================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_raw_data AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=========================================================';

		PRINT '---------------------------------------------------------';
		PRINT 'Loading Domestic Tables';
		PRINT '---------------------------------------------------------';
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_domestic_accommodation;
		BULK INSERT bronze.raw_domestic_accommodation 
		FROM '<path_to_dataset>\Domestic Tourism-Accommodation.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ';',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_domestic_trip;
		BULK INSERT bronze.raw_domestic_trip 
		FROM '<path_to_dataset>\Domestic Tourism-Trips.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ';',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '---------------------------------------------------------';
		PRINT 'Loading Inbound Tables';
		PRINT '---------------------------------------------------------';
	
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_inbound_accommodation;
		BULK INSERT bronze.raw_inbound_accommodation 
		FROM '<path_to_dataset>\Inbound Tourism-Accommodation.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ';',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_inbound_arrivals;
		BULK INSERT bronze.raw_inbound_arrivals 
		FROM '<path_to_dataset>\Inbound Tourism-Arrivals.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ';',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_inbound_expenditure;
		BULK INSERT bronze.raw_inbound_expenditure 
		FROM '<path_to_dataset>\Inbound Tourism-Expenditure.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ';',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_inbound_purpose;
		BULK INSERT bronze.raw_inbound_purpose 
		FROM '<path_to_dataset>\Inbound Tourism-Purpose.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ';',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_inbound_regions;
		BULK INSERT bronze.raw_inbound_regions 
		FROM '<path_to_dataset>\Inbound Tourism-Regions.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ';',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_inbound_transport;
		BULK INSERT bronze.raw_inbound_transport 
		FROM '<path_to_dataset>\Inbound Tourism-Transport.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ';',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '---------------------------------------------------------';
		PRINT 'Loading Outbound Tables';
		PRINT '---------------------------------------------------------';
	
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_outbound_departures;
		BULK INSERT bronze.raw_outbound_departures 
		FROM '<path_to_dataset>\Outbound Tourism-Departures.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ';',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_outbound_expenditure;
		BULK INSERT bronze.raw_outbound_expenditure 
		FROM ''<path_to_dataset>\Outbound Tourism-Expenditure.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ';',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '---------------------------------------------------------';
		PRINT 'Loading SDG Tables';
		PRINT '---------------------------------------------------------';
	
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_sdg_891;
		BULK INSERT bronze.raw_sdg_891 
		FROM ''<path_to_dataset>\SDG 8.9.1.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_sdg_892;
		BULK INSERT bronze.raw_sdg_892 
		FROM ''<path_to_dataset>\UN_TourismCSV\SDG 8.9.2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_sdg_12b1;
		BULK INSERT bronze.raw_sdg_12b1 
		FROM ''<path_to_dataset>\SDG 12.b.1.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '---------------------------------------------------------';
		PRINT 'Loading Tourism_Industries Tables';
		PRINT '---------------------------------------------------------';
	
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_tourism_industries;
		BULK INSERT bronze.raw_tourism_industries 
		FROM '<path_to_dataset>\Tourism Industries.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ';',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================================';
		PRINT 'Loading Bronze Layer is Completed';
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

