/*
=========================================================
Stored Procedure: Load Bronze Layer
=========================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.

NOTE: 
  For the 'Inbound Tourism-Arrivals' dataset, the original CSV contained a comma
  inside a text field ("of which, cruise passengers"), causing column misalignment
  during import.
  The file was re-exported from Power Query with text qualifiers (") and a semicolon (;)
  as the new field delimiter.
  Consequently, the BULK INSERT for this table uses FIELDTERMINATOR = ';'
  instead of the default comma.
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
		FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Domestic Tourism-Accommodation.csv'
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
		TRUNCATE TABLE bronze.raw_domestic_trip;
		BULK INSERT bronze.raw_domestic_trip 
		FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Domestic Tourism-Trips.csv'
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
		PRINT 'Loading Inbound Tables';
		PRINT '---------------------------------------------------------';
	
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_inbound_accommodation;
		BULK INSERT bronze.raw_inbound_accommodation 
		FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Inbound Tourism-Accommodation.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		-- NOTE: This CSV was re-exported from Power Query with text qualifiers (")
		-- and a semicolon (;) as the field delimiter to fix column shifting caused
		-- by a comma inside the text ("of which, cruise passengers").
		-- BULK INSERT uses FIELDTERMINATOR = ';' accordingly.
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_inbound_arrivals;
		BULK INSERT bronze.raw_inbound_arrivals 
		FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Inbound Tourism-Arrivals.csv'
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
		FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Inbound Tourism-Expenditure.csv'
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
		TRUNCATE TABLE bronze.raw_inbound_purpose;
		BULK INSERT bronze.raw_inbound_purpose 
		FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Inbound Tourism-Purpose.csv'
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
		TRUNCATE TABLE bronze.raw_inbound_regions;
		BULK INSERT bronze.raw_inbound_regions 
		FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Inbound Tourism-Regions.csv'
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
		TRUNCATE TABLE bronze.raw_inbound_transport;
		BULK INSERT bronze.raw_inbound_transport 
		FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Inbound Tourism-Transport.csv'
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
		PRINT 'Loading Outbound Tables';
		PRINT '---------------------------------------------------------';
	
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_outbound_departures;
		BULK INSERT bronze.raw_outbound_departures 
		FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Outbound Tourism-Departures.csv'
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
		TRUNCATE TABLE bronze.raw_outbound_expenditure;
		BULK INSERT bronze.raw_outbound_expenditure 
		FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Outbound Tourism-Expenditure.csv'
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
		PRINT 'Loading SDG Tables';
		PRINT '---------------------------------------------------------';
	
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.raw_sdg_891;
		BULK INSERT bronze.raw_sdg_891 
		FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\SDG 8.9.1.csv'
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
		FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\SDG 8.9.2.csv'
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
		FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\SDG 12.b.1.csv'
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
		FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Tourism Industries.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
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
END
