CREATE OR ALTER PROCEDURE bronze.load_raw_data AS
BEGIN
	TRUNCATE TABLE bronze.raw_domestic_accommodation;
	BULK INSERT bronze.raw_domestic_accommodation 
	FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Domestic Tourism-Accommodation.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
	);

	TRUNCATE TABLE bronze.raw_domestic_trip;
	BULK INSERT bronze.raw_domestic_trip 
	FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Domestic Tourism-Trips.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
	);

	TRUNCATE TABLE bronze.raw_inbound_accommodation;
	BULK INSERT bronze.raw_inbound_accommodation 
	FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Inbound Tourism-Accommodation.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
	);

	TRUNCATE TABLE bronze.raw_inbound_arrivals;
	BULK INSERT bronze.raw_inbound_arrivals 
	FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Inbound Tourism-Arrivals.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
	);

	TRUNCATE TABLE bronze.raw_inbound_expenditure;
	BULK INSERT bronze.raw_inbound_expenditure 
	FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Inbound Tourism-Expenditure.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
	);

	TRUNCATE TABLE bronze.raw_inbound_purpose;
	BULK INSERT bronze.raw_inbound_purpose 
	FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Inbound Tourism-Purpose.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
	);

	TRUNCATE TABLE bronze.raw_inbound_regions;
	BULK INSERT bronze.raw_inbound_regions 
	FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Inbound Tourism-Regions.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
	);

	TRUNCATE TABLE bronze.raw_inbound_transport;
	BULK INSERT bronze.raw_inbound_transport 
	FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Inbound Tourism-Transport.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
	);

	TRUNCATE TABLE bronze.raw_outbound_departures;
	BULK INSERT bronze.raw_outbound_departures 
	FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Outbound Tourism-Departures.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
	);

	TRUNCATE TABLE bronze.raw_outbound_expenditure;
	BULK INSERT bronze.raw_outbound_expenditure 
	FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Outbound Tourism-Expenditure.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
	);

	TRUNCATE TABLE bronze.raw_sdg_891;
	BULK INSERT bronze.raw_sdg_891 
	FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\SDG 8.9.1.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
	);

	TRUNCATE TABLE bronze.raw_sdg_892;
	BULK INSERT bronze.raw_sdg_892 
	FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\SDG 8.9.2.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
	);

	TRUNCATE TABLE bronze.raw_sdg_12b1;
	BULK INSERT bronze.raw_sdg_12b1 
	FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\SDG 12.b.1.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
	);

	TRUNCATE TABLE bronze.raw_tourism_industries;
	BULK INSERT bronze.raw_tourism_industries 
	FROM 'C:\Users\Utente\Desktop\SQL\Progetto Global Tourism Statistics\UN_TourismCSV\Tourism Industries.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
	);
END
