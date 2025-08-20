/*
===============================================
CREATE DATABASE 'TOURISMDB' AND SCHEMAS
===============================================
*/

USE master;
GO

-- Check if the TourismDB database already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'TourismDB')
BEGIN
	ALTER DATABASE TourismDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE TourismDB;
END;
GO

-- Create the 'TourismDB'
CREATE DATABASE TourismDB;
GO

USE TourismDB;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
