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

