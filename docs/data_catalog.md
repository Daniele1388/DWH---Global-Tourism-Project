*** GLOBAL TOURISM STATISTICS - GOLD LAYER DATA CATALOG ***

Overview:
The Gold Layer of the Data Warehouse provides cleaned, standardized, and analysis-ready data for tourism statistics.
It is built on top of the Silver Layer and organizes data into dimension and fact views, following a star schema design.

The goal is to make data easy to query, consistent across sources, and ready for business intelligence and reporting.

---------------------------------------------------------------------------------------
1. Dimension Views
---------------------------------------------------------------------------------------

***gold.dim_country

Purpose: Provides a standardized list of countries and their codes, used for joining with fact tables.
Columns:
Country_key (BIGINT, surrogate key) – Unique identifier for each country.
Country_id (INT) – Standard country code (from UN dataset).
Country_name (NVARCHAR) – Official country name.

***gold.dim_indicator

Purpose: Defines tourism-related indicators (e.g., accommodation, inbound tourism, outbound tourism).
Columns:
Indicator_key (BIGINT, surrogate key) – Unique identifier for each indicator.
Indicator_id (NVARCHAR) – Original code from source dataset.
Indicator_name (NVARCHAR) – Descriptive name of the indicator.
Source_table (VARCHAR) – Source table from silver layer.

***gold.dim_year

Purpose: Centralized time dimension for all tourism data.
Columns:
Year_key (BIGINT, surrogate key) – Unique identifier for each year.
Year (INT) – Calendar year (1995–2023).

***gold.dim_unit_of_measure

Purpose: Provides consistent units of measurement for tourism statistics.
Columns:
Units_key (BIGINT, surrogate key) – Unique identifier for each unit.
Measure_Units (NVARCHAR) – Example: “US$ millions”, “Number”, “Percentage”.

---------------------------------------------------------------------------------------
2. Fact Views
---------------------------------------------------------------------------------------

***gold.fact_domestic_tourism

Purpose: Contains domestic tourism statistics (guests and overnights in accommodation).
Columns:
Country_key (FK → dim_country)
Indicator_key (FK → dim_indicator)
Year_key (FK → dim_year)
Units_key (FK → dim_unit_of_measure)
Value (DECIMAL) – Numeric value for the tourism measure.

***gold.fact_inbound_tourism

Purpose: Contains inbound tourism data (visitors and expenditures from abroad).
Columns: Same structure as fact_domestic_tourism.

***gold.fact_outbound_tourism

Purpose: Contains outbound tourism data (residents traveling abroad).
Columns: Same structure as fact_domestic_tourism.

***gold.fact_tourism_industries

Purpose: Contains tourism-related industry statistics (employment, GDP contribution, etc.).
Columns: Same structure as fact_domestic_tourism.

***gold.fact_sdg

Purpose: Contains tourism-related Sustainable Development Goals (SDG) indicators from the UN, standardized for cross-country and time-series analysis.
Columns:
Country_key (FK → dim_country)
Year_key (FK → dim_year)
Units_key (FK → dim_unit_of_measure)
Value (DECIMAL) – Numeric value for the tourism measure.

---------------------------------------------------------------------------------------
3. Relationships
---------------------------------------------------------------------------------------

Each fact view links to the four dimension views:

Country_key → dim_country
Indicator_key → dim_indicator
Year_key → dim_year
Units_key → dim_unit_of_measure

This creates a one-to-many relationship:
One country → many fact rows.
One indicator → many fact rows.
One year → many fact rows.
One unit → many fact rows.
