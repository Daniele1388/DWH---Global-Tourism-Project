# DWH---Global-Tourism-Project
This project is a complete SQL-based Data Warehouse built from official UN Tourism statistics (UNWTO), covering global tourism trends from 1995 to 2022.

It follows a Medallion Architecture (Bronze → Silver → Gold) and implements a Kimball-style star schema to support exploratory data analysis, performance tracking, and sustainable development goals (SDG 8.9 & 12.b).

✅ Key Features:
Data from official UNWTO sources (via Kaggle)
Cleaned and modeled in SQL Server
Full ETL logic: ingestion, normalization, unpivoting
Dimensions: Country, Year, Indicator, Unit, Source
Fact tables for inbound, outbound, and domestic tourism
Business-ready views for tourism KPIs, expenditure, accommodation and SDG metrics
