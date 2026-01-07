# FlexiMart Data Architecture Project
Student Name: [Your Name]
Student ID: [Your ID]
Email: [Your Email]
Date: [Date]

## Project Overview
ETL pipeline to clean CSVs and load into MySQL, NoSQL analysis with MongoDB, and a data warehouse (star schema) with analytics queries.

## Repo structure
(put the files exactly as described in earlier plan)

## Setup
1. Install Python deps: pip install pandas mysql-connector-python
2. Run ETL: python part1-database-etl/etl_pipeline.py --csv_dir data --db_pass <pwd>
3. Load warehouse schema: mysql -u root -p fleximart_dw < part3-datawarehouse/warehouse_schema.sql
4. Generate and load warehouse data: python part3-datawarehouse/warehouse_generator.py
   mysql -u root -p fleximart_dw < part3-datawarehouse/warehouse_data.sql
5. MongoDB: mongoimport --db fleximart_nosql --collection products --file part2-nosql/products_catalog.json --jsonArray
   mongosh --file part2-nosql/mongodb_operations.js
