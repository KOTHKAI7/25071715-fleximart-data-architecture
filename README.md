FlexiMart Data Architecture Project

Student Name: Kaivalya Kothari
Student ID: bitsom_ba_25071715
Email: kotharikaivalya7@gmail.com
Submission Date: 8 January 2026

⸻

Project Overview

This project implements an end-to-end data architecture for the FlexiMart retail system, covering:
	•	Relational ETL pipeline using Python and MySQL
	•	NoSQL data modeling and analysis using MongoDB
	•	Data warehouse design using a star schema with analytical SQL queries

The objective is to demonstrate data cleaning, transformation, storage, and analytical querying across multiple database paradigms.

⸻

Repository Structure

fleximart-assignment/
│
├── data/
│   └── (raw CSV files used for ETL)
│
├── part1-database-etl/
│   ├── etl_pipeline.py
│   ├── business_queries.sql
│   └── data_quality_report.txt
│
├── part2-nosql/
│   ├── products_catalog.json
│   ├── mongodb_operations.js
│   └── nosql_analysis.md
│
├── part3-datawarehouse/
│   ├── warehouse_schema.sql
│   ├── warehouse_generator.py
│   └── analytics_queries.sql
│
├── .gitignore
└── README.md


⸻

Setup and Execution

Prerequisites
	•	Python 3.10+
	•	MySQL 8.x
	•	MongoDB Server + MongoDB Tools
	•	pip, git, and mongosh available in PATH

⸻

Part 1: Relational ETL (MySQL)
	1.	Install Python dependencies:

pip install pandas mysql-connector-python


	2.	Run the ETL pipeline:

python part1-database-etl/etl_pipeline.py --csv_dir data --db_pass <your_mysql_password>



This step cleans raw CSV data and loads it into the fleximart MySQL database.

⸻

Part 2: NoSQL Analysis (MongoDB)
	1.	Import product catalog into MongoDB:

mongoimport --db fleximart_nosql --collection products \
  --file part2-nosql/products_catalog.json --jsonArray


	2.	Run MongoDB operations:

mongosh --file part2-nosql/mongodb_operations.js



This section demonstrates document modeling, aggregation, and basic analytical queries.

⸻

Part 3: Data Warehouse & Analytics
	1.	Create warehouse schema:

mysql -u root -p fleximart_dw < part3-datawarehouse/warehouse_schema.sql


	2.	Populate the data warehouse:

python part3-datawarehouse/warehouse_generator.py


	3.	Run analytical queries:

mysql -u root -p fleximart_dw < part3-datawarehouse/analytics_queries.sql



The warehouse follows a star schema with fact and dimension tables to support analytical workloads.

⸻

Outcome
	•	Cleaned and validated transactional data
	•	NoSQL document-based analysis
	•	Fully populated data warehouse
	•	Business-oriented analytical queries for reporting and insights

⸻
