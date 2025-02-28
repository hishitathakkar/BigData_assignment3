Quickstart Specification: Building an Incremental Data Pipeline for CO₂ Emissions Data Using Snowflake and Snowpark

Overview

This Quickstart guides users through building a Snowflake-based data pipeline that ingests, transforms, and updates daily CO₂ emissions data from the Mauna Loa Observatory. The pipeline is implemented using Snowpark Python and Snowflake Notebooks, leveraging Snowflake Tasks, Streams, and Stored Procedures to enable incremental data updates.

Expected Outcome

By following this Quickstart:
1. Users will have a fully functional Snowflake-based data pipeline.
2. CO₂ emissions data will be updated daily in an incremental fashion.
3. Analytics tables will provide daily percentage change in emissions and historical trends.
4. The architecture uses at least two UDFs (one SQL and one Python) and a stored procedure for updates.
5. A Snowflake Notebook will facilitate Snowpark-based transformations and analytics.
6. A testing framework and Jinja-based scripts will ensure DEV and PROD support.

Architecture Overview

Pipeline Components

1. External Data Source:
The pipeline will pull daily CO₂ emissions data from a public API (e.g., NOAA, ESRL). Data will be ingested in CSV/JSON format via Snowflake External Stages (S3 or Google Buckets).

2. Raw Data Storage (RAW_DATA Schema):
The ingested data will be stored in a staging table (RAW_CO2_STAGING). Snowflake Streams will track new records for incremental updates.

3. Data Harmonization & Transformation (HARMONIZED_CO2 Schema):
Transformation logic using Snowpark Python:
i.  Convert raw API response into a structured table.
ii. Standardize timestamps and emission metrics.
iii.Ensure data consistency and remove duplicates.
iv. A harmonized table (CO2_HARMONIZED) will store cleaned and structured emissions data.

User-Defined Functions (UDFs):
SQL UDF: A function to normalize CO₂ measurements across different units.
Python UDF: A function to calculate daily percentage change in CO₂ emissions.

4. Analytics & Aggregation (ANALYTICS_CO2 Schema):
Precomputed analytics tables for:
i) Daily percentage change in CO₂ emissions.
ii) Long-term CO₂ concentration trends.
iii) Stored Procedure (UPDATE_CO2_SP) to handle incremental updates and apply transformations.

5. Task Orchestration & Automation:
Snowflake Tasks will automate:
Data ingestion (UPDATE_CO2_DATA).
Daily updates (UPDATE_DOW30_METRICS_TASK).
A Snowflake Notebook will be used for data engineering and Snowpark Python transformations.
GitHub Actions Integration for CI/CD of Snowpark Python code.

6. Testing & Validation:
Implement unit tests for UDFs and stored procedures.
Use sample datasets for validating pipeline correctness.
Monitor task execution logs to ensure proper scheduling and data updates.

7. Environment Management with Jinja Templates:
Create Jinja-based scripts to support DEV and PROD environments.
Use parameterized configurations to dynamically adjust Snowflake roles, schemas, and warehouses per environment.

Summary
This Quickstart enables users to build a scalable, automated, and efficient data pipeline for CO₂ emissions data using Snowflake and Snowpark, ensuring accurate and timely updates for environmental analytics.

AI USE DISCLOSURE:

Required Attestation and Contribution Declaration
WE ATTEST THAT WE HAVEN’T USED ANY OTHER STUDENTS’ WORK IN OUR
ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK.
Contribution:
Example:
● Hishita Thakkar: 33 1/3%
● Asavari Shejwal: 33 1/3%
● Pratik Kanade: 33 1/3%
Links to GitHub tasks and tasks owned by each member
