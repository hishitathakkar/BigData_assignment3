summary: CO₂ Data Pipeline - Fetch, Store, Process in Snowflake
id: co2-data-pipeline
categories: Data Engineering, Cloud, Snowflake, AWS
status: Published
authors: Hishita Thakkar
feedback link: https://your-feedback-link.com

# CO₂ Data Pipeline - Fetch, Store, Process in Snowflake

This guide walks through **building a CO₂ data pipeline** from **data ingestion to analytics**, using:
✅ **NOAA Data Fetching**  
✅ **AWS S3 for Storage**  
✅ **Snowflake for Processing & Analytics**  
✅ **Automated Updates with Stored Procedures & Tasks**  
✅ **CI/CD Deployment via GitHub Actions**  
✅ **Jinja for Dynamic SQL Templates** 

---

## **📌 Table of Contents**
1. [Prerequisites](#prerequisites)
2. [Fetching CO₂ Data](#fetching-co₂-data)
3. [Uploading Data to AWS S3](#uploading-data-to-aws-s3)
4. [Loading Data into Snowflake](#loading-data-into-snowflake)
5. [Transforming & Normalizing Data](#transforming-and-normalizing-data)
6. [Analytics & Reporting](#analytics-and-reporting)
7. [Automating Updates](#automating-updates)
8. [Using Jinja for SQL Templating](#using-jinja-for-sql-templating)
9. [Deployment with GitHub Actions](#deployment-with-github-actions)
10. [Conclusion](#conclusion)

---

## **1️⃣ Prerequisites**
Before starting, ensure you have:
- **Python 3.x installed**
- **AWS S3 credentials set up**
- **A Snowflake account with schema permissions**
- **Installed Python libraries** (`boto3`, `snowflake-connector-python`, etc.)

---

## **2️⃣ Fetching CO₂ Data**
We fetch **daily CO₂ readings** from **NOAA’s data portal**.

```python
import requests

URL = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.txt"
response = requests.get(URL)

# Save as CSV
with open("co2_data.csv", "w") as f:
    f.write(response.text)

print("✅ CO₂ Data Fetched Successfully")
```

## **3️⃣ Uploading Data to AWS S3**
Once fetched, the data is stored in an AWS S3 bucket.

```python
import boto3

s3 = boto3.client('s3')
bucket_name = "big.data.ass3"

s3.upload_file("co2_data.csv", bucket_name, "raw_data/co2_data.csv")

print("✅ CO₂ Data Uploaded to S3")
```

To verify:
```bash
aws s3 ls s3://big.data.ass3/raw_data/
```

## **4️⃣ Loading Data into Snowflake**
### Storage Integration
We first set up Snowflake Storage Integration for AWS S3.

```sql
CREATE OR REPLACE STORAGE INTEGRATION CO2_S3_INTEGRATION
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::183295451617:role/MySnowflakeRole'
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('s3://big.data.ass3/');
```

### Defining Snowflake Stage & File Format
```sql
CREATE OR REPLACE STAGE CO2_STAGE
STORAGE_INTEGRATION = CO2_S3_INTEGRATION
URL = 's3://big.data.ass3/';

CREATE OR REPLACE FILE FORMAT CO2_CSV_FORMAT
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1;
```

### Loading Data from S3 into Snowflake
```sql
CREATE OR REPLACE TABLE RAW_DOW30_STAGING (
    YEAR INT,
    MONTH INT,
    DAY INT,
    DECIMAL_DATE FLOAT,
    CO2_EMISSION FLOAT
);

COPY INTO RAW_DOW30_STAGING
FROM @CO2_STAGE/raw_data/co2_data.csv
FILE_FORMAT = (FORMAT_NAME = 'CO2_CSV_FORMAT')
FORCE = TRUE;
```

## **5️⃣ Transforming & Normalizing Data**
### Harmonizing CO₂ Data
We store harmonized data in the PUBLIC schema.

```sql
CREATE OR REPLACE TABLE DOW30_HARMONIZED AS
SELECT YEAR, MONTH, DAY, DECIMAL_DATE, CO2_EMISSION FROM RAW_DOW30_STAGING;
```

### Normalizing CO₂ Emissions
We add a normalized CO₂ column and compute values.

```sql
ALTER TABLE DOW30_HARMONIZED ADD COLUMN NORMALIZED_CO2 FLOAT;
UPDATE DOW30_HARMONIZED
SET NORMALIZED_CO2 = CO2_EMISSION / 400.0;
```

## **6️⃣ Analytics & Reporting**
We create an analytics table in `ANALYTICS_DOW30` to compute monthly CO₂ statistics.

```sql
CREATE OR REPLACE TABLE ANALYTICS_DOW30 AS
SELECT 
    YEAR, 
    MONTH, 
    AVG(CO2_EMISSION) AS AVG_CO2, 
    MAX(CO2_EMISSION) AS MAX_CO2, 
    MIN(CO2_EMISSION) AS MIN_CO2
FROM DOW30_HARMONIZED
GROUP BY YEAR, MONTH;
```

## **7️⃣ Automating Updates**
To keep the dataset up-to-date, we automate merging new data.

### Stored Procedure for Updates
```sql
CREATE OR REPLACE PROCEDURE UPDATE_CO2_PROCEDURE()
RETURNS STRING
LANGUAGE SQL
AS 
$$
BEGIN
    MERGE INTO DOW30_HARMONIZED AS TARGET
    USING (SELECT * FROM RAW_DOW30_STAGING) AS SOURCE
    ON TARGET.YEAR = SOURCE.YEAR AND TARGET.MONTH = SOURCE.MONTH AND TARGET.DAY = SOURCE.DAY
    WHEN MATCHED THEN 
        UPDATE SET TARGET.CO2_EMISSION = SOURCE.CO2_EMISSION
    WHEN NOT MATCHED THEN 
        INSERT (YEAR, MONTH, DAY, DECIMAL_DATE, CO2_EMISSION) 
        VALUES (SOURCE.YEAR, SOURCE.MONTH, SOURCE.DAY, SOURCE.DECIMAL_DATE, SOURCE.CO2_EMISSION);
    
    RETURN 'Update Successful!';
END;
$$;
```


### Scheduled Task for Daily Updates
```sql
CREATE OR REPLACE TASK UPDATE_CO2_DATA
SCHEDULE = 'USING CRON 0 8 * * * UTC'
AS 
CALL UPDATE_CO2_PROCEDURE();

ALTER TASK UPDATE_CO2_DATA RESUME;
```

## **8️⃣ Using Jinja for SQL Templating**
Jinja helps create dynamic SQL queries with placeholders for variables.

### Example Jinja Template (`templates/load_data.sql.jinja`)
```sql
COPY INTO {{ table_name }}
FROM @{{ stage_name }}/{{ file_path }}
FILE_FORMAT = (FORMAT_NAME = '{{ file_format }}')
FORCE = TRUE;
```

### Using Jinja in Python
```python
from jinja2 import Environment, FileSystemLoader

env = Environment(loader=FileSystemLoader("templates"))
template = env.get_template("load_data.sql.jinja")

sql_query = template.render(
    table_name="RAW_DOW30_STAGING",
    stage_name="CO2_STAGE",
    file_path="raw_data/co2_data.csv",
    file_format="CO2_CSV_FORMAT"
)

print(sql_query)
```


## **9️⃣ Deployment with GitHub Actions**
We use GitHub Actions to automate pipeline execution.

### `.github/workflows/snowflake_ci.yml`
```yaml
name: Snowflake CI Pipeline
on:
  push:
    branches: [ main ]
jobs:
  snowflake_ci:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Repository
        uses: actions/checkout@v3
      - name: Install Dependencies
        run: |
          pip install snowflake-connector-python boto3 requests pandas "pyarrow<19.0.0"
      - name: Run Snowflake Pipeline
        run: python "co2_data_weather_data_pipeline/sql/snowflake_pipeline_1.py"
```

## **🔟🎯 Conclusion**
In this guide, we:  
✅ Fetched CO₂ data from NOAA  
✅ Stored it in AWS S3  
✅ Loaded & processed it in Snowflake  
✅ Built analytics reports  
✅ Automated data updates  
✅ Deployed via GitHub Actions  

📌 Repository: [[GitHub Link](https://github.com/hishitathakkar/BigData_assignment3)]  
📌 Author: Hishita Thakkar
