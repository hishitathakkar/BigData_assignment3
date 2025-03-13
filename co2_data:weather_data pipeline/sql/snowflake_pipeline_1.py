import snowflake.connector
import os

# ✅ Fix Snowflake Account Name (Remove ".snowflakecomputing.com" if needed)
snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT", "")
snowflake_account = snowflake_account.replace(".snowflakecomputing.com", "")

# 🏗️ Establish Snowflake Connection
conn = snowflake.connector.connect(
    account=snowflake_account,  # ✅ Fixed format
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role="ACCOUNTADMIN"
)
cursor = conn.cursor()

# 1️⃣ **Check if Storage Integration Already Exists**
cursor.execute("SHOW STORAGE INTEGRATIONS;")
integrations = cursor.fetchall()
existing_integrations = [row[0] for row in integrations]

if "CO2_S3_INTEGRATION" not in existing_integrations:
    print("🚀 Creating CO2_S3_INTEGRATION since it does not exist...")
    cursor.execute("""
        CREATE STORAGE INTEGRATION CO2_S3_INTEGRATION
        TYPE = EXTERNAL_STAGE
        STORAGE_PROVIDER = 'S3'
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::183295451617:role/MySnowflakeRole'
        ENABLED = TRUE
        STORAGE_ALLOWED_LOCATIONS = ('s3://big.data.ass3/');
    """)
    print("✅ Storage Integration Created.")
else:
    print("✅ Storage Integration CO2_S3_INTEGRATION already exists. Skipping creation.")

# ✅ Proceed with Creating Stage
cursor.execute("""
    CREATE OR REPLACE STAGE CO2_STAGE
    STORAGE_INTEGRATION = CO2_S3_INTEGRATION
    URL = 's3://big.data.ass3/';
""")

# ✅ Create File Format for CSV
cursor.execute("""
    CREATE OR REPLACE FILE FORMAT CO2_CSV_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE;
""")

# ✅ Create Raw Data Table
cursor.execute("""
    CREATE OR REPLACE TABLE RAW_DOW30_STAGING (
        YEAR INT,
        MONTH INT,
        DAY INT,
        DECIMAL_DATE FLOAT,
        CO2_EMISSION FLOAT
    );
""")

# ✅ Load Data from S3 to Snowflake
cursor.execute("""
    COPY INTO RAW_DOW30_STAGING
    FROM @CO2_STAGE/raw_data/co2_data_v2.csv
    FILE_FORMAT = (FORMAT_NAME = 'CO2_CSV_FORMAT')
    FORCE = TRUE;
""")
print("✅ Data loaded into RAW_DOW30_STAGING")

# ✅ Create Harmonized Data Table
cursor.execute("""
    CREATE OR REPLACE TABLE DOW30_HARMONIZED AS
    SELECT YEAR, MONTH, DAY, DECIMAL_DATE, CO2_EMISSION FROM RAW_DOW30_STAGING;
""")

# ✅ Add Normalized Column
cursor.execute("""
    ALTER TABLE DOW30_HARMONIZED ADD COLUMN NORMALIZED_CO2 FLOAT;
""")

# ✅ Insert Normalized Data
cursor.execute("""
    INSERT INTO DOW30_HARMONIZED (YEAR, MONTH, DAY, CO2_EMISSION, NORMALIZED_CO2)
    SELECT 
        YEAR, 
        MONTH, 
        DAY, 
        CO2_EMISSION,
        CO2_EMISSION / 400.0 AS NORMALIZED_CO2
    FROM RAW_DOW30_STAGING;
""")

# ✅ Create Analytics Table
cursor.execute("""
    CREATE OR REPLACE TABLE ANALYTICS_DOW30 AS
    SELECT 
        YEAR, 
        MONTH, 
        AVG(CO2_EMISSION) AS AVG_CO2, 
        MAX(CO2_EMISSION) AS MAX_CO2, 
        MIN(CO2_EMISSION) AS MIN_CO2
    FROM DOW30_HARMONIZED
    GROUP BY YEAR, MONTH;
""")

# ✅ Create Python Function for Percent Change Calculation
cursor.execute("""
    CREATE OR REPLACE FUNCTION CALCULATE_PERCENT_CHANGE(
    current_value FLOAT, previous_value FLOAT
) RETURNS FLOAT 
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'calculate_change'
AS $$
def calculate_change(current_value, previous_value):
    if previous_value == 0 or previous_value is None:
        return None
    return ((current_value - previous_value) / previous_value) * 100
$$;

""")

# ✅ Create Daily Percent Change Table
cursor.execute("""
    CREATE OR REPLACE TABLE CO2_DAILY_PERCENT_CHANGE AS
    SELECT 
        YEAR, 
        MONTH, 
        CO2_EMISSION,
        CALCULATE_PERCENT_CHANGE(CO2_EMISSION, LAG(CO2_EMISSION) OVER (ORDER BY YEAR, MONTH)) 
        AS DAILY_PERCENT_CHANGE
    FROM DOW30_HARMONIZED;
""")

# ✅ Create Python Function for Weekly Percent Change
cursor.execute("""
    CREATE OR REPLACE FUNCTION CALCULATE_WEEKLY_PERCENT_CHANGE(
    current_value FLOAT, week_ago_value FLOAT
) RETURNS FLOAT 
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'calculate_change'
AS $$
def calculate_change(current_value, week_ago_value):
    if week_ago_value == 0 or week_ago_value is None:
        return None
    return ((current_value - week_ago_value) / week_ago_value) * 100
$$;
""")

# ✅ Create Weekly Percent Change Table
cursor.execute("""
    CREATE OR REPLACE TABLE CO2_WEEKLY_PERCENT_CHANGE AS
    SELECT 
        YEAR, 
        MONTH, 
        CO2_EMISSION,
        CALCULATE_WEEKLY_PERCENT_CHANGE(
            CO2_EMISSION, 
            LAG(CO2_EMISSION, 7) OVER (ORDER BY YEAR, MONTH, DAY)
        ) AS WEEKLY_PERCENT_CHANGE
    FROM DOW30_HARMONIZED;
""")

# ✅ Create Stored Procedure to Update Data
cursor.execute("""
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
""")

# ✅ Create Task for Daily Updates
cursor.execute("""
    CREATE OR REPLACE TASK UPDATE_CO2_DATA
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
    AS 
    CALL UPDATE_CO2_PROCEDURE();
""")
cursor.execute("ALTER TASK UPDATE_CO2_DATA RESUME;")

# ✅ Final Monitoring Queries
cursor.execute("SELECT COUNT(*) FROM DOW30_HARMONIZED;")
cursor.execute("SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY COMPLETED_TIME DESC LIMIT 10;")

# ✅ Close Connection
cursor.close()
conn.close()
print("✅ Snowflake pipeline completed successfully!")
