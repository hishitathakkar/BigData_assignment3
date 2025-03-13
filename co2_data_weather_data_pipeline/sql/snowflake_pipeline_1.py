import snowflake.connector
import os

# ✅ Fix Snowflake Account Name (Remove ".snowflakecomputing.com" if needed)
snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT", "").replace(".snowflakecomputing.com", "")

# 🏗️ Establish Snowflake Connection
conn = snowflake.connector.connect(
    account=snowflake_account,
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE")
)
cursor = conn.cursor()

# 1️⃣ **Set Database & Schema**
cursor.execute("USE DATABASE CO2_DB;")
cursor.execute("USE SCHEMA RAW_DATA;")

# 2️⃣ **Check & Create Storage Integration**
cursor.execute("SHOW STORAGE INTEGRATIONS;")
integrations = cursor.fetchall()
existing_integrations = [row[0] for row in integrations]

if "CO2_S3_INTEGRATION" not in existing_integrations:
    cursor.execute("""
        CREATE STORAGE INTEGRATION CO2_S3_INTEGRATION
        TYPE = EXTERNAL_STAGE
        STORAGE_PROVIDER = 'S3'
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::183295451617:role/MySnowflakeRole'
        ENABLED = TRUE
        STORAGE_ALLOWED_LOCATIONS = ('s3://big.data.ass3/');
    """)
print("✅ Storage Integration Checked.")

# 3️⃣ **Create External Stage & File Format**
cursor.execute("""
    CREATE OR REPLACE STAGE CO2_STAGE
    STORAGE_INTEGRATION = CO2_S3_INTEGRATION
    URL = 's3://big.data.ass3/';
""")
cursor.execute("""
    CREATE OR REPLACE FILE FORMAT CO2_CSV_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE;
""")

# 4️⃣ **Create Raw Data Table (Staging)**
cursor.execute("""
    CREATE OR REPLACE TABLE RAW_DOW30_STAGING (
        YEAR INT,
        MONTH INT,
        DAY INT,
        DECIMAL_DATE FLOAT,
        CO2_EMISSION FLOAT
    );
""")

# 5️⃣ **Load Data from S3**
cursor.execute("""
    COPY INTO RAW_DOW30_STAGING
    FROM @CO2_STAGE/raw_data/co2_data_v2.csv
    FILE_FORMAT = (FORMAT_NAME = 'CO2_CSV_FORMAT')
    FORCE = TRUE;
""")
print("✅ Data Loaded into RAW_DOW30_STAGING.")

# 6️⃣ **Create Stream for Incremental Updates**
cursor.execute("CREATE OR REPLACE STREAM CO2_STREAM ON TABLE RAW_DOW30_STAGING;")

# 7️⃣ **Switch to PUBLIC Schema for Transformations**
cursor.execute("USE SCHEMA PUBLIC;")

# 8️⃣ **Create Harmonized Data Table**
cursor.execute("""
    CREATE OR REPLACE TABLE DOW30_HARMONIZED AS
    SELECT YEAR, MONTH, DAY, DECIMAL_DATE, CO2_EMISSION FROM RAW_DOW30_STAGING;
""")

# 9️⃣ **Add Normalized Column & Function**
cursor.execute("ALTER TABLE DOW30_HARMONIZED ADD COLUMN NORMALIZED_CO2 FLOAT;")
cursor.execute("""
    CREATE OR REPLACE FUNCTION NORMALIZE_CO2_EMISSION(CO2_VALUE FLOAT, REFERENCE_VALUE FLOAT)
    RETURNS FLOAT AS $$ CO2_VALUE / REFERENCE_VALUE $$;
""")
cursor.execute("""
    UPDATE DOW30_HARMONIZED
    SET NORMALIZED_CO2 = NORMALIZE_CO2_EMISSION(CO2_EMISSION, 400.0);
""")

# 🔟 **Switch to ANALYTICS_DOW30 Schema for Reports**
cursor.execute("USE SCHEMA ANALYTICS_DOW30;")

# 1️⃣1️⃣ **Create Analytics Table**
cursor.execute("""
    CREATE OR REPLACE TABLE ANALYTICS_DOW30 AS
    SELECT YEAR, MONTH, AVG(CO2_EMISSION) AS AVG_CO2, MAX(CO2_EMISSION) AS MAX_CO2, MIN(CO2_EMISSION) AS MIN_CO2
    FROM PUBLIC.DOW30_HARMONIZED
    GROUP BY YEAR, MONTH;
""")

# 1️⃣2️⃣ **Create & Execute Stored Procedure for Updates**
cursor.execute("""
    CREATE OR REPLACE PROCEDURE UPDATE_DOW30_SP()
    RETURNS STRING LANGUAGE SQL AS $$
    BEGIN
        MERGE INTO PUBLIC.DOW30_HARMONIZED AS TARGET
        USING (SELECT * FROM RAW_DATA.CO2_STREAM) AS SOURCE
        ON TARGET.YEAR = SOURCE.YEAR
        WHEN MATCHED THEN UPDATE SET TARGET.CO2_EMISSION = SOURCE.CO2_EMISSION
        WHEN NOT MATCHED THEN INSERT (YEAR, MONTH, DAY, DECIMAL_DATE, CO2_EMISSION)
        VALUES (SOURCE.YEAR, SOURCE.MONTH, SOURCE.DAY, SOURCE.DECIMAL_DATE, SOURCE.CO2_EMISSION);
        RETURN 'Update Successful!';
    END;
    $$;
""")
cursor.execute("CALL UPDATE_DOW30_SP();")

# 1️⃣3️⃣ **Schedule & Enable Update Task**
cursor.execute("""
    CREATE OR REPLACE TASK UPDATE_DOW30_METRICS_TASK
    SCHEDULE = 'USING CRON 0 12 * * * UTC'
    AS CALL UPDATE_DOW30_SP();
""")
cursor.execute("ALTER TASK UPDATE_DOW30_METRICS_TASK RESUME;")

# ✅ **Final Monitoring Queries**
cursor.execute("SELECT COUNT(*) FROM PUBLIC.DOW30_HARMONIZED;")
cursor.execute("SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY COMPLETED_TIME DESC LIMIT 10;")

# ✅ **Close Connection**
cursor.close()
conn.close()
print("✅ Snowflake pipeline completed successfully!")
