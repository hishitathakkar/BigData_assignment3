import snowflake.connector
import os

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cursor = conn.cursor()

def test_calculate_percent_change():
    query = """
    SELECT CO2_DB.ANALYTICS_DOW30.CALCULATE_PERCENT_CHANGE(426.11, 425.94);
    """
    cursor.execute(query)
    result = cursor.fetchone()[0]
    assert round(result, 5) == 0.03991  # Corrected expected percent change


def test_calculate_weekly_percent_change():
    """ Test the CALCULATE_WEEKLY_PERCENT_CHANGE UDF """
    query = """
    SELECT CO2_DB.ANALYTICS_DOW30.CALCULATE_WEEKLY_PERCENT_CHANGE(426.11, 425.50);
    """
    cursor.execute(query)
    result = cursor.fetchone()[0]
    print("Weekly Percent Change:", result)  # verification
    assert round(result, 5) == 0.14336  # Correct expected percent change




test_calculate_percent_change()
test_calculate_weekly_percent_change()

cursor.close()
conn.close()

print("✅ All tests passed!")
