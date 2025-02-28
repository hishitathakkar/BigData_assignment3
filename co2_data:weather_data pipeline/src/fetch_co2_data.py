import requests
import pandas as pd
import boto3
from io import StringIO

# AWS S3 Configurations
BUCKET_NAME = "big.data.ass3"  # Replace with your actual bucket
S3_FILE_PATH = "raw_data/co2_data_v2.csv"

# NOAA CO2 Data URL
URL = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.txt"

def fetch_co2_data():
    """Fetch CO₂ data from NOAA website, clean, and return as DataFrame."""
    response = requests.get(URL)
    
    if response.status_code != 200:
        print("❌ Failed to fetch data from NOAA website")
        return None
    
    data = response.text.split("\n")

    # Remove comment lines (starting with #) and empty lines
    data = [line for line in data if not line.startswith("#") and line.strip()]

    # Convert text data into a DataFrame
    df = pd.DataFrame([line.split() for line in data])

    # Print the number of detected columns
    print(f"✅ Detected {df.shape[1]} columns in NOAA dataset")

    # Assign column names based on actual structure (5 columns)
    if df.shape[1] == 5:
        df.columns = ["year", "month", "day", "decimal_date", "co2"]
    else:
        print("❌ Unexpected column count. Please check the data source.")
        return None

    # Convert numeric columns to proper data types
    df = df.astype({
        "year": int,
        "month": int,
        "day": int,
        "decimal_date": float,
        "co2": float
    })

    return df

def upload_to_s3(df):
    """Upload the CO₂ dataset to an S3 bucket as a CSV file."""
    s3_client = boto3.client("s3")
    csv_buffer = StringIO()
    
    # Save DataFrame to CSV format in memory
    df.to_csv(csv_buffer, index=False)

    # Upload the CSV file to S3
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=S3_FILE_PATH,
            Body=csv_buffer.getvalue()
        )
        print(f"✅ Uploaded CO₂ data to S3: s3://{BUCKET_NAME}/{S3_FILE_PATH}")
    except Exception as e:
        print(f"❌ Failed to upload data to S3: {e}")

if __name__ == "__main__":
    df = fetch_co2_data()
    
    if df is not None:
        upload_to_s3(df)
    else:
        print("❌ No data available for upload.")
