import requests
import pandas as pd
import boto3
from io import StringIO

BUCKET_NAME = "big.data.ass3"
S3_FILE_PATH = "co2_data/co2_dataset.csv"

def fetch_co2_data():
    response = requests.get("https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.txt")
    if response.status_code != 200:
        print("❌ Failed to fetch data from NOAA website")
        return None

    data = response.text.split("\n")
    data = [line for line in data if line and not line.startswith("#")]
    df = pd.DataFrame([row.split() for row in data])

    if df.shape[1] == 5:
        df.columns = ["year", "month", "day", "decimal_date", "co2"]
    else:
        print(f"❌ Unexpected number of columns: {df.shape[1]}")
        return None

    df = df.astype({"year": int, "month": int, "day": int, "decimal_date": float, "co2": float})
    print(f"✅ CO2 data fetched successfully with {df.shape[0]} rows.")

    return df

def upload_to_s3(df):
    s3_client = boto3.client("s3", region_name="us-east-1")  # replace with your actual region
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    try:
        s3_client.put_object(Bucket=BUCKET_NAME, Key=S3_FILE_PATH, Body=csv_buffer.getvalue())
        print(f"✅ Uploaded to S3 at s3://{BUCKET_NAME}/{S3_FILE_PATH}")
    except Exception as e:
        print(f"❌ Failed to upload to S3: {e}")

if __name__ == "__main__":
    df = fetch_co2_data()
    if df is not None:
        upload_to_s3(df)
