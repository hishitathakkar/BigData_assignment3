import requests
import pandas as pd

URL = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.txt"

def fetch_co2_data():
    response = requests.get(URL)
    data = response.text.split("\n")

    # Remove comment lines (starting with #) and empty lines
    data = [line for line in data if not line.startswith("#") and line.strip()]

    # Convert text data into a DataFrame
    df = pd.DataFrame([line.split() for line in data])

    # Print the number of detected columns
    print(f"Detected {df.shape[1]} columns")

    # Assign column names based on actual structure (5 columns)
    df.columns = ["year", "month", "day", "decimal_date", "co2"]

    # Convert numeric columns to proper data types
    df = df.astype({
        "year": int,
        "month": int,
        "day": int,
        "decimal_date": float,
        "co2": float
    })

    return df

df = fetch_co2_data()
print(df.head())  # Display first few rows
