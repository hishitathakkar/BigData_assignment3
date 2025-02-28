import requests
import json
import time
from datetime import datetime

# NOAA API Endpoint
BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"

# Your NOAA API Token
API_TOKEN = "xAQdxHKrmmlCWSskgShQjkvWDnbGmTiL"

# Boston Station IDs
STATION_IDS = [
    "GHCND:USW00014739",  # Logan International Airport
    "GHCND:USW00014732",  # Blue Hill Observatory
    "GHCND:USW00014754",  # Norwood Memorial Airport
    "GHCND:USW00014764",  # Bedford Airport
    "GHCND:USW00014742",  # Beverly Municipal Airport
]

# Set date range
START_YEAR = 2020
CURRENT_YEAR = datetime.today().year

# Max retries on API errors
MAX_RETRIES = 3


def fetch_weather_data():
    """
    Fetches TAVG data for the given locations in yearly chunks, with retries on failure.
    """
    headers = {"token": API_TOKEN}
    weather_data = []

    for station in STATION_IDS:
        for year in range(START_YEAR, CURRENT_YEAR + 1):  # Skip 2025
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31" if year < CURRENT_YEAR else datetime.today().strftime("%Y-%m-%d")

            params = {
                "datasetid": "GHCND",
                "datatypeid": "TAVG",
                "startdate": start_date,
                "enddate": end_date,
                "stationid": station,
                "limit": 1000,
            }

            retries = 0
            while retries < MAX_RETRIES:
                response = requests.get(BASE_URL, headers=headers, params=params)

                if response.status_code == 200:
                    data = response.json()
                    weather_data.extend(data.get("results", []))
                    print(f"✅ Data fetched for {station} ({year})")
                    break  # Success, exit retry loop
                elif response.status_code == 503:
                    print(f"⚠️ 503 Error: Server unavailable for {station} ({year}). Retrying...")
                    retries += 1
                    time.sleep(5)  # Wait before retrying
                else:
                    print(f"❌ Error fetching data for {station} ({year}): {response.status_code}, {response.text}")
                    break  # Other errors, no retries

    # Save results to a JSON file
    with open("boston_tavg_2020_present.json", "w") as file:
        json.dump(weather_data, file, indent=4)

    print(f"✅ Data successfully saved to 'boston_tavg_2020_present.json'!")


if __name__ == "__main__":
    fetch_weather_data()
