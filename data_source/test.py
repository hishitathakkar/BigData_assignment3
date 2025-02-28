import requests
import json
import time
from datetime import datetime

# NOAA API Endpoint

#BASE_URL = 'https://www.ncei.noaa.gov/cdo-web/api/v2/locations?locationcategoryid=ZIP&sortfield=name&sortorder=asc'
BASE_URL = 'https://www.ncei.noaa.gov/cdo-web/api/v2/stations/GHCND:USW00014732'

#BASE_URL = 'https://www.ncei.noaa.gov/cdo-web/api/v2/stations'


# Boston Station IDs
# "GHCND:USW00014739",  # LOGAN INTERNATIONAL AIRPORT, MA US
# "GHCND:USR0000MCAP", # CAPE COD MASSACHUSETTS, MA US
# "GHCND:USW00094746", # WORCESTER REGIONAL AIRPORT, MA US


# Your NOAA API Token
API_TOKEN = ''


def fetch_weather_data():
    """
    Fetches TAVG data for the given locations in yearly chunks, with retries on failure.
    """
    headers = {"token": API_TOKEN}

    #"id": "ZIP:02116", "id": "ZIP:02127", "id": "ZIP:02128"
    #"id": "ZIP:02138", "id": "ZIP:02139", "id": "ZIP:02140", 

    #"id": "ZIP:02128"
    weather_data = []

    params = {                
        "datasetid": "GHCND",
        "datatypeid": "TAVG",
        #"startdate": "2020-01-01",
        #"enddate": "2025-02-20",
        #"extend": "42.2,-71.2,42.4,-70.9",
        #"offset": 1100,
        #"locationid": "FIPS:25",
        #"locationid": "ZIP:02140",
        #"offset": 2500,
        "limit": 1000
    }

    response = requests.get(BASE_URL, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        print(data)
        weather_data.extend(data.get("results", []))
        print(f"✅ Data fetched")
    else:
        print(f"❌ Error fetching data): {response.status_code}, {response.text}")

    with open("stations.json", "w") as file:
        json.dump(weather_data, file, indent=4)


fetch_weather_data()