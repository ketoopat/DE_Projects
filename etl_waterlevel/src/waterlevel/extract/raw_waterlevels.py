import requests
import json
import pandas as pd
from waterlevel.extract.district_id import getdistrict_id
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

district_map = getdistrict_id()
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
url_waterlevel = "https://infobanjirjps.selangor.gov.my/JPSAPI/api/StationRiverLevels/GetWLAllStationData/{}"

def fetch_waterlevels():

    all_stations = []

    for id, name in district_map.items():
        url = url_waterlevel.format(id)
        response = requests.get(url,headers=headers, verify=False)
        data = response.json()

        stations = data.get("stations", [])
        all_stations.extend(stations)
    return all_stations

def main():
    raw_waterlevels = fetch_waterlevels()
    print(raw_waterlevels)


if __name__ == "__main__":
    main()