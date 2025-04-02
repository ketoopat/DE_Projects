import requests
import json
import pandas as pd


headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

url_district = "https://infobanjirjps.selangor.gov.my/JPSAPI/api/StationRiverLevels/GetWLDistricts"


def fetch_districtId():
    response = requests.get(url_district, headers=headers, verify=False)
    if response.status_code == 200:
        return response.json()
    
    else:
        print("unable to scrape District IDs, using hard-coded version...")
        x = """[
  {
    "id": 1,
    "name": "KUALA SELANGOR",
    "order": 0
  },
  {
    "id": 4,
    "name": "SEPANG",
    "order": 1
  },
  {
    "id": 2,
    "name": "SABAK BERNAM",
    "order": 2
  },
  {
    "id": 3,
    "name": "HULU LANGAT",
    "order": 4
  },
  {
    "id": 5,
    "name": "KUALA LANGAT",
    "order": 5
  },
  {
    "id": 6,
    "name": "KLANG",
    "order": 6
  },
  {
    "id": 7,
    "name": "PETALING",
    "order": 7
  },
  {
    "id": 9,
    "name": "HULU SELANGOR",
    "order": 8
  },
  {
    "id": 8,
    "name": "GOMBAK",
    "order": 9
  }
]"""

    return json.loads(x)

def getdistrict_id():
    district_data = fetch_districtId()
    return {item['id']: item['name'] for item in district_data}


def main():
    district_data = fetch_districtId()

    district_details = dict(sorted({item['id']:item['name'] for item in district_data}.items()))

    print(district_details)


if __name__ == "__main__":
    main()
