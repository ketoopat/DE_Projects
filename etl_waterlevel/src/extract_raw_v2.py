import requests
import certifi
import pandas as pd
from lxml import etree
from sqlalchemy import create_engine
import pandas as pd
from config import DATABASE_URL
import re
import snowflake.connector

#### this file extracts data from publicinfobanjir, validates df using pandas, loads df into PostgreSQL for further transformation (split clean_df into tables etc.) ###

url = "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=SEL&district=ALL&station=ALL&lang=en"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

engine = create_engine(DATABASE_URL)


def fetch_data():
    # get URL to fetch data #
    response = requests.get(url, headers=headers, verify=False)
    if response.status_code == 200:
        return response.content
    
    else:
        print("scraper ada problem!!")
        return None


def parse_data(html_content):
    tree = etree.HTML(html_content)

    column_names = tree.xpath(
        "//table[@class='normaltable-fluid']/thead/tr/th[not(contains(text(), 'Threshold'))]/text()[1]"
    )

    rows = tree.xpath("//table[@class='normaltable-fluid']/tbody/tr[@class='item']")
    station_ids = tree.xpath("//table[@class='normaltable-fluid']/tbody/tr[@class='item']/td/a/@href")

    row_data = [
        [td.xpath("string(.)").strip() for td in row.xpath(".//td")] for row in rows
    ]

    # replace 2nd columns with station_ids
    for i, link in enumerate(station_ids):
        match = re.search(r"stationid=([a-zA-Z0-9]+)", link)
        if match and i < len(row_data):  # Ensure index does not exceed row_data length
            row_data[i][1] = match.group(1)  # Extracted station ID


    return column_names, row_data


def clean(raw_df):

    raw_df.dropna(how="all", inplace=True)

    # convert numeric col to float
    convert_cols = ["Water Level (m)", "Normal", "Alert", "Warning", "Danger"]

    for col in convert_cols:
        if col in raw_df.columns:
            raw_df[col] = pd.to_numeric(raw_df[col], errors="coerce")

    # clean column names, strip()
    raw_df.columns = [col.strip() for col in raw_df.columns]

    # Water Level (m) = -9999, replace as NaN
    raw_df['Water Level (m)'] = raw_df['Water Level (m)'].replace(-9999,pd.NA)


    return raw_df

def load_postgres(df):
    try:
        df.to_sql("raw_publicinfobanjir", con=engine, if_exists="replace", index=False)
        print("Data successfully stored in PostgreSQL.")
    except Exception as e:
        print(f"Error storing data: {e}")


def main():
    html_content = fetch_data()
    if html_content:
        column_names, row_data = parse_data(html_content)

        # store in df
        raw_df = pd.DataFrame(row_data, columns=column_names)
        clean_df = clean(raw_df)
        
        # load to postgres db
        load_postgres(clean_df)


if __name__ == "__main__":
    main()
