# tento kód je upravená kopie kódu od Kačky Jaškové
# úpravy:
# - doplněné párování o parametr PSC
# - doplnění o testovací vzorek
# 
# format souboru - row 23-
# testovací vzorek - row 46
# parametry vstupní tabulky (názvy sloupců) - 23-25


import pandas as pd
import requests
import urllib.parse
import time
from bs4 import BeautifulSoup
import re  # Import the regular expression module

# --- Configuration ---
INPUT_EXCEL_FILE = 'sabi/zasilkovna_enriched (1).xlsx'  # Replace with your input file name
OUTPUT_EXCEL_FILE = INPUT_EXCEL_FILE.replace(".xlsx", "") + "_kod_obce.xlsx"

ADDRESS_COLUMN = 'branch_code'
CITY_COLUMN = 'city'

KOD_OBCE_COLUMN = "kod_obce"
SEARCH_TERM_COLUMN = 'search_term'
SEARCH_TYPE_COLUMN = 'search_type'

# API Endpoints
API_FULLTEXT_URL = 'https://vdp.cuzk.gov.cz/vdp/ruian/adresnimista/fulltext'
API_AMD_TO_KOD_OBCE_URL_TEMPLATE = 'https://vdp.cuzk.gov.cz/vdp/ruian/adresnimista/{}'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',  # Example User-Agent
    'Accept': 'application/json, text/javascript, */*; q=0.01',  # Indicate preference for JSON
    'Accept-Language': 'en-US,en;q=0.5',  # Example Accept-Language
    'Connection': 'keep-alive',
    'X-Requested-With': 'XMLHttpRequest'  # Often sent by AJAX requests in browsers
}


def get_address_alternatives(address_detail, city):
    full = f"{address_detail} {city}"
    no_num = f"{address_detail} {city}"
    no_num =  "".join(char for char in no_num if not char.isdigit() and char != '/')
    no_num = no_num.replace("nám.", "")
    no_street = f"{city}"
    no_city = f"{address_detail}"

    return[('full', full),('no_num', no_num),(['no_street', no_street]),('no_city', no_city)]

def get_address_code(address_detail, city):
    if pd.isna(address_detail) or pd.isna(city):
        print(f"Missing address detail or city: {address_detail}, {city}")
        return None, None, None

    address_alternatives = get_address_alternatives(address_detail, city)
    for search_type, address in address_alternatives:
        encoded_address = urllib.parse.quote(address)
        url = f"{API_FULLTEXT_URL}?adresa={encoded_address}"
        print(f"Fetching AMD for: {address} (URL: {url})")

        try:
            response = requests.get(url, timeout=30, headers=HEADERS)
            response.raise_for_status()
            data = response.json()

            # Assuming the first result is the most relevant
            if data and 'polozky' in data and data['polozky']:
                if data['polozky'][0].get('kod'):
                    address_code = data['polozky'][0]['kod']
                    print(f"\tFound AMD code: {address_code}")
                    return address_code, search_type, address
                else:
                    print(f"\tError finding AMD code: {data=}")
            else:
                print(f"\tNo AMD code found for address: {address}. Response: {data}")
        except Exception as e:
            print(f"\Error while fetching AMD code for {address}: {e}")
    return None, None, None


def get_kod_obce(address_code):
    if not address_code:
        return None

    url = API_AMD_TO_KOD_OBCE_URL_TEMPLATE.format(address_code)
    print(f"Fetching kod_obce for AMD: {address_code} (URL: {url})")

    try:
        response = requests.get(url, timeout=10, headers=HEADERS)
        response.raise_for_status()

        try:
          target_href_pattern = re.compile(r"/vdp/ruian/obce/")
          soup = BeautifulSoup(response.text, 'html.parser')
          obec_href = soup.find('a', href=target_href_pattern).get('href')
          matched_id = re.search(r"/vdp/ruian/obce/(\d+)", obec_href)

          return int(matched_id.group(1))
        except Exception as e:
            print(f"ParseError for AMD {address_code}: {e}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching kod_obce for AMD {address_code}: {e}")
        return None


def match_address_to_city_code():
    print(f"Starting script. Reading input file: {INPUT_EXCEL_FILE}")
    try:
        df = pd.read_excel(INPUT_EXCEL_FILE)
        print(f"Successfully read {len(df)} rows from {INPUT_EXCEL_FILE}")
    except FileNotFoundError:
        print(f"Error: Input file '{INPUT_EXCEL_FILE}' not found. Please make sure it's in the same directory as the script or provide the full path.")
        return
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    if ADDRESS_COLUMN not in df.columns or CITY_COLUMN not in df.columns:
        print(f"Error: Required columns '{ADDRESS_COLUMN}' or '{CITY_COLUMN}' not found in the Excel file: {df.columns.tolist()}")
        return

    kod_obce_list = []
    search_term_list = []
    search_type_list = []
    for index, row in df.iterrows():
        print(f"\nProcessing row {index + 1}/{len(df)}...")
        address_detail = row[ADDRESS_COLUMN]
        city = row[CITY_COLUMN]

        address_code, search_type, address = get_address_code(address_detail, city)
        kod_obce = get_kod_obce(address_code) if address_code else None

        kod_obce_list.append(kod_obce)
        search_type_list.append(search_type)
        search_term_list.append(address)
        time.sleep(0.1)  # Be respectful to the API

    df[KOD_OBCE_COLUMN] = kod_obce_list
    df[SEARCH_TERM_COLUMN] = search_term_list
    df[SEARCH_TYPE_COLUMN] = search_type_list
    found_count = sum(1 for kod in kod_obce_list if pd.notna(kod))
    print(f"\nProcessed all rows. Found kod_obce for {found_count}/{len(kod_obce_list)} addresses.")

    try:
        df.to_excel(OUTPUT_EXCEL_FILE, index=False)
        print(f"Successfully saved updated data to '{OUTPUT_EXCEL_FILE}'")
    except Exception as e:
        print(f"Error saving Excel file: {e}")


if __name__ == "__main__":
    match_address_to_city_code()