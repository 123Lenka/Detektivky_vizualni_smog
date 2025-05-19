import pandas as pd
import requests
import urllib.parse
import time
from bs4 import BeautifulSoup
import re
import json  # Import knihovny json pro práci s JSON daty

# --- Konfigurace ---
INPUT_FILE = 'dpd_getAll_country203.json'  # Změň na název tvého JSON souboru
OUTPUT_FILE = INPUT_FILE.replace(".json", "") + "_kod_obce.csv"

# Vstup - tvůj JSON soubor
DATA_ITEMS_PATH = ['data', 'items']  # Cesta k listu s daty
CITY_COL = 'city'
STREET_COL = 'street'
HOUSE_COL = 'house_number'
PSC_COL = 'postcode'

# Výstup
KOD_ADRESY_COLUMN = "kod_adresy"
KOD_OBCE_COLUMN = "kod_obce"
SEARCH_TERM_COLUMN = 'search_term'
SEARCH_TYPE_COLUMN = 'search_type'
SEARCH_MATCHES_COLUMN = 'search_matches_cnt'

# API Endpoints (zůstávají stejné)
API_FULLTEXT_URL = 'https://vdp.cuzk.gov.cz/vdp/ruian/adresnimista/fulltext'
API_AMD_TO_KOD_OBCE_URL_TEMPLATE = 'https://vdp.cuzk.gov.cz/vdp/ruian/adresnimista/{}'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'X-Requested-With': 'XMLHttpRequest'
}


def get_address_alternatives(data_item):
    """Generuje různé podoby adresy z jednoho záznamu JSON."""
    if not data_item:
        print("\tSkipping empty item")
        return []
    address_detail = ""
    city_plus_code = ""
    if STREET_COL in data_item:
        address_detail = f"{data_item[STREET_COL]}"
        if HOUSE_COL in data_item:
            address_detail = address_detail + f" {data_item[HOUSE_COL]}"
    if CITY_COL in data_item:
        city_plus_code = f"{data_item[CITY_COL]}"
        if PSC_COL in data_item:
            city_plus_code = city_plus_code + f" {data_item[PSC_COL]}"

    simplified_address = f"{address_detail} {city_plus_code}"
    simplified_address = "".join(char for char in simplified_address if not char.isdigit() and char != '/')
    simplified_address = simplified_address.replace("nám.", "")

    return [('full_address', f"{address_detail} {city_plus_code}"),
            ('simplified_address', simplified_address),
            ('city_psc_only', city_plus_code),
            ('street_num_only', address_detail)]


def get_address_code(data_item):
    """Získá kód adresního místa (AMD) pomocí API."""
    address_alternatives = get_address_alternatives(data_item)
    for search_type, address in address_alternatives:
        encoded_address = urllib.parse.quote(address)
        url = f"{API_FULLTEXT_URL}?adresa={encoded_address}"
        print(f"\tFetching AMD for: {address} (URL: {url})")

        try:
            response = requests.get(url, timeout=30, headers=HEADERS)
            response.raise_for_status()
            data = response.json()

            if data and 'polozky' in data and data['polozky']:
                num_matches = len(data['polozky'])
                if data['polozky'][0].get('kod'):
                    address_code = data['polozky'][0]['kod']
                    print(f"\tFound AMD code: {address_code}")
                    return {KOD_ADRESY_COLUMN: address_code, SEARCH_TYPE_COLUMN: search_type, SEARCH_TERM_COLUMN: address, SEARCH_MATCHES_COLUMN: num_matches}
                else:
                    print(f"\tError finding AMD code: {data=}")
            else:
                print(f"\tNo AMD code found for address: {address}. Response: {data}")
        except requests.exceptions.RequestException as e:
            print(f"\tError while fetching AMD code for {address}: {e}")
        except json.JSONDecodeError as e:
            print(f"\tError decoding JSON response for {address}: {e}, Response text: {response.text}")
    return {}


def get_kod_obce(address_code):
    """Získá kód obce z kódu adresního místa pomocí API."""
    if not address_code:
        return None

    url = API_AMD_TO_KOD_OBCE_URL_TEMPLATE.format(address_code)
    print(f"\tFetching kod_obce for AMD: {address_code} (URL: {url})")

    try:
        response = requests.get(url, timeout=10, headers=HEADERS)
        response.raise_for_status()

        try:
            target_href_pattern = re.compile(r"/vdp/ruian/obce/")
            soup = BeautifulSoup(response.text, 'html.parser')
            obec_href = soup.find('a', href=target_href_pattern).get('href')
            matched_id = re.search(r"/vdp/ruian/obce/(\d+)", obec_href)

            return int(matched_id.group(1))
        except AttributeError:
            print(f"\tCould not find obec link in response for AMD {address_code}")
            return None
        except Exception as e:
            print(f"\tParseError for AMD {address_code}: {e}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"\tError fetching kod_obce for AMD {address_code}: {e}")
        return None


def match_address_to_city_code_from_json():
    """Hlavní funkce pro zpracování JSON souboru a získání kódů obcí."""
    print(f"Starting script. Reading input file: {INPUT_FILE}")
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        data_items = json_data
        for key in DATA_ITEMS_PATH:
            if key in data_items:
                data_items = data_items[key]
            else:
                print(f"Error: Key '{key}' not found in JSON structure.")
                return

        print(f"Successfully loaded {len(data_items)} items from {INPUT_FILE}")

    except FileNotFoundError:
        print(f"Error: Input file '{INPUT_FILE}' not found. Please make sure it's in the same directory as the script or provide the full path.")
        return
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON file: {e}")
        return
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    output_list = []
    for index, item in enumerate(data_items):
        print(f"\nProcessing item {index + 1}/{len(data_items)}...")
        output_dict = get_address_code(item)
        kod_obce = get_kod_obce(output_dict.get(KOD_ADRESY_COLUMN))
        output_dict.update({KOD_OBCE_COLUMN: kod_obce})
        output_list.append(output_dict)
        time.sleep(0.1)  # Be respectful to the API

    enriched_df = pd.DataFrame(output_list)
    # Propojíme s původními daty (pokud je to potřeba)
    original_df = pd.json_normalize(data_items) # Převod listu slovníků na DataFrame
    final_df = pd.concat([original_df, enriched_df], axis=1)

    valid_cnt = (~final_df[KOD_OBCE_COLUMN].isna()).sum()
    print(f"\nProcessed all items. Found kod_obce for {valid_cnt}/{len(final_df)} addresses.")

    try:
        final_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        print(f"Successfully saved updated data to '{OUTPUT_FILE}'")
    except Exception as e:
        print(f"Error saving file: {e}")


if __name__ == "__main__":
    match_address_to_city_code_from_json()