# tento kód je upravená kopie kódu od Kačky Jaškové
# úpravy:
# - doplněné párování o parametr PSC
# - doplnění o testovací vzorek
# - přidáno doplnění o ulici i č.p z RUAIAN, jen pro kontrolu (není logické vyhle
#dávat č.p., nehce se mi to z kódu mazat.)
# 
# format souboru - row 23-
# testovací vzorek - row 46
# parametry vstupní tabulky (názvy sloupců) - 23-25


import pandas as pd
import requests
import urllib.parse
import time
from bs4 import BeautifulSoup
import re

# --- Konfigurace ---
INPUT_EXCEL_FILE = 'data/unikatni_ppl_adresy.xlsx'  # Replace with your input file name
OUTPUT_EXCEL_FILE = INPUT_EXCEL_FILE.replace(".xlsx", "") + "_kod_obce.xlsx"

# Vstup - tvůj xlsx soubor
ADDRESS_COLUMN = 'ulice'
CITY_COLUMN = 'mesto'
PSC_COLUMN = 'psc'

CITY_CODE_COLUMN = "city_code"
CITY_NAME_COLUMN = "city_name"
SEARCH_TERM_COLUMN = 'search_term'
SEARCH_TYPE_COLUMN = 'search_type'
ULICE_RUIAN_COLUMN = "ulice_z_ruian"
CISLO_CP_RUIAN_COLUMN = "cislo_popisne_z_ruian"

API_FULLTEXT_URL = 'https://vdp.cuzk.gov.cz/vdp/ruian/adresnimista/fulltext'
API_AMD_TO_KOD_OBCE_URL_TEMPLATE = 'https://vdp.cuzk.gov.cz/vdp/ruian/adresnimista/{}'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'cs-CZ,cs;q=0.9',
    'Connection': 'keep-alive',
    'X-Requested-With': 'XMLHttpRequest'
}

MAX_ROWS = None  # nastav na None pro celý soubor

def get_address_alternatives(address_detail, city, psc):
    full = f"{address_detail} {city} {psc}"
    no_num = "".join(char for char in address_detail if not char.isdigit() and char != '/')
    no_num = no_num.replace("nám.", "")
    no_num_full = f"{no_num} {city} {psc}"
    no_street = f"{city} {psc}"
    no_city = f"{address_detail} {psc}"

    return [
        ('full', full),
        ('no_num', no_num_full),
        ('no_street', no_street),
        ('no_city', no_city)
    ]

def get_address_code(address_detail, city, psc):
    if pd.isna(address_detail) or pd.isna(city) or pd.isna(psc):
        print(f"Chybí adresa, město nebo PSČ: {address_detail}, {city}, {psc}")
        return None, None, None, None, None

    address_alternatives = get_address_alternatives(address_detail, city, psc)
    for search_type, address in address_alternatives:
        encoded_address = urllib.parse.quote(address)
        url = f"{API_FULLTEXT_URL}?adresa={encoded_address}"
        print(f"Hledání AMD pro: {address} (URL: {url})")

        try:
            response = requests.get(url, timeout=30, headers=HEADERS)
            response.raise_for_status()
            data = response.json()

            if data and 'polozky' in data and data['polozky']:
                for polozka in data['polozky']:
                    if 'kod' in polozka and 'adresa' in polozka:
                        adresa_info = polozka['adresa']
                        psc_api = adresa_info.get('psc')
                        if psc_api and str(psc_api).strip() == str(psc).strip():
                            address_code = polozka['kod']
                            ulice_z_ruian = adresa_info.get('nazevUlice', None)
                            cp_z_ruian = adresa_info.get('cisloDomovni', None)
                            print(f"\tNalezena shoda: AMD {address_code}, ulice: {ulice_z_ruian}, č.p.: {cp_z_ruian}")
                            return address_code, search_type, address, ulice_z_ruian, cp_z_ruian
                # fallback: první výsledek
                first = data['polozky'][0]
                address_code = first.get('kod')
                adresa_info = first.get('adresa', {})
                ulice_z_ruian = adresa_info.get('nazevUlice', None)
                cp_z_ruian = adresa_info.get('cisloDomovni', None)
                print(f"\tBez shody PSČ – použit 1. výsledek: AMD {address_code}, ulice: {ulice_z_ruian}, č.p.: {cp_z_ruian}")
                return address_code, search_type, address, ulice_z_ruian, cp_z_ruian
            else:
                print(f"\tŽádný výsledek pro adresu: {address}")
        except Exception as e:
            print(f"Chyba při získávání AMD pro {address}: {e}")
    return None, None, None, None, None

def get_kod_obce(address_code):
    if not address_code:
        return None, None

    url = API_AMD_TO_KOD_OBCE_URL_TEMPLATE.format(address_code)
    print(f"Získávání kódu obce pro AMD: {address_code} (URL: {url})")

    try:
        response = requests.get(url, timeout=10, headers=HEADERS)
        response.raise_for_status()

        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            obec_link = soup.find('a', href=re.compile(r"/vdp/ruian/obce/"))
            if obec_link:
                matched_id = re.search(r"/vdp/ruian/obce/(\d+)", obec_link['href'])
                if matched_id:
                    kod_obce = int(matched_id.group(1))
                    nazev_obce = obec_link.text.strip()
                    return kod_obce, nazev_obce
        except Exception as e:
            print(f"Chyba při parsování HTML: {e}")
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"Chyba při získávání kódu obce: {e}")
        return None, None

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

    if ADDRESS_COLUMN not in df.columns or CITY_COLUMN not in df.columns or PSC_COLUMN not in df.columns:
        print(f"Chyba: Některý z požadovaných sloupců chybí.")
        return

    if MAX_ROWS is not None:
        df = df.head(MAX_ROWS)

    city_code_list = []
    city_name_list = []
    search_term_list = []
    search_type_list = []
    address_list = []
    ulice_z_ruian_list = []
    cp_z_ruian_list = []

    for index, row in df.iterrows():
        print(f"\nZpracovávám řádek {index + 1}/{len(df)}")
        address_detail = row[ADDRESS_COLUMN]
        city = row[CITY_COLUMN]
        psc = row[PSC_COLUMN]

        address_code, search_type, address, ulice_z_ruian, cp_z_ruian = get_address_code(address_detail, city, psc)
        city_code, city_name = get_kod_obce(address_code) if address_code else (None, None)

        city_code_list.append(city_code)
        city_name_list.append(city_name)
        search_type_list.append(search_type)
        search_term_list.append(address)
        address_list.append(address_detail)
        ulice_z_ruian_list.append(ulice_z_ruian)
        cp_z_ruian_list.append(cp_z_ruian)

        time.sleep(0.1)

    df[CITY_CODE_COLUMN] = city_code_list
    df[CITY_NAME_COLUMN] = city_name_list
    df[SEARCH_TERM_COLUMN] = search_term_list
    df[SEARCH_TYPE_COLUMN] = search_type_list
    df['address_detail'] = address_list
    #df['adresa_kontrola'] = df[ADDRESS_COLUMN] + ", " + df[CITY_COLUMN] + " " + df[PSC_COLUMN]
    df[ULICE_RUIAN_COLUMN] = ulice_z_ruian_list
    df[CISLO_CP_RUIAN_COLUMN] = cp_z_ruian_list

    found_count = sum(1 for kod in city_code_list if pd.notna(kod))
    print(f"\nHotovo: nalezeno kódů obcí pro {found_count}/{len(df)} adres.")

    try:
        df.to_excel(OUTPUT_EXCEL_FILE, index=False)
        print(f"Successfully saved updated data to '{OUTPUT_EXCEL_FILE}'")
    except Exception as e:
        print(f"Error saving Excel file: {e}")

if __name__ == "__main__":
    match_address_to_city_code()
