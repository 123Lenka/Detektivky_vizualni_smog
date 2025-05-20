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
import re

# --- Konfigurace ---
INPUT_CSV_FILE = r"C:\Users\Sabina\Sabi dokumenty\01 Czechitas\01 PYTHON\Python\Detektivky_vizualni_smog\sabi\zasilkovna_enriched - zasilkovna_enriched (1).csv"
OUTPUT_CSV_FILE = INPUT_CSV_FILE.replace(".csv", "") + "_kod_obce.csv"

ADDRESS_COLUMN = 'street'
CITY_COLUMN = 'City'
PSC_COLUMN = 'Psc'

CITY_CODE_COLUMN = "city_code"
CITY_NAME_COLUMN = "city_name"
SEARCH_TERM_COLUMN = 'search_term'
SEARCH_TYPE_COLUMN = 'search_type'

# API Endpoints
API_FULLTEXT_URL = 'https://vdp.cuzk.gov.cz/vdp/ruian/adresnimista/fulltext'
API_AMD_TO_KOD_OBCE_URL_TEMPLATE = 'https://vdp.cuzk.gov.cz/vdp/ruian/adresnimista/{}'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'cs-CZ,cs;q=0.9',
    'Connection': 'keep-alive',
    'X-Requested-With': 'XMLHttpRequest'
}

# --- Nastavení pro testování ---
# Pokud chcete zpracovat pouze prvních N řádků, nastavte MAX_ROWS na požadovanou hodnotu (např. 10).
# Pokud chcete zpracovat celý soubor, nastavte MAX_ROWS na None.
MAX_ROWS = 10  # Změňte na None pro zpracování celého souboru

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
        return None, None, None

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
                            print(f"\tNalezena shoda s PSČ: {psc_api}, AMD kód: {address_code}")
                            return address_code, search_type, address
                # Pokud žádná shoda PSČ, použij první výsledek
                first = data['polozky'][0]
                address_code = first.get('kod')
                print(f"\tŽádná shoda PSČ, použit první výsledek: AMD kód: {address_code}")
                return address_code, search_type, address
            else:
                print(f"\tŽádný výsledek pro adresu: {address}. Odpověď: {data}")
        except Exception as e:
            print(f"Chyba při získávání AMD kódu pro {address}: {e}")
    return None, None, None

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
            print(f"Chyba při parsování pro AMD {address_code}: {e}")
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"Chyba při získávání kódu obce pro AMD {address_code}: {e}")
        return None, None

def match_address_to_city_code():
    print(f"Spouštění skriptu. Načítání vstupního souboru: {INPUT_CSV_FILE}")
    try:
        # Načtení CSV s explicitním určením datového typu pro sloupec PSČ
        with open(INPUT_CSV_FILE, mode='r', encoding='windows-1250', errors='ignore') as f:
            df = pd.read_csv(f, dtype={PSC_COLUMN: str})
        # df = pd.read_csv(INPUT_CSV_FILE, dtype={PSC_COLUMN: str}, encoding='windows-1250')
        print(f"Úspěšně načteno {len(df)} řádků z {INPUT_CSV_FILE}")
    except FileNotFoundError:
        print(f"Chyba: Vstupní soubor '{INPUT_CSV_FILE}' nenalezen.")
        return
    except Exception as e:
        print(f"Chyba při čtení souboru: {e}")
        return

    if ADDRESS_COLUMN not in df.columns or CITY_COLUMN not in df.columns or PSC_COLUMN not in df.columns:
        print(f"Chyba: Požadované sloupce '{ADDRESS_COLUMN}', '{CITY_COLUMN}' nebo '{PSC_COLUMN}' nejsou v souboru.")
        return

    # Pokud je nastaveno omezení počtu řádků, aplikuj ho
    if MAX_ROWS is not None:
        df = df.head(MAX_ROWS)

    city_code_list = []
    city_name_list = []
    search_term_list = []
    search_type_list = []

    for index, row in df.iterrows():
        print(f"\nZpracování řádku {index + 1}/{len(df)}...")
        address_detail = row[ADDRESS_COLUMN]
        city = row[CITY_COLUMN]
        psc = row[PSC_COLUMN]

        address_code, search_type, address = get_address_code(address_detail, city, psc)
        city_code, city_name = get_kod_obce(address_code) if address_code else (None, None)

        city_code_list.append(city_code)
        city_name_list.append(city_name)
        search_type_list.append(search_type)
        search_term_list.append(address)
        time.sleep(0.1)  # Respektování API

    df[CITY_CODE_COLUMN] = city_code_list
    df[CITY_NAME_COLUMN] = city_name_list
    df[SEARCH_TERM_COLUMN] = search_term_list
    df[SEARCH_TYPE_COLUMN] = search_type_list
    found_count = sum(1 for kod in city_code_list if pd.notna(kod))
    print(f"\nZpracováno všech řádků. Nalezeno kódů obcí pro {found_count}/{len(city_code_list)} adres.")

    try:
        df.to_csv(OUTPUT_CSV_FILE, index=False)
        print(f"Úspěšně uložena aktualizovaná data do '{OUTPUT_CSV_FILE}'")
    except Exception as e:
        print(f"Chyba při ukládání souboru: {e}")

if __name__ == "__main__":
    match_address_to_city_code()