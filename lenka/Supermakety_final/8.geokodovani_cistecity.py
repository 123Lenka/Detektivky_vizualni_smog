import pandas as pd
import requests
import json
import re
import time
import urllib.parse

from bs4 import BeautifulSoup

# Z dalšího čištění a přiřazení vzešlo už jen 800 nepřiřazených hodnot. 
# Většinou dělají problém názvy měst typu Šlapanice u Brna, Hustopeče u Brna, 
# kde ale skutečný název obci je Hustopeče, tím u Brna chceme jen upřesnit které, ale kod to mate.
# Pokud by se ještě tato věc odčistila, měl by kod pro hledání ruian najit Hustopeče bez u Brna 
# a to by mohlo lépe fungovat.

# Špatně přiřazené řádky tedy znovu vyčištíme a propojíme

# --- Konfigurace cest k souborům ---
INPUT_FILE = '7.vybrane_supermarkety_nepodarilo_geokodovat_znovu.json' # Zde načítáme pouze ty negeokódované
OUTPUT_SUCCESS_JSON_PATH = '8.vybrane_supermarkety_regeokodovano_cistecity.json' # Všechny záznamy po tomto kroku
OUTPUT_FAILED_JSON_PATH = '8.vybrane_supermarkety_nepodarilo_geokodovat_cistecity.json'
OUTPUT_FAILED_CSV_PATH = '8.vybrane_supermarkety_nepodarilo_geokodovat_cistecity.csv'

# RÚIAN API konfigurace (dle uživatelova funkčního kódu - bez API klíče)
RUIAN_API_FULLTEXT_URL = "https://vdp.cuzk.gov.cz/vdp/ruian/adresnimista/fulltext"
RUIAN_API_AMD_TO_KOD_OBCE_URL_TEMPLATE = 'https://vdp.cuzk.gov.cz/vdp/ruian/adresnimista/{}'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'X-Requested-With': 'XMLHttpRequest'
}

# Sloupce pro kontrolu, zda je záznam geokódován
KOD_ADRESY_COLUMN = "ruian_kod_adresniho_mista"
KOD_OBCE_COLUMN = "ruian_kod_obce"
GEOCODING_STATUS_COLUMN = 'ruian_geocoding_status'
SEARCH_TERM_COLUMN = 'ruian_search_term'
SEARCH_TYPE_COLUMN = 'ruian_search_type'
MATCHES_COUNT_COLUMN = 'ruian_matches_count'

# --- Funkce pro čištění názvů obcí od předložek a doplňků ---
def clean_city_for_ruian(city_name):
    if not isinstance(city_name, str):
        return None
    
    # Seznam vzorů pro odstranění (pozor na pořadí, obecnější na konci)
    patterns = [
        r'\s+u\s+[A-ZÁČDĚÉĚÍŇÓŘŠŤÚŮÝŽCMNĚÍÝŘŠŤÚŮŽ]{1}[a-záčd-ěéěi-nóř-šťúůýžcmněíýřšťúůžA-ZÁČDĚÉĚÍŇÓŘŠŤÚŮÝŽCMNĚÍÝŘŠŤÚŮŽ]+\s*$', # u <Název>
        r'\s+na\s+[A-ZÁČDĚÉĚÍŇÓŘŠŤÚŮÝŽCMNĚÍÝŘŠŤÚŮŽ]{1}[a-záčd-ěéěi-nóř-šťúůýžcmněíýřšťúůžA-ZÁČDĚÉĚÍŇÓŘŠŤÚŮÝŽCMNĚÍÝŘŠŤÚŮŽ]+\s*$', # na <Název>
        r'\s+v\s+[A-ZÁČDĚÉĚÍŇÓŘŠŤÚŮÝŽCMNĚÍÝŘŠŤÚŮŽ]{1}[a-záčd-ěéěi-nóř-šťúůýžcmněíýřšťúůžA-ZÁČDĚÉĚÍŇÓŘŠŤÚŮÝŽCMNĚÍÝŘŠŤÚŮŽ]+\s*$', # v <Název>
        r'\s+nad\s+[A-ZÁČDĚÉĚÍŇÓŘŠŤÚŮÝŽCMNĚÍÝŘŠŤÚŮŽ]{1}[a-záčd-ěéěi-nóř-šťúůýžcmněíýřšťúůžA-ZÁČDĚÉĚÍŇÓŘŠŤÚŮÝŽCMNĚÍÝŘŠŤÚŮŽ]+\s*$', # nad <Název>
        r'\s+pod\s+[A-ZÁČDĚÉĚÍŇÓŘŠŤÚŮÝŽCMNĚÍÝŘŠŤÚŮÝŽCMNĚÍÝŘŠŤÚŮŽ]+\s*$', # pod <Název> (upraveno tak, aby to chytilo jakékoli slovo za pod)
        r'\s+v\s+Čechách\s*$',
        r'\s+v\s+Jeseníkách\s*$',
        r'\s+II+\s*$', # II, III atd.
        r'\s+I+\s*$',
        r'\s*\(.*\)\s*$', # cokoli v závorkách
        r',\s*Česko\s*$', # odstranění ", Česko"
        r'\s*k\.č\.\s*[0-9]+\s*$', # k.č. 123
    ]

    cleaned_city = city_name
    for pattern in patterns:
        cleaned_city = re.sub(pattern, '', cleaned_city, flags=re.IGNORECASE).strip()
    
    # Kontrola, zda po čištění zbylo jen číslo (a původně to nebyla jen číselná hodnota)
    # Pokud city_name je "126 Úsobí" a vyčistí se na "126", nebo "126.0", pak je to okay.
    # Pokud city_name je "Borotín u Boskovic" a omylem se vyčistí na číslo, pak je to problém.
    # Cílem je pustit číselné hodnoty jen pokud už byly v city_cleaned.
    if cleaned_city.replace('.', '', 1).isdigit() and not city_name.replace('.', '', 1).isdigit():
        return None # Pokud zbylo jen číslo a původně to nebylo číslo, pak je to špatné čištění
    
    return cleaned_city if cleaned_city else None # Vracíme None, pokud by zbylo prázdné


# --- Funkce pro získání kódu obce z kódu adresního místa (z uživatelova kódu) ---
def get_kod_obce_from_amd(address_code):
    if not address_code:
        return None

    url = RUIAN_API_AMD_TO_KOD_OBCE_URL_TEMPLATE.format(address_code)
    # print(f"\tFetching kod_obce for AMD: {address_code} (URL: {url})") # Debugging - vypnuto pro zkrácení výstupu

    try:
        response = requests.get(url, timeout=10, headers=HEADERS)
        response.raise_for_status()

        # Parse HTML to find the link to the municipality
        soup = BeautifulSoup(response.text, 'html.parser')
        target_href_pattern = re.compile(r"/vdp/ruian/obce/")
        
        # Find the link with href containing "/vdp/ruian/obce/"
        obec_href_tag = soup.find('a', href=target_href_pattern)
        
        if obec_href_tag:
            obec_href = obec_href_tag.get('href')
            matched_id = re.search(r"/vdp/ruian/obce/(\d+)", obec_href)
            if matched_id:
                return int(matched_id.group(1))
        # else:
            # print(f"\tParseError for AMD {address_code}: Could not find matching <a> tag for /vdp/ruian/obce/") # Debugging

    except requests.exceptions.RequestException as e:
        # print(f"\tError fetching kod_obce for AMD {address_code}: {e}") # Debugging
        pass # Handle silently, status will reflect failure
    except Exception as e: # Catch all other parsing errors
        # print(f"\tParseError for AMD {address_code}: {e}") # Debugging
        pass
    return None

# --- Funkce pro geokódování jednoho záznamu s novou strategií ---
def geocode_record_with_cleaned_city_improved(record):
    city_raw = record.get('city_cleaned')
    street_cleaned = record.get('street_cleaned')
    zip_code_cleaned = record.get('zip_code_cleaned')

    # Inicializace RÚIAN sloupců pro tento záznam
    record[GEOCODING_STATUS_COLUMN] = None
    record[KOD_ADRESY_COLUMN] = None
    record[KOD_OBCE_COLUMN] = None
    record[SEARCH_TERM_COLUMN] = None
    record[SEARCH_TYPE_COLUMN] = "cleaned_city_no_preposition_and_street"
    record[MATCHES_COUNT_COLUMN] = None
    record['ruian_nazev_obce'] = None # Bude doplněno, pokud se najde AMD a obec

    if not city_raw:
        record[GEOCODING_STATUS_COLUMN] = "No city_cleaned to process"
        return record

    city_ruian_friendly = clean_city_for_ruian(city_raw)

    if not city_ruian_friendly:
        record[GEOCODING_STATUS_COLUMN] = f"City_cleaned '{city_raw}' could not be sufficiently cleaned (resulted in None)."
        return record

    # Sestavení adresy pro fulltext search (bez apikey)
    search_address_parts = []
    if street_cleaned:
        search_address_parts.append(street_cleaned)
    if record.get('house_no'): # Pokud existuje sloupec 'house_no' a je vyplněn
        search_address_parts.append(str(record['house_no']))
    
    # Použijeme city_ruian_friendly jako základní název obce
    search_address_parts.append(city_ruian_friendly)

    if pd.notna(zip_code_cleaned):
        search_address_parts.append(str(int(zip_code_cleaned))) # Převedeme float PSČ na int a pak na string

    search_address = ", ".join(filter(None, search_address_parts)).strip()

    if not search_address:
        record[GEOCODING_STATUS_COLUMN] = "No valid address components for search."
        return record

    encoded_address = urllib.parse.quote(search_address)
    fulltext_url = f"{RUIAN_API_FULLTEXT_URL}?adresa={encoded_address}"

    try:
        response = requests.get(fulltext_url, timeout=30, headers=HEADERS)
        response.raise_for_status() # Vyvolá HTTPError pro špatné odpovědi (4xx nebo 5xx)
        data = response.json()

        record[SEARCH_TERM_COLUMN] = search_address
        record[SEARCH_TYPE_COLUMN] = "cleaned_city_no_preposition_and_street"

        if data and data.get("polozky"): # Uživatelův skript používá 'polozky'
            am_list = data["polozky"]
            record[MATCHES_COUNT_COLUMN] = len(am_list)
            
            # Použijeme první nalezenou položku jako nejlepší shodu, jak to dělá uživatelův kód
            first_match = am_list[0]
            
            if first_match.get('kod'):
                amd_kod = first_match['kod']
                record[KOD_ADRESY_COLUMN] = amd_kod
                record[GEOCODING_STATUS_COLUMN] = "Found AMD code from cleaned city."

                # Zkusíme získat kód obce
                kod_obce = get_kod_obce_from_amd(amd_kod)
                if kod_obce:
                    record[KOD_OBCE_COLUMN] = kod_obce
                    record[GEOCODING_STATUS_COLUMN] = "Geocoded successfully by cleaned_city_no_preposition_and_street"
                    # RÚIAN API fulltext JSON neposkytuje název obce přímo,
                    # a AMD detail HTML parsování je složité na získání názvu obce.
                    # Prozatím ruian_nazev_obce zůstane None.
                else:
                    record[GEOCODING_STATUS_COLUMN] = "Found AMD code but failed to get municipality code."
            else:
                record[GEOCODING_STATUS_COLUMN] = f"Found matches but no AMD kod in first item for '{search_address}'."
        else:
            record[GEOCODING_STATUS_COLUMN] = f"No AMD candidates for '{search_address}' using 'cleaned_city_no_preposition_and_street'"

    except requests.exceptions.RequestException as e:
        record[GEOCODING_STATUS_COLUMN] = f"Request error for '{search_address}' using 'cleaned_city_no_preposition_and_street': {e}"
    except json.JSONDecodeError:
        record[GEOCODING_STATUS_COLUMN] = f"JSON decode error for '{search_address}' using 'cleaned_city_no_preposition_and_street'"
    except Exception as e:
        record[GEOCODING_STATUS_COLUMN] = f"An unexpected error occurred during geocoding: {e}"

    time.sleep(0.1) # Malá prodleva, abychom nepřetížili API
    return record

def process_data_step_8():
    print(f"Starting Krok 8. Reading input file: {INPUT_FILE}")
    try:
        # Přečteme JSON soubor, který jsme si uložili v předchozím kroku.
        # Tento soubor by měl obsahovat záznamy, které se dosud nepodařilo geokódovat.
        df = pd.read_json(INPUT_FILE, encoding='utf-8')
        print(f"Successfully read {len(df)} rows from {INPUT_FILE}")
    except FileNotFoundError:
        print(f"Error: Input file '{INPUT_FILE}' not found. Make sure step 7 was run successfully and the file name is correct.")
        return
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # Zkopírujeme DataFrame pro iteraci a plnění výsledků
    records_to_process = df.to_dict(orient='records')
    processed_records = []

    print("Attempting to geocode with cleaned city names (without prepositions) and street/zip code...")
    for i, record in enumerate(records_to_process):
        # print(f"Processing record {i+1}/{len(records_to_process)}: ID {record.get('id')}") # Pro detailní ladění
        processed_records.append(geocode_record_with_cleaned_city_improved(record))

    processed_df = pd.DataFrame(processed_records)

    # Uložení všech záznamů po tomto kroku
    # Protože načítáme jen negeokódované, tento soubor bude obsahovat
    # ty, které jsme nyní geokódovali, a ty, které se stále nedaří.
    print(f"Saving all processed records to '{OUTPUT_SUCCESS_JSON_PATH}'...")
    processed_df.to_json(OUTPUT_SUCCESS_JSON_PATH, orient='records', indent=4, force_ascii=False)
    print(f"Successfully saved all processed records to '{OUTPUT_SUCCESS_JSON_PATH}'")

    # Identifikace a uložení záznamů, které se stále nepodařilo geokódovat
    # Ty, které mají kod_obce NaN
    failed_df = processed_df[processed_df[KOD_OBCE_COLUMN].isna()].copy()
    print(f"Found {len(failed_df)} records that still failed to geocode after step 8.")

    if not failed_df.empty:
        print(f"Saving still failed records to '{OUTPUT_FAILED_JSON_PATH}' and '{OUTPUT_FAILED_CSV_PATH}'...")
        failed_df.to_json(OUTPUT_FAILED_JSON_PATH, orient='records', indent=4, force_ascii=False)
        failed_df.to_csv(OUTPUT_FAILED_CSV_PATH, index=False, encoding='utf-8')
        print("Successfully saved still failed records.")
    else:
        print("No failed records found after step 8. All addresses were successfully geocoded!")

    print("\nKrok 8 finished!")

if __name__ == "__main__":
    process_data_step_8()