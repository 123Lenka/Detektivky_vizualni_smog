import pandas as pd
import requests
import urllib.parse
import time
from bs4 import BeautifulSoup
import re
import random # Pro náhodné zpoždění

# Takto vzniklému datasetu nechám přiřadit ruina - identifikátor města, nazývaný v datasetu jako sloupec kod_obce. 
# Skript na přiřazení ruianu nám poskytla mentorka. 
# Jen bylo třeba si jej upravit pro účely supermarketů json. 
# Proces trvá 5 hodin.

# --- Konfigurace cest k souborům ---
# Vstupní soubor z kroku 2 (vyfiltrované kategorie)
INPUT_FILE = '2.vybrane_supermarkety_filtrovane_kategorie.json'

# Výstupní soubor s RÚIAN daty (hlavní výstup v JSON)
OUTPUT_FILE = '3.vybrane_supermarkety_s_ruian_daty.json'
# Kontrolní CSV výstup (pro snadnou kontrolu v tabulce)
OUTPUT_CHECK_CSV_PATH = '3.vybrane_supermarkety_geokodovane_pro_kontrolu.csv'

# input - sloupce z vyčištěných dat z kroku 1 a 2
CITY_COL = 'city_cleaned'
STREET_COL = 'street_cleaned'
PSC_COL = 'zip_code_cleaned'

# output
KOD_ADRESY_COLUMN = "ruian_kod_adresniho_mista" # Přesnější název pro AMD kód
KOD_OBCE_COLUMN = "ruian_kod_obce"
NAZEV_OBCE_COLUMN = "ruian_nazev_obce" # Nový sloupec pro název obce
SEARCH_TERM_COLUMN = 'ruian_search_term'
SEARCH_TYPE_COLUMN = 'ruian_search_type'
SEARCH_MATCHES_COLUMN = 'ruian_matches_count'
GEOCODING_STATUS_COLUMN = 'ruian_geocoding_status' # Pro sledování úspěšnosti

# API Endpoints
API_FULLTEXT_URL = 'https://vdp.cuzk.gov.cz/vdp/ruian/adresnimista/fulltext'
API_AMD_TO_KOD_OBCE_URL_TEMPLATE = 'https://vdp.cuzk.gov.cz/vdp/ruian/adresnimista/{}'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'X-Requested-With': 'XMLHttpRequest'
}

def get_address_alternatives(data_row):
    """
    Generuje alternativní vyhledávací termíny na základě vyčištěných dat.
    """
    alternatives = []

    city_cleaned = str(data_row[CITY_COL]).strip() if pd.notna(data_row[CITY_COL]) else ''
    street_cleaned = str(data_row[STREET_COL]).strip() if pd.notna(data_row[STREET_COL]) else ''
    psc_cleaned = str(data_row[PSC_COL]).strip() if pd.notna(data_row[PSC_COL]) else ''

    # 1. Kompletní adresa (ulice + město + PSČ)
    full_address = f"{street_cleaned}, {city_cleaned} {psc_cleaned}".strip(', ').strip()
    if full_address:
        alternatives.append(('full_address', full_address))

    # 2. Ulice a město (bez PSČ)
    street_city_only = f"{street_cleaned}, {city_cleaned}".strip(', ').strip()
    if street_city_only and street_city_only != full_address:
        alternatives.append(('street_city_only', street_city_only))
    
    # 3. Město a PSČ (pro případy, kdy ulice není klíčová nebo je problémová)
    city_psc_only = f"{city_cleaned} {psc_cleaned}".strip()
    if city_psc_only and city_psc_only != full_address:
        alternatives.append(('city_psc_only', city_psc_only))

    # 4. Pouze město (jako poslední možnost)
    if city_cleaned and city_cleaned != full_address:
        alternatives.append(('city_only', city_cleaned))

    return alternatives

def get_address_code_and_info(data_row):
    """
    Pokusí se získat kód adresního místa (AMD) a související informace z RÚIAN API.
    """
    address_alternatives = get_address_alternatives(data_row)
    
    result = {
        KOD_ADRESY_COLUMN: pd.NA,
        KOD_OBCE_COLUMN: pd.NA,
        NAZEV_OBCE_COLUMN: pd.NA,
        SEARCH_TYPE_COLUMN: pd.NA,
        SEARCH_TERM_COLUMN: pd.NA,
        SEARCH_MATCHES_COLUMN: pd.NA,
        GEOCODING_STATUS_COLUMN: "No valid input for geocoding" # Výchozí stav
    }

    for search_type, address_term in address_alternatives:
        if not address_term.strip(): # Přeskočit prázdné vyhledávací termíny
            continue

        encoded_address = urllib.parse.quote(address_term)
        url = f"{API_FULLTEXT_URL}?adresa={encoded_address}"

        try:
            response = requests.get(url, timeout=15, headers=HEADERS) # Zvýšený timeout
            response.raise_for_status()
            data = response.json()

            if data and 'polozky' in data and data['polozky']:
                num_matches = len(data['polozky'])
                best_match = data['polozky'][0]

                if best_match.get('kod'):
                    amd_code = best_match['kod']
                    
                    result.update({
                        KOD_ADRESY_COLUMN: amd_code,
                        SEARCH_TYPE_COLUMN: search_type,
                        SEARCH_TERM_COLUMN: address_term,
                        SEARCH_MATCHES_COLUMN: num_matches,
                        GEOCODING_STATUS_COLUMN: "AMD Found"
                    })
                    return result # Vracíme první úspěšnou shodu

                else:
                    result[GEOCODING_STATUS_COLUMN] = f"AMD data incomplete for '{address_term}'"
            else:
                result[GEOCODING_STATUS_COLUMN] = f"No AMD candidates for '{address_term}'"
        except requests.exceptions.Timeout:
            result[GEOCODING_STATUS_COLUMN] = f"Request timed out for '{address_term}'"
        except requests.exceptions.RequestException as e:
            result[GEOCODING_STATUS_COLUMN] = f"Request error for '{address_term}': {e}"
        except json.JSONDecodeError:
            result[GEOCODING_STATUS_COLUMN] = f"JSON decode error for '{address_term}'"
        except Exception as e:
            result[GEOCODING_STATUS_COLUMN] = f"Unexpected error for '{address_term}': {e}"
    
    return result

def get_kod_obce_and_nazev_obce(address_code):
    """
    Získává kód a název obce z webové stránky adresního místa.
    """
    if pd.isna(address_code) or not address_code:
        return pd.NA, pd.NA

    url = API_AMD_TO_KOD_OBCE_URL_TEMPLATE.format(address_code)

    try:
        response = requests.get(url, timeout=10, headers=HEADERS)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        
        obec_link = soup.find('a', href=re.compile(r"/vdp/ruian/obce/\d+"))
        if obec_link:
            obec_href = obec_link.get('href')
            matched_id = re.search(r"/vdp/ruian/obce/(\d+)", obec_href)
            if matched_id:
                kod_obce = int(matched_id.group(1))
                nazev_obce = obec_link.get_text(strip=True)
                return kod_obce, nazev_obce
        
        return pd.NA, pd.NA

    except requests.exceptions.RequestException as e:
        return pd.NA, pd.NA
    except Exception as e:
        return pd.NA, pd.NA


def match_address_to_city_code():
    print(f"Starting script. Reading input file: {INPUT_FILE}")
    try:
        df = pd.read_json(INPUT_FILE, encoding='utf-8')
        print(f"Successfully read {len(df)} rows from {INPUT_FILE}")
    except FileNotFoundError:
        print(f"Error: Input file '{INPUT_FILE}' not found. Please make sure it's in the same directory as the script or provide the full path.")
        return
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # Připravit sloupce pro výsledky geokódování
    df[KOD_ADRESY_COLUMN] = pd.NA
    df[KOD_OBCE_COLUMN] = pd.NA
    df[NAZEV_OBCE_COLUMN] = pd.NA
    df[SEARCH_TERM_COLUMN] = pd.NA
    df[SEARCH_TYPE_COLUMN] = pd.NA
    df[SEARCH_MATCHES_COLUMN] = pd.NA
    df[GEOCODING_STATUS_COLUMN] = pd.NA

    total_rows = len(df)
    successful_geocodes = 0

    print("Starting geocoding process...")
    for index, row in df.iterrows():
        print(f"\nProcessing row {index + 1}/{total_rows} (ID: {row['id']})...")
        
        amd_info = get_address_code_and_info(row)
        
        df.at[index, KOD_ADRESY_COLUMN] = amd_info.get(KOD_ADRESY_COLUMN, pd.NA)
        df.at[index, SEARCH_TERM_COLUMN] = amd_info.get(SEARCH_TERM_COLUMN, pd.NA)
        df.at[index, SEARCH_TYPE_COLUMN] = amd_info.get(SEARCH_TYPE_COLUMN, pd.NA)
        df.at[index, SEARCH_MATCHES_COLUMN] = amd_info.get(SEARCH_MATCHES_COLUMN, pd.NA)
        df.at[index, GEOCODING_STATUS_COLUMN] = amd_info.get(GEOCODING_STATUS_COLUMN, pd.NA)

        if pd.notna(df.at[index, KOD_ADRESY_COLUMN]):
            kod_obce, nazev_obce = get_kod_obce_and_nazev_obce(df.at[index, KOD_ADRESY_COLUMN])
            df.at[index, KOD_OBCE_COLUMN] = kod_obce
            df.at[index, NAZEV_OBCE_COLUMN] = nazev_obce
            
            if pd.notna(kod_obce):
                df.at[index, GEOCODING_STATUS_COLUMN] = "Success"
                successful_geocodes += 1
            else:
                df.at[index, GEOCODING_STATUS_COLUMN] = "AMD found, but Kod Obce not extracted"

        # Náhodná pauza mezi 0.1 a 0.3 sekundy
        time.sleep(random.uniform(0.1, 0.3))


    print(f"\nProcessed all rows. Successfully geocoded {successful_geocodes}/{total_rows} addresses.")

    # Uložení výsledků do JSON (hlavní výstup)
    print(f"\nSaving geocoded data to '{OUTPUT_FILE}'...")
    try:
        # Zde odstraněn argument 'ensure_ascii=False', protože starší verze Pandas ho nepodporuje
        df.to_json(OUTPUT_FILE, orient='records', indent=4)
        print(f"Successfully saved updated data to '{OUTPUT_FILE}'")
    except Exception as e:
        print(f"Error saving JSON file: {e}")

    # Uložení výsledků do CSV pro kontrolu
    print(f"\nSaving geocoded data to '{OUTPUT_CHECK_CSV_PATH}' for easy review...")
    try:
        df.to_csv(OUTPUT_CHECK_CSV_PATH, index=False, encoding='utf-8')
        print(f"Successfully saved check CSV to '{OUTPUT_CHECK_CSV_PATH}'")
    except Exception as e:
        print(f"Error saving CSV file: {e}")

    print("\nGeocoding process finished!")

if __name__ == "__main__":
    match_address_to_city_code()