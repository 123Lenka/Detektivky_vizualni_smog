import pandas as pd
import requests
import urllib.parse
import time
from bs4 import BeautifulSoup
import re
import random 

# Čištění v bodě 5. se povedlo. Sloupec cityPart se vytvořil. 

# Následné nové přiřazování ruian bychom mohli nastavit tak, 
# že logika zkusí najít hodnoty podle city cleaned a pokud se to nepovede, 
# může se ještě dotázat na přiřazení pomocí cityPart, pokud je v něm hodnota. 

# --- Konfigurace cest k souborům ---
# Vstupní soubor z kroku 5 (znovu vyčištěné neúspěšné záznamy)
INPUT_FILE = '5.vybrane_supermarkety_nepodarilo_geokodovat_vycisteno.json'

# Výstupní soubor s RÚIAN daty z tohoto re-geokódování (HLAVNÍ VÝSTUP V JSON)
OUTPUT_FILE = '6.vybrane_supermarkety_regeokodovano_ruian_data.json'
# Kontrolní CSV výstup
OUTPUT_CHECK_CSV_PATH = '6.vybrane_supermarkety_regeokodovano_pro_kontrolu.csv'

# input - sloupce z vyčištěných dat
CITY_COL = 'city_cleaned'
CITY_PART_COL = 'cityPart' # Nový sloupec pro fallback
STREET_COL = 'street_cleaned'
PSC_COL = 'zip_code_cleaned'

# output - sloupce pro RÚIAN data (stejné jako v kroku 3)
KOD_ADRESY_COLUMN = "ruian_kod_adresniho_mista"
KOD_OBCE_COLUMN = "ruian_kod_obce"
NAZEV_OBCE_COLUMN = "ruian_nazev_obce"
SEARCH_TERM_COLUMN = 'ruian_search_term'
SEARCH_TYPE_COLUMN = 'ruian_search_type'
SEARCH_MATCHES_COLUMN = 'ruian_matches_count'
GEOCODING_STATUS_COLUMN = 'ruian_geocoding_status'

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

def get_address_alternatives(data_row, current_city_column):
    """
    Generuje alternativní vyhledávací termíny na základě vyčištěných dat,
    s dynamickým výběrem sloupce pro město.
    """
    alternatives = []

    city_cleaned = str(data_row[current_city_column]).strip() if pd.notna(data_row[current_city_column]) else ''
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

def get_address_code_and_info(data_row, current_city_column):
    """
    Pokusí se získat kód adresního místa (AMD) a související informace z RÚIAN API.
    Nyní přijímá i název sloupce pro město.
    """
    address_alternatives = get_address_alternatives(data_row, current_city_column)
    
    result = {
        KOD_ADRESY_COLUMN: pd.NA,
        KOD_OBCE_COLUMN: pd.NA,
        NAZEV_OBCE_COLUMN: pd.NA,
        SEARCH_TYPE_COLUMN: pd.NA,
        SEARCH_TERM_COLUMN: pd.NA,
        SEARCH_MATCHES_COLUMN: pd.NA,
        GEOCODING_STATUS_COLUMN: f"Failed to geocode using '{current_city_column}'" # Specifičtější status
    }

    for search_type, address_term in address_alternatives:
        if not address_term.strip():
            continue

        encoded_address = urllib.parse.quote(address_term)
        url = f"{API_FULLTEXT_URL}?adresa={encoded_address}"

        try:
            response = requests.get(url, timeout=15, headers=HEADERS)
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
                    return result

                else:
                    result[GEOCODING_STATUS_COLUMN] = f"AMD data incomplete for '{address_term}' using '{current_city_column}'"
            else:
                result[GEOCODING_STATUS_COLUMN] = f"No AMD candidates for '{address_term}' using '{current_city_column}'"
        except requests.exceptions.Timeout:
            result[GEOCODING_STATUS_COLUMN] = f"Request timed out for '{address_term}' using '{current_city_column}'"
        except requests.exceptions.RequestException as e:
            result[GEOCODING_STATUS_COLUMN] = f"Request error for '{address_term}' using '{current_city_column}': {e}"
        except json.JSONDecodeError:
            result[GEOCODING_STATUS_COLUMN] = f"JSON decode error for '{address_term}' using '{current_city_column}'"
        except Exception as e:
            result[GEOCODING_STATUS_COLUMN] = f"Unexpected error for '{address_term}' using '{current_city_column}': {e}"
    
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


def re_geocode_data():
    print(f"Starting script. Reading input file for re-geocoding: {INPUT_FILE}")
    try:
        df = pd.read_json(INPUT_FILE, encoding='utf-8')
        print(f"Successfully read {len(df)} rows from {INPUT_FILE}")
    except FileNotFoundError:
        print(f"Error: Input file '{INPUT_FILE}' not found. Please make sure it's in the same directory as the script or provide the full path.")
        return
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # Připravit sloupce pro výsledky geokódování, pokud ještě neexistují
    for col in [KOD_ADRESY_COLUMN, KOD_OBCE_COLUMN, NAZEV_OBCE_COLUMN, 
                SEARCH_TERM_COLUMN, SEARCH_TYPE_COLUMN, SEARCH_MATCHES_COLUMN, 
                GEOCODING_STATUS_COLUMN]:
        if col not in df.columns:
            df[col] = pd.NA

    total_rows = len(df)
    successful_geocodes_this_run = 0
    skipped_rows = 0

    print("Starting re-geocoding process with fallback logic...")
    for index, row in df.iterrows():
        # Pokud už má záznam kód obce, přeskočíme ho (pro případ, že bychom spouštěli na starší verzi)
        if pd.notna(row[KOD_OBCE_COLUMN]):
            skipped_rows += 1
            print(f"Skipping row {index + 1}/{total_rows} (ID: {row['id']}) - already geocoded. ({skipped_rows} skipped so far)")
            continue

        print(f"\nProcessing row {index + 1}/{total_rows} (ID: {row['id']})...")
        
        # 1. Pokus s city_cleaned
        current_status = "Failed - city_cleaned" # Výchozí status, pokud selže i get_address_code_and_info
        amd_info = get_address_code_and_info(row, CITY_COL)
        
        if pd.notna(amd_info.get(KOD_ADRESY_COLUMN)):
            # Pokud se podařilo najít AMD kód s city_cleaned, pokusíme se získat KOD OBCE
            kod_obce, nazev_obce = get_kod_obce_and_nazev_obce(amd_info.get(KOD_ADRESY_COLUMN))
            if pd.notna(kod_obce):
                # Úspěch s city_cleaned
                df.at[index, KOD_ADRESY_COLUMN] = amd_info[KOD_ADRESY_COLUMN]
                df.at[index, KOD_OBCE_COLUMN] = kod_obce
                df.at[index, NAZEV_OBCE_COLUMN] = nazev_obce
                df.at[index, SEARCH_TERM_COLUMN] = amd_info[SEARCH_TERM_COLUMN]
                df.at[index, SEARCH_TYPE_COLUMN] = amd_info[SEARCH_TYPE_COLUMN]
                df.at[index, SEARCH_MATCHES_COLUMN] = amd_info[SEARCH_MATCHES_COLUMN]
                df.at[index, GEOCODING_STATUS_COLUMN] = "Success (re-geocoded via city_cleaned)"
                successful_geocodes_this_run += 1
            else:
                # AMD nalezeno, ale KOD OBCE neextrahován - stále neúspěch
                df.at[index, KOD_ADRESY_COLUMN] = amd_info[KOD_ADRESY_COLUMN] # Uložíme alespoň AMD, kdyžtak
                df.at[index, SEARCH_TERM_COLUMN] = amd_info[SEARCH_TERM_COLUMN]
                df.at[index, SEARCH_TYPE_COLUMN] = amd_info[SEARCH_TYPE_COLUMN]
                df.at[index, SEARCH_MATCHES_COLUMN] = amd_info[SEARCH_MATCHES_COLUMN]
                df.at[index, GEOCODING_STATUS_COLUMN] = "AMD found, but Kod Obce not extracted (re-attempt with city_cleaned)"
        else:
            # První pokus (city_cleaned) selhal úplně, zkusíme cityPart
            current_status = amd_info.get(GEOCODING_STATUS_COLUMN, "Failed - city_cleaned general") # Získáme konkrétní selhání z prvního pokusu
            
            if pd.notna(row.get(CITY_PART_COL)) and str(row[CITY_PART_COL]).strip(): # Zkontrolovat, zda cityPart existuje a není prázdný
                print(f"Attempting fallback with cityPart: {row[CITY_PART_COL]}")
                amd_info_fallback = get_address_code_and_info(row, CITY_PART_COL)
                
                if pd.notna(amd_info_fallback.get(KOD_ADRESY_COLUMN)):
                    kod_obce, nazev_obce = get_kod_obce_and_nazev_obce(amd_info_fallback.get(KOD_ADRESY_COLUMN))
                    if pd.notna(kod_obce):
                        # Úspěch s cityPart
                        df.at[index, KOD_ADRESY_COLUMN] = amd_info_fallback[KOD_ADRESY_COLUMN]
                        df.at[index, KOD_OBCE_COLUMN] = kod_obce
                        df.at[index, NAZEV_OBCE_COLUMN] = nazev_obce
                        df.at[index, SEARCH_TERM_COLUMN] = amd_info_fallback[SEARCH_TERM_COLUMN]
                        df.at[index, SEARCH_TYPE_COLUMN] = amd_info_fallback[SEARCH_TYPE_COLUMN]
                        df.at[index, SEARCH_MATCHES_COLUMN] = amd_info_fallback[SEARCH_MATCHES_COLUMN]
                        df.at[index, GEOCODING_STATUS_COLUMN] = "Success (re-geocoded via cityPart)"
                        successful_geocodes_this_run += 1
                    else:
                        # AMD nalezeno přes cityPart, ale KOD OBCE neextrahován - stále neúspěch
                        df.at[index, KOD_ADRESY_COLUMN] = amd_info_fallback[KOD_ADRESY_COLUMN] # Uložíme alespoň AMD
                        df.at[index, SEARCH_TERM_COLUMN] = amd_info_fallback[SEARCH_TERM_COLUMN]
                        df.at[index, SEARCH_TYPE_COLUMN] = amd_info_fallback[SEARCH_TYPE_COLUMN]
                        df.at[index, SEARCH_MATCHES_COLUMN] = amd_info_fallback[SEARCH_MATCHES_COLUMN]
                        df.at[index, GEOCODING_STATUS_COLUMN] = "AMD found, but Kod Obce not extracted (re-attempt with cityPart)"
                else:
                    # Oba pokusy selhaly, použijeme poslední status z fallback pokusu
                    df.at[index, GEOCODING_STATUS_COLUMN] = amd_info_fallback.get(GEOCODING_STATUS_COLUMN, current_status + " | Failed - cityPart general")
            else:
                # cityPart nebyl k dispozici nebo byl prázdný, použijeme status z prvního pokusu
                df.at[index, GEOCODING_STATUS_COLUMN] = current_status


        # Náhodná pauza mezi 0.1 a 0.3 sekundy
        time.sleep(random.uniform(0.1, 0.3))


    print(f"\nRe-geocoding process finished.")
    print(f"Total rows processed: {total_rows}")
    print(f"Rows skipped (already geocoded): {skipped_rows}")
    print(f"Successfully re-geocoded in this run: {successful_geocodes_this_run} addresses.")
    
    final_failed_count = df[df[KOD_OBCE_COLUMN].isna()].shape[0]
    print(f"Remaining failed records after this run: {final_failed_count}")


    # Uložení výsledků do JSON (hlavní výstup)
    print(f"\nSaving re-geocoded data to '{OUTPUT_FILE}'...")
    try:
        df.to_json(OUTPUT_FILE, orient='records', indent=4)
        print(f"Successfully saved updated data to '{OUTPUT_FILE}'")
    except Exception as e:
        print(f"Error saving JSON file: {e}")

    # Uložení výsledků do CSV pro kontrolu
    print(f"\nSaving re-geocoded data to '{OUTPUT_CHECK_CSV_PATH}' for easy review...")
    try:
        df.to_csv(OUTPUT_CHECK_CSV_PATH, index=False, encoding='utf-8')
        print(f"Successfully saved check CSV to '{OUTPUT_CHECK_CSV_PATH}'")
    except Exception as e:
        print(f"Error saving CSV file: {e}")

    print("\nRe-geocoding process finished!")

if __name__ == "__main__":
    re_geocode_data()