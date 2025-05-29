import pandas as pd
import requests
import json
import re
import time
from bs4 import BeautifulSoup
from unidecode import unidecode

# Po dalším čištění (předložek a textu za nimi) se přiřadilo  dalších přes 600 řádků, 
# zůstává 197 nepřiřazeno. Důvody jsou různé, tyto adresy už jsou hodně rozmanité, 
# některé jsou chybné nebo je v nich geolokace, či jen číslo namísto adresy. 
# Ještě je zkouším doplnit agresivnějším opakujícícm se přiřazováním, 
# ale pokud se to nepovede, bude třeba je buď doplnit ručně, nebo zahodit.

# --- Konfigurace ---
INPUT_CSV_PATH = '8.vybrane_supermarkety_nepodarilo_geokodovat_cistecity.csv'
OUTPUT_GEOCODED_JSON_PATH = '9.vybrane_supermarkety_geokodovano_pokrocile.json'
OUTPUT_UNGECODED_CSV_PATH = '9.vybrane_supermarkety_nepodarilo_geokodovat_pokrocile.csv'
RUIAN_API_FULLTEXT_URL = "https://vdp.cuzk.cz/vdp/ruian/adresnimista/fulltext"
RUIAN_API_DETAIL_URL_BASE = "https://vdp.cuzk.cz/vdp/ruian/adresnimista/"

# Seznam běžných předložek a spojek pro čištění názvů obcí
PREPOSITIONS = ["u", "nad", "pod", "pri", "v", "z", "na", "do", "k", "pro", "od", "za", "pred", "pres", "bez", "kolem", "mezi", "skrz", "kvuli", "diky", "misto", "podle", "krome", "okolo", "vne", "uvnitr", "blizko", "vedle", "naproti"]

# Seznam slov signalizujících cizí zemi v adrese (pro vyloučení)
FOREIGN_COUNTRY_KEYWORDS = ["Chorvatsko", "Slovensko", "Polsko", "Německo", "Rakousko", "Maďarsko", "Slovenija", "Poland", "Germany", "Austria", "Hungary", "Slovakia", "Croatia", "deutschland", "austria", "slovakia", "poland", "croatia"]


def clean_city_name(city_name):
    """
    Vyčistí název města odstraněním diakritiky a běžných předložek/spojek.
    Používá se pro vytvoření jednoduššího vyhledávacího dotazu.
    """
    if not isinstance(city_name, str):
        return None
    city_name_lower = unidecode(city_name).lower()
    words = city_name_lower.split()
    cleaned_words = [word for word in words if word not in PREPOSITIONS]
    return ' '.join(cleaned_words) if cleaned_words else None

def extract_city_from_address(address_str, postal_code=None):
    """
    Pokusí se extrahovat název obce z nestandardních formátů v poli 'address'.
    Priorita: PSČ a název obce, nebo jen název obce na konci.
    Pokud je k dispozici PSČ, zkusí ho použít k extrakci.
    """
    if not isinstance(address_str, str):
        return None

    # Normalizace mezer a čárek
    address_str_normalized = address_str.replace(', Česko', '').replace(',Česko', '').strip()
    address_str_normalized = re.sub(r'\s+', ' ', address_str_normalized) # Více mezer na jednu
    address_str_normalized = re.sub(r',\s*,', ',', address_str_normalized) # Dvojité čárky

    # Odstranění koordinátů a čísel, které vypadají jako popisná/orientační
    address_str_no_coords = re.sub(r'\d{1,3}°\d{1,2}\'\d{1,2}\.\d{1,2}"[N|S|E|W]', '', address_str_normalized) # Odstranění koordinátů
    address_str_no_coords = re.sub(r'\b\d+\s*\d{2}\b', '', address_str_no_coords) # Odstranění PSČ formátu XXXXX YYY
    address_str_no_coords = re.sub(r'\b\d{5}\b', '', address_str_no_coords) # Odstranění PSČ formátu XXXXX
    address_str_no_coords = re.sub(r',\s*\d+(\s*[a-zA-Z])?(\s*-\s*\d+)?\s*$', '', address_str_no_coords) # Odstranění čísla na konci po čárce (popisné/orientační)

    # Zkusit regex pro "Číslo Město" nebo "Město" na konci
    # Např. "126 Úsobí", "461 Horní Jelení"
    match_hn_city = re.search(r'(\d+)\s+([A-ZĚŠČŘŽÝÁÍÉÚŮÓĎŤŇa-zěščřžýáíéúůóďťň\s-]+)$', address_str_no_coords)
    if match_hn_city:
        return match_hn_city.group(2).strip()

    # Zkusit regex pro "Ulice Číslo, PSČ Město"
    # Např. "Hlavní 520, 73911 Frýdlant nad Ostravicí"
    match_street_zip_city = re.search(r'\d{3}\s*\d{2}\s+([A-ZĚŠČŘŽÝÁÍÉÚŮÓĎŤŇa-zěščřžýáíéúůóďťň\s-]+)$', address_str_normalized)
    if match_street_zip_city:
        return match_street_zip_city.group(1).strip()

    # Zkusit extrakci obce, pokud je v adrese PSČ
    if postal_code and isinstance(postal_code, str):
        # Najít část adresy po PSČ a zkusit z ní vyextrahovat obec
        pc_pattern = re.escape(postal_code.replace(' ', '')) # Odstranit mezery z PSČ pro regex
        match_after_pc = re.search(r'' + pc_pattern + r'\s*([A-ZĚŠČŘŽÝÁÍÉÚŮÓĎŤŇa-zěščřžýáíéúůóďťň\s-]+)$', address_str_normalized)
        if match_after_pc:
            return match_after_pc.group(1).strip()

    # Jednoduchá heuristika: Poslední dvě slova v adrese po odstranění PSČ a čísla vypadajícího jako popisné
    temp_address = address_str_no_coords
    words = temp_address.split(',')[-1].strip().split() # Vezmeme poslední část po čárce
    
    if len(words) >= 1:
        # Zkusit vzít posledních N slov a zkontrolovat, jestli nevypadají jako ulice (obsahují 'tř.', 'nám.', atd.)
        for i in range(len(words), 0, -1):
            candidate_city = ' '.join(words[-i:]).strip()
            if not re.search(r'(ul\.|nám\.|tř\.|křižovatka)', candidate_city, re.IGNORECASE) and \
               not re.search(r'^\d+$', candidate_city) and \
               len(candidate_city) > 2: # Město by mělo mít alespoň 3 znaky
                return candidate_city
        
        return words[-1].strip() # Jako fallback poslední slovo, pokud je validní

    return None

def geocode_ruian_advanced(search_term, search_type, timeout_seconds=30, retries=3, delay=1):
    """
    Provede geokódování na RÚIAN API s daným vyhledávacím termínem a typem.
    Vrací data o adresním místě nebo None.
    Zahrnuje mechanismus opakování pro chyby serveru.
    """
    params = {'adresa': search_term}
    headers = {'Accept': 'application/json'} # Žádáme JSON odpověď

    for attempt in range(retries):
        try:
            response = requests.get(RUIAN_API_FULLTEXT_URL, params=params, headers=headers, timeout=timeout_seconds)
            response.raise_for_status()  # Vyhodí HTTPError pro špatné odpovědi (4xx nebo 5xx)
            
            if not response.text.strip():
                return "No content in response", 0, None

            data = response.json()
            
            if not data:
                return "No AMD candidates for '{}' using '{}'".format(search_term, search_type), 0, None
            
            if 'adresniMista' in data and data['adresniMista']:
                amd_data = data['adresniMista'][0]
                
                amd_detail_url = RUIAN_API_DETAIL_URL_BASE + str(amd_data['kod'])
                
                detail_response = requests.get(amd_detail_url, timeout=10)
                detail_response.raise_for_status()
                
                soup = BeautifulSoup(detail_response.text, 'html.parser')
                
                kod_obce_match = soup.find('th', string='Obec').find_next_sibling('td').find('a')
                kod_obce = kod_obce_match.text.strip() if kod_obce_match else None
                
                nazev_obce_match = soup.find('th', string='Obec').find_next_sibling('td')
                nazev_obce = nazev_obce_match.get_text(separator=' ').split('(')[0].strip() if nazev_obce_match else None
                
                return "Success", len(data['adresniMista']), {
                    'ruian_kod_adresniho_mista': amd_data.get('kod'),
                    'ruian_kod_obce': kod_obce,
                    'ruian_nazev_obce': nazev_obce,
                    'ruian_search_term': search_term,
                    'ruian_search_type': search_type,
                    'ruian_matches_count': len(data['adresniMista']),
                    'ruian_geocoding_status': "Success"
                }
            else:
                return "No AMD candidates for '{}' using '{}'".format(search_term, search_type), 0, None

        except requests.exceptions.HTTPError as e:
            if e.response.status_code >= 500 and attempt < retries - 1:
                print(f"Retrying on 5xx error for '{search_term}' (attempt {attempt + 1}/{retries})...")
                time.sleep(delay * (attempt + 1)) # Zvětšující se prodleva
                continue
            return "Request error for '{}' using '{}': {}".format(search_term, search_type, e), 0, None
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                print(f"Retrying on timeout for '{search_term}' (attempt {attempt + 1}/{retries})...")
                time.sleep(delay * (attempt + 1))
                continue
            return "Timeout error for '{}' using '{}'".format(search_term, search_type), 0, None
        except requests.exceptions.RequestException as e:
            return "Request error for '{}' using '{}': {}".format(search_term, search_type, e), 0, None
        except json.JSONDecodeError:
            return "JSON decode error for '{}' using '{}'".format(search_term, search_type), 0, None
        except Exception as e:
            return "An unexpected error occurred for '{}' using '{}': {}".format(search_term, search_type, e), 0, None
    return "Failed after multiple retries", 0, None # Pokud všechny pokusy selžou


def process_ungeocoded_records(df):
    geocoded_records = []
    ungeocoded_records = []

    print("Starting Krok 9. Processing un-geocoded records with advanced strategies...")

    for index, row in df.iterrows():
        record = row.to_dict()

        # 1. Filtrace zahraničních adres
        is_foreign = False
        if isinstance(record.get('address'), str):
            for keyword in FOREIGN_COUNTRY_KEYWORDS:
                if keyword.lower() in record['address'].lower():
                    is_foreign = True
                    break
        if is_foreign:
            record['ruian_geocoding_status'] = "Foreign Address"
            ungeocoded_records.append(record)
            print(f"ID {record['id']}: Skipping foreign address.")
            time.sleep(0.05)
            continue
            
        # 2. Agresivní extrakce města, pokud city_cleaned chybí nebo je prázdné
        original_city_cleaned = record.get('city_cleaned')
        extracted_city = None

        if not original_city_cleaned or pd.isna(original_city_cleaned) or str(original_city_cleaned).strip() == '':
            extracted_city = extract_city_from_address(record.get('address'), record.get('postalCode'))
            if extracted_city:
                record['city_cleaned'] = extracted_city
                print(f"ID {record['id']}: Extracted city '{extracted_city}' from address.")
            else:
                record['ruian_geocoding_status'] = "Failed to extract cleaned city from address"
                ungeocoded_records.append(record)
                print(f"ID {record['id']}: Failed to extract cleaned city. Skipping.")
                time.sleep(0.05)
                continue
        
        city_cleaned = record.get('city_cleaned')
        street_cleaned = record.get('street_cleaned')
        zip_code_cleaned_val = int(record['zip_code_cleaned']) if pd.notnull(record.get('zip_code_cleaned')) else None
        
        strategies = []

        # Strategie A: Plná adresa (ulice, město, PSČ)
        if street_cleaned and city_cleaned and zip_code_cleaned_val:
            strategies.append((f"{street_cleaned}, {city_cleaned}, {zip_code_cleaned_val}", "full_address"))
        # Strategie B: Ulice, město (bez PSČ)
        if street_cleaned and city_cleaned:
            strategies.append((f"{street_cleaned}, {city_cleaned}", "street_city_only"))
        # Strategie C: Číslo popisné/orientační (pokud street_cleaned je jen číslo), město, PSČ
        is_street_number_only = re.match(r'^\d+$', str(street_cleaned).strip()) if street_cleaned else False
        if is_street_number_only and city_cleaned and zip_code_cleaned_val:
            strategies.append((f"{street_cleaned}, {city_cleaned}, {zip_code_cleaned_val}", "hn_city_zip"))
        # Strategie D: Město a PSČ
        if city_cleaned and zip_code_cleaned_val:
            strategies.append((f"{city_cleaned}, {zip_code_cleaned_val}", "city_zip_only"))
        # Strategie E: Zjednodušené město a PSČ (odstranění "u", "nad" atd.)
        if city_cleaned and zip_code_cleaned_val:
            simplified_city = clean_city_name(city_cleaned)
            if simplified_city and simplified_city != city_cleaned:
                strategies.append((f"{simplified_city}, {zip_code_cleaned_val}", "simplified_city_zip"))
        # Strategie F: Jen město
        if city_cleaned:
            strategies.append((f"{city_cleaned}", "city_only"))
        # Strategie G: Jen zjednodušené město
        if city_cleaned:
            simplified_city = clean_city_name(city_cleaned)
            if simplified_city and simplified_city != city_cleaned:
                strategies.append((f"{simplified_city}", "simplified_city_only"))

        geocoded = False
        for search_term, search_type in strategies:
            status, matches_count, geocoded_data = geocode_ruian_advanced(search_term, search_type)
            if geocoded_data:
                record.update(geocoded_data)
                geocoded_records.append(record)
                print(f"ID {record['id']}: Successfully geocoded with {search_type}.")
                geocoded = True
                break # Přeskočit na další záznam, pokud se najde shoda
            else:
                # Uložit status jen pro poslední pokus, který selhal, pokud se nic nenajde
                record['ruian_geocoding_status'] = status
                record['ruian_search_term'] = search_term
                record['ruian_search_type'] = search_type
            time.sleep(0.1) # Malá prodleva mezi pokusy, aby se API nepřetížilo

        if not geocoded:
            ungeocoded_records.append(record)
            print(f"ID {record['id']}: Still un-geocoded. Status: {record.get('ruian_geocoding_status', 'Unknown')}")
            time.sleep(0.05)

    print("Krok 9 finished!")
    return pd.DataFrame(geocoded_records), pd.DataFrame(ungeocoded_records)

# --- Hlavní spuštění ---
if __name__ == "__main__":
    try:
        df_ungeocoded = pd.read_csv(INPUT_CSV_PATH)
        print(f"Successfully read {len(df_ungeocoded)} rows from {INPUT_CSV_PATH}")

        # Nastavení typů dat, pokud nejsou správně načteny z CSV
        df_ungeocoded['zip_code_cleaned'] = pd.to_numeric(df_ungeocoded['zip_code_cleaned'], errors='coerce').astype('Int64')
        
        # Zajištění, že 'id' je správně nastaveno, pokud by nebylo
        if 'id' not in df_ungeocoded.columns:
            df_ungeocoded['id'] = range(1, len(df_ungeocoded) + 1)
        
        # Ujistit se, že 'postalCode' je string
        if 'postalCode' in df_ungeocoded.columns:
            df_ungeocoded['postalCode'] = df_ungeocoded['postalCode'].astype(str)
        else:
            df_ungeocoded['postalCode'] = None # Pokud sloupec neexistuje, nastavíme ho na None

        df_geocoded_this_step, df_still_ungeocoded = process_ungeocoded_records(df_ungeocoded)

        # Ukládání úspěšně geokódovaných záznamů do JSON
        if not df_geocoded_this_step.empty:
            df_geocoded_this_step = df_geocoded_this_step.convert_dtypes()
            
            for col in ['ruian_kod_adresniho_mista', 'ruian_kod_obce', 'ruian_matches_count']:
                if col in df_geocoded_this_step.columns:
                    df_geocoded_this_step[col] = df_geocoded_this_step[col].apply(lambda x: int(x) if pd.notnull(x) else None)
            
            with open(OUTPUT_GEOCODED_JSON_PATH, 'w', encoding='utf-8') as f:
                json.dump(df_geocoded_this_step.to_dict(orient='records'), f, ensure_ascii=False, indent=4)
            print(f"Successfully saved {len(df_geocoded_this_step)} geocoded records to {OUTPUT_GEOCODED_JSON_PATH}")
        else:
            print(f"No records geocoded in this step. {OUTPUT_GEOCODED_JSON_PATH} will not be created.")

        # Ukládání stále negeokódovaných záznamů do CSV
        if not df_still_ungeocoded.empty:
            df_still_ungeocoded.to_csv(OUTPUT_UNGECODED_CSV_PATH, index=False, encoding='utf-8')
            print(f"Successfully saved {len(df_still_ungeocoded)} still un-geocoded records to {OUTPUT_UNGECODED_CSV_PATH}")
        else:
            print(f"All records geocoded. {OUTPUT_UNGECODED_CSV_PATH} will not be created.")

    except FileNotFoundError:
        print(f"Error: Input file '{INPUT_CSV_PATH}' not found. Please ensure the file exists in the same directory as the script, or provide its full path.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")