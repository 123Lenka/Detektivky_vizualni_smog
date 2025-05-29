import pandas as pd
import json

# Z dalšího čištění a přiřazení vzešlo už jen 800 nepřiřazených hodnot. 
# Zatím si vytáhneme nepropojené řádky zase zvlášť

# --- Konfigurace cest k souborům ---
INPUT_FILE = '6.vybrane_supermarkety_regeokodovano_ruian_data.json'

# Výstupní soubory pro nepropojené záznamy
OUTPUT_FAILED_JSON_PATH = '7.vybrane_supermarkety_nepodarilo_geokodovat_znovu.json'
OUTPUT_FAILED_CSV_PATH = '7.vybrane_supermarkety_nepodarilo_geokodovat_znovu.csv'

# Sloupce pro kontrolu, zda je záznam geokódován
KOD_ADRESY_COLUMN = "ruian_kod_adresniho_mista"
KOD_OBCE_COLUMN = "ruian_kod_obce"
GEOCODING_STATUS_COLUMN = 'ruian_geocoding_status'

def extract_failed_records():
    print(f"Starting script. Reading input file: {INPUT_FILE}")
    try:
        df = pd.read_json(INPUT_FILE, encoding='utf-8')
        print(f"Successfully read {len(df)} rows from {INPUT_FILE}")
    except FileNotFoundError:
        print(f"Error: Input file '{INPUT_FILE}' not found. Make sure step 6 was run successfully.")
        return
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # Filtrování záznamů, které nemají ani kód adresního místa, ani kód obce
    # Používáme pd.isna(), protože chybějící hodnoty jsou NaN/NaT/None
    failed_df = df[df[KOD_OBCE_COLUMN].isna()].copy()
    
    print(f"Found {len(failed_df)} records that still failed to geocode.")

    if not failed_df.empty:
        # Uložení nepropojených záznamů do JSON
        print(f"Saving failed records to '{OUTPUT_FAILED_JSON_PATH}'...")
        try:
            failed_df.to_json(OUTPUT_FAILED_JSON_PATH, orient='records', indent=4, force_ascii=False)
            print(f"Successfully saved failed records to '{OUTPUT_FAILED_JSON_PATH}'")
        except Exception as e:
            print(f"Error saving JSON file: {e}")

        # Uložení nepropojených záznamů do CSV pro snadnou kontrolu
        print(f"Saving failed records to '{OUTPUT_FAILED_CSV_PATH}' for review...")
        try:
            failed_df.to_csv(OUTPUT_FAILED_CSV_PATH, index=False, encoding='utf-8')
            print(f"Successfully saved failed records to '{OUTPUT_FAILED_CSV_PATH}'")
        except Exception as e:
            print(f"Error saving CSV file: {e}")
    else:
        print("No failed records found. All addresses were successfully geocoded!")

    print("\nExtraction of failed records finished!")

if __name__ == "__main__":
    extract_failed_records()