import pandas as pd
import json

# Další čištění a propojení se už nepodařilo. Zůstalo 197 řádků z 16571 nevyplněno.
# Další krok bude všechny datasety s ruiany sjednotit v jeden.

# --- Konfigurace cest k souborům ---
INPUT_FILE_3 = '3.vybrane_supermarkety_s_ruian_daty.json'
INPUT_FILE_6 = '6.vybrane_supermarkety_regeokodovano_ruian_data.json'
INPUT_FILE_8_SUCCESS = '8.vybrane_supermarkety_regeokodovano_cistecity.json' # Soubor s úspěšnými geokódováními z kroku 8

OUTPUT_MERGED_JSON_PATH = '10.slouceni_ruian_kod_obce_only.json'
OUTPUT_MERGED_CSV_PATH = '10.slouceni_ruian_kod_obce_only.csv'

# Sloupec, který budeme doplňovat
KOD_OBCE_COLUMN = "ruian_kod_obce"
GEOCODING_STATUS_COLUMN = "ruian_geocoding_status" # Doplňuje se i status, aby bylo vidět, odkud data pochází

def merge_ruian_kod_obce():
    print("Načítám data z jednotlivých fází geokódování...")

    try:
        df_3 = pd.read_json(INPUT_FILE_3, encoding='utf-8')
        print(f"Načteno {len(df_3)} záznamů z '{INPUT_FILE_3}'")
    except FileNotFoundError:
        print(f"CHYBA: Soubor '{INPUT_FILE_3}' nebyl nalezen. Ujistěte se, že je ve stejném adresáři jako skript.")
        return
    except Exception as e:
        print(f"CHYBA při načítání '{INPUT_FILE_3}': {e}")
        return

    try:
        df_6 = pd.read_json(INPUT_FILE_6, encoding='utf-8')
        print(f"Načteno {len(df_6)} záznamů z '{INPUT_FILE_6}'")
    except FileNotFoundError:
        print(f"CHYBA: Soubor '{INPUT_FILE_6}' nebyl nalezen. Ujistěte se, že je ve stejném adresáři jako skript.")
        return
    except Exception as e:
        print(f"CHYBA při načítání '{INPUT_FILE_6}': {e}")
        return

    try:
        df_8_success = pd.read_json(INPUT_FILE_8_SUCCESS, encoding='utf-8')
        print(f"Načteno {len(df_8_success)} záznamů z '{INPUT_FILE_8_SUCCESS}'")
    except FileNotFoundError:
        print(f"Upozornění: Soubor '{INPUT_FILE_8_SUCCESS}' nebyl nalezen. Sloučení proběhne bez dat z kroku 8.")
        df_8_success = pd.DataFrame(columns=['id', KOD_OBCE_COLUMN, GEOCODING_STATUS_COLUMN]) # Prázdný DF
    except Exception as e:
        print(f"CHYBA při načítání '{INPUT_FILE_8_SUCCESS}': {e}")
        df_8_success = pd.DataFrame(columns=['id', KOD_OBCE_COLUMN, GEOCODING_STATUS_COLUMN]) # Prázdný DF
        print("Upozornění: Chyba při načítání souboru 8, sloučení proběhne bez něj.")

    # Nastavíme 'id' jako index pro efektivní slučování
    df_3 = df_3.set_index('id')
    df_6 = df_6.set_index('id')
    df_8_success = df_8_success.set_index('id')

    # Začneme s df_3 jako základem pro sloučená data
    merged_df = df_3.copy()

    print("\nProvádím slučování 'ruian_kod_obce' (priorita: Krok 8 > Krok 6 > Krok 3)...")

    # Krok 1: Doplnění 'ruian_kod_obce' a 'ruian_geocoding_status' z df_6, pokud je v merged_df NaN
    ids_to_update_from_6 = df_6[df_6[KOD_OBCE_COLUMN].notna()].index.intersection(merged_df[merged_df[KOD_OBCE_COLUMN].isna()].index)
    if not ids_to_update_from_6.empty:
        merged_df.loc[ids_to_update_from_6, KOD_OBCE_COLUMN] = df_6.loc[ids_to_update_from_6, KOD_OBCE_COLUMN]
        # Doplňte i status, aby bylo vidět, odkud data pochází
        merged_df.loc[ids_to_update_from_6, GEOCODING_STATUS_COLUMN] = df_6.loc[ids_to_update_from_6, GEOCODING_STATUS_COLUMN]
        print(f"Doplněno {len(ids_to_update_from_6)} kódů obcí z '{INPUT_FILE_6}'.")

    # Krok 2: Doplnění 'ruian_kod_obce' a 'ruian_geocoding_status' z df_8_success, pokud je v merged_df stále NaN
    ids_to_update_from_8 = df_8_success[df_8_success[KOD_OBCE_COLUMN].notna()].index.intersection(merged_df[merged_df[KOD_OBCE_COLUMN].isna()].index)
    if not ids_to_update_from_8.empty:
        merged_df.loc[ids_to_update_from_8, KOD_OBCE_COLUMN] = df_8_success.loc[ids_to_update_from_8, KOD_OBCE_COLUMN]
        # Doplňte i status, aby bylo vidět, odkud data pochází
        merged_df.loc[ids_to_update_from_8, GEOCODING_STATUS_COLUMN] = df_8_success.loc[ids_to_update_from_8, GEOCODING_STATUS_COLUMN]
        print(f"Doplněno {len(ids_to_update_from_8)} kódů obcí z '{INPUT_FILE_8_SUCCESS}'.")
        
    # Resetujeme index 'id' zpět na sloupec
    merged_df = merged_df.reset_index()

    print(f"\nSloučení dokončeno. Celkový počet záznamů: {len(merged_df)}")
    final_geocoded_count = merged_df[KOD_OBCE_COLUMN].notna().sum()
    final_failed_count = merged_df[KOD_OBCE_COLUMN].isna().sum()
    print(f"Počet záznamů s doplněným '{KOD_OBCE_COLUMN}': {final_geocoded_count}")
    print(f"Počet záznamů stále bez '{KOD_OBCE_COLUMN}': {final_failed_count}")

    # Uložení sloučených dat do JSON
    print(f"\nUkládám sloučená data do '{OUTPUT_MERGED_JSON_PATH}'...")
    try:
        merged_df.to_json(OUTPUT_MERGED_JSON_PATH, orient='records', indent=4, force_ascii=False)
        print(f"Sloužená data uložena do JSON souboru: '{OUTPUT_MERGED_JSON_PATH}'")
    except Exception as e:
        print(f"CHYBA při ukládání sloučených dat do JSON: {e}")

    # Uložení sloučených dat do CSV pro kontrolu
    print(f"\nUkládám sloučená data pro kontrolu do '{OUTPUT_MERGED_CSV_PATH}'...")
    try:
        merged_df.to_csv(OUTPUT_MERGED_CSV_PATH, index=False, encoding='utf-8')
        print(f"Kontrolní CSV soubor uložen do: '{OUTPUT_MERGED_CSV_PATH}'.")
    except Exception as e:
        print(f"CHYBA při ukládání kontrolního CSV souboru: {e}")

    print("\nProces sloučení 'ruian_kod_obce' dokončen!")

# Spustit funkci
merge_ruian_kod_obce()