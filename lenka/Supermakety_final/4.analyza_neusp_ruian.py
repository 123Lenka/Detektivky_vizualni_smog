import pandas as pd
import os

# Ukončilo se vkládání ruian. 
# Přiřazené hodnoty vypadají pěkně.
# Vyfiltruji si tedy nepřiřazené adresy do speciálního souboru, abych je mohla lépe prozkoumat.

# --- Konfigurace cest k souborům ---
# Vstupní soubor z kroku 3 (geokódovaná data)
INPUT_GEOCODED_FILE_PATH = '3.vybrane_supermarkety_s_ruian_daty.json'
# Výstupní soubor pro neúspěšné záznamy (JSON)
OUTPUT_FAILED_JSON_PATH = '4.vybrane_supermarkety_nepodarilo_geokodovat.json'
# Výstupní soubor pro neúspěšné záznamy (CSV pro snadnou kontrolu)
OUTPUT_FAILED_CSV_PATH = '4.vybrane_supermarkety_nepodarilo_geokodovat.csv'

# Sloupec, který kontrolujeme pro neúspěšné geokódování
RUIAN_KOD_OBCE_COLUMN = 'ruian_kod_obce'
GEOCODING_STATUS_COLUMN = 'ruian_geocoding_status'

def analyze_failed_ruian_records():
    print(f"Načítám geokódovaná data z '{INPUT_GEOCODED_FILE_PATH}' pro analýzu neúspěšných záznamů...")
    try:
        df = pd.read_json(INPUT_GEOCODED_FILE_PATH, encoding='utf-8')
        print(f"Načteno {len(df)} záznamů.")
    except FileNotFoundError:
        print(f"CHYBA: Vstupní soubor '{INPUT_GEOCODED_FILE_PATH}' nebyl nalezen. Ujistěte se, že je ve správné složce.")
        return
    except Exception as e:
        print(f"CHYBA při načítání dat: {e}")
        return

    # Filtrujeme DataFrame pro záznamy, které nemají RÚIAN kód obce
    # Kontrolujeme, zda je sloupec RUIAN_KOD_OBCE_COLUMN NaN (Not a Number) nebo prázdný
    df_failed = df[df[RUIAN_KOD_OBCE_COLUMN].isna()].copy()
    
    total_records = len(df)
    failed_count = len(df_failed)

    print(f"\nAnalýza dokončena.")
    print(f"Celkový počet záznamů: {total_records}")
    print(f"Počet záznamů bez přiřazeného RÚIAN Kód Obce: {failed_count}")
    print(f"Úspěšně geokódováno: {total_records - failed_count} záznamů.")

    if not df_failed.empty:
        # Uložíme neúspěšné záznamy do JSON souboru
        print(f"\nUkládám {failed_count} neúspěšných záznamů do '{OUTPUT_FAILED_JSON_PATH}' (JSON)...")
        try:
            # Opět bez ensure_ascii=False pro kompatibilitu se staršími Pandas
            df_failed.to_json(OUTPUT_FAILED_JSON_PATH, orient='records', indent=4)
            print(f"Neúspěšné záznamy uloženy do JSON souboru: '{OUTPUT_FAILED_JSON_PATH}'")
        except Exception as e:
            print(f"CHYBA při ukládání neúspěšných záznamů do JSON: {e}")

        # Uložíme neúspěšné záznamy do CSV souboru (pro snadnou vizuální kontrolu)
        print(f"\nUkládám {failed_count} neúspěšných záznamů do '{OUTPUT_FAILED_CSV_PATH}' (CSV)...")
        try:
            df_failed.to_csv(OUTPUT_FAILED_CSV_PATH, index=False, encoding='utf-8')
            print(f"Neúspěšné záznamy uloženy do CSV souboru: '{OUTPUT_FAILED_CSV_PATH}'")
        except Exception as e:
            print(f"CHYBA při ukládání neúspěšných záznamů do CSV: {e}")
    else:
        print("\nVšechny záznamy byly úspěšně geokódovány, žádné neúspěšné záznamy k uložení.")

    print("\nAnalýza neúspěšných RÚIAN záznamů dokončena!")

if __name__ == "__main__":
    analyze_failed_ruian_records()