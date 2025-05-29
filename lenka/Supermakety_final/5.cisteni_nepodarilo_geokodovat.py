import pandas as pd
import re
import os

#  V nepřiřazených adresách (3373 řádků) je problém s městem. 
# Zůstaly hodnoty, které mají formát s pomlčkou Brankovice-Nesovice, 
# nebo s předložkou Boršice u Buchlovic, 
# nebo s pomlčkou a číslem Písek 1-Budějovické Předměstí. 
# Navrhuji tyto města vyčistit tak, že vše, co je před pomlčkou zůstane v city cleaned 
# a do toho by se tato první část města mohla vyčistit i od dalších možných znaků, pokud tam jsou, 
# jako v případě Praha mezera jedna. Část za pomlčkou by se uložila do nového sloupce cityPart. 
# Nežádoucí znaky za touto částí města nemáme, 
# jelikož jsme je vyčistili už v bodě 1 v rámci čištění celého města.

# --- Konfigurace cest k souborům ---
# Vstupní soubor z kroku 4 (neúspěšné záznamy)
INPUT_FAILED_JSON_PATH = '4.vybrane_supermarkety_nepodarilo_geokodovat.json'
# Výstupní soubor s vyčištěnými neúspěšnými záznamy (JSON pro re-geokódování)
OUTPUT_CLEANED_FAILED_JSON_PATH = '5.vybrane_supermarkety_nepodarilo_geokodovat_vycisteno.json'
# Výstupní soubor s vyčištěnými neúspěšnými záznamy (CSV pro kontrolu)
OUTPUT_CLEANED_FAILED_CSV_PATH = '5.vybrane_supermarkety_nepodarilo_geokodovat_vycisteno.csv'


CITY_CLEANED_COL = 'city_cleaned'
# Nový sloupec pro část města po pomlčce
CITY_PART_COL = 'cityPart'

def clean_failed_city_names():
    print(f"Načítám neúspěšné záznamy z '{INPUT_FAILED_JSON_PATH}' pro další čištění...")
    try:
        df = pd.read_json(INPUT_FAILED_JSON_PATH, encoding='utf-8')
        print(f"Načteno {len(df)} záznamů k čištění.")
    except FileNotFoundError:
        print(f"CHYBA: Vstupní soubor '{INPUT_FAILED_JSON_PATH}' nebyl nalezen. Ujistěte se, že je ve správné složce.")
        return
    except Exception as e:
        print(f"CHYBA při načítání dat: {e}")
        return

    # Vytvoření nového sloupce pro části města
    df[CITY_PART_COL] = pd.NA

    # Funkce pro čištění názvu města
    def clean_city_name_logic(city_name):
        if pd.isna(city_name):
            return pd.NA, pd.NA
        
        original_city_name = str(city_name).strip()
        cleaned_city = original_city_name
        city_part = pd.NA

        # 1. Rozdělení podle první pomlčky
        if '-' in original_city_name:
            parts = original_city_name.split('-', 1) # Rozdělit jen na dvě části u první pomlčky
            cleaned_city = parts[0].strip()
            city_part = parts[1].strip()
        
        # 2. Odstranění "mezera číslo" na konci hlavní části města (např. "Praha 1" -> "Praha")
        # Regulární výraz: najde mezeru a jedno nebo více čísel ($ na konci řetězce)
        cleaned_city = re.sub(r'\s+\d+$', '', cleaned_city).strip()

        return cleaned_city, city_part

    # Aplikace čisticí logiky na DataFrame
    df[[CITY_CLEANED_COL, CITY_PART_COL]] = df.apply(
        lambda row: clean_city_name_logic(row[CITY_CLEANED_COL]), 
        axis=1, 
        result_type='expand'
    )
    
    print("\nČištění názvů měst dokončeno.")
    print(f"Nyní uložíme {len(df)} upravených záznamů pro další zpracování.")

    # Uložení upravených dat do JSON
    print(f"\nUkládám vyčištěná data do '{OUTPUT_CLEANED_FAILED_JSON_PATH}' (JSON)...")
    try:
        df.to_json(OUTPUT_CLEANED_FAILED_JSON_PATH, orient='records', indent=4)
        print(f"Vyčištěná data uložena do JSON souboru: '{OUTPUT_CLEANED_FAILED_JSON_PATH}'")
    except Exception as e:
        print(f"CHYBA při ukládání vyčištěných dat do JSON: {e}")

    # Uložení upravených dat do CSV pro kontrolu
    print(f"\nUkládám vyčištěná data pro kontrolu do '{OUTPUT_CLEANED_FAILED_CSV_PATH}' (CSV)...")
    try:
        df.to_csv(OUTPUT_CLEANED_FAILED_CSV_PATH, index=False, encoding='utf-8')
        print(f"Kontrolní CSV soubor uložen do: '{OUTPUT_CLEANED_FAILED_CSV_PATH}'.")
    except Exception as e:
        print(f"CHYBA při ukládání kontrolního CSV souboru: {e}")

    print("\nČištění neúspěšných záznamů dokončeno!")


if __name__ == "__main__":
    clean_failed_city_names()