import pandas as pd
import json
import os

# Adresy vypadají v pořádku, pro zmenšení velikosti datasetu budu dál pracovat jen s nameCategory, 
# které se týká obchodů, nakupování a potravin. 
# V datasetu jsou i jiné hodnoty, které nepotřebujeme - kavárna, apartmána...

# --- Konfigurace cest k souborům ---
# Vstupní soubor z prvního kroku (vyčištěné adresy)
INPUT_CLEANED_FILE_PATH = '1.vybrane_supermarkety_s_id_a_rozdelenou_adresou.json'

# Výstupní soubor s vyfiltrovanými kategoriemi (JSON)
OUTPUT_FILTERED_JSON_PATH = '2.vybrane_supermarkety_filtrovane_kategorie.json'
# Výstupní soubor s vyfiltrovanými kategoriemi (CSV pro kontrolu)
OUTPUT_FILTERED_CSV_PATH = '2.vybrane_supermarkety_filtrovane_kategorie_pro_kontrolu.csv'

# Seznam přesných názvů kategorií, které chceme zachovat
ALLOWED_CATEGORIES = [
    'Obchod s potravinami',
    'Prodejna',
    'Diskontní samoobsluha',
    'Smíšené zboží',
    'Obchody a nakupování',
    'Prodejna ovoce a zeleniny',
    'Tržnice s ovocem a zeleniny',
    'Velkoobchod s ovocem a zeleniny',
    'Supermarket',
    'Hypermarket',
    'Pekařství',
    'Pekárna',
    'Pekárenství',
    'Prodejna pečiva – bagelů',
    'Řeznictví',
    'Velkoobchod s masem',
    'Prodejna uzenin',
    'Uzenářství',
    'Masna',
    'Velkoobchod',
    'Velkoobchod s potravinami',
    'Tržnice'
]

def filter_data_by_category():
    print(f"Načítám data z '{INPUT_CLEANED_FILE_PATH}' pro filtraci kategorií...")
    try:
        df = pd.read_json(INPUT_CLEANED_FILE_PATH, encoding='utf-8')
        print(f"Načteno {len(df)} záznamů.")
    except FileNotFoundError:
        print(f"CHYBA: Vstupní soubor '{INPUT_CLEANED_FILE_PATH}' nebyl nalezen. Ujistěte se, že je ve správné složce.")
        return
    except Exception as e:
        print(f"CHYBA při načítání dat: {e}")
        return

    print(f"\nBudou zachovány pouze záznamy s následujícími kategoriemi (přesná shoda):")
    for cat in ALLOWED_CATEGORIES:
        print(f"- {cat}")

    # Filtrujeme DataFrame na základě sloupce 'categoryName'
    # Použijeme .isin() pro efektivní filtrování
    df_filtered = df[df['categoryName'].isin(ALLOWED_CATEGORIES)].copy()

    original_count = len(df)
    filtered_count = len(df_filtered)

    print(f"\nFiltrace dokončena.")
    print(f"Původní počet záznamů: {original_count}")
    print(f"Počet záznamů po filtraci: {filtered_count}")
    print(f"Odstraněno záznamů: {original_count - filtered_count}")

    # Uložení vyfiltrovaných dat do JSON
    print(f"\nUkládám vyfiltrovaná data do '{OUTPUT_FILTERED_JSON_PATH}'...")
    try:
        # Zde odstraněn argument 'ensure_ascii=False'
        df_filtered.to_json(OUTPUT_FILTERED_JSON_PATH, orient='records', indent=4)
        print(f"Vyfiltrovaná data uložena do JSON souboru: '{OUTPUT_FILTERED_JSON_PATH}'")
    except Exception as e:
        print(f"CHYBA při ukládání vyfiltrovaných dat do JSON: {e}")

    # Uložení vyfiltrovaných dat do CSV pro kontrolu
    print(f"\nUkládám vyfiltrovaná data pro kontrolu do '{OUTPUT_FILTERED_CSV_PATH}'...")
    try:
        df_filtered.to_csv(OUTPUT_FILTERED_CSV_PATH, index=False, encoding='utf-8')
        print(f"Kontrolní CSV soubor uložen do: '{OUTPUT_FILTERED_CSV_PATH}'.")
    except Exception as e:
        print(f"CHYBA při ukládání kontrolního CSV souboru: {e}")

    print("\nFiltrace kategorií dokončena!")


if __name__ == "__main__":
    filter_data_by_category()