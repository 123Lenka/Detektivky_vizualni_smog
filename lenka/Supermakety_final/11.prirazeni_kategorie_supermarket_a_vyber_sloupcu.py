import pandas as pd
import json

# Potřebuji ve výsledném datasetu smazat řádky, ve kterých není ruian kod obce vyplněn, mělo by jich být 197, 
# potom potřebuji do sloupce id zadat nové hodnoty začínající číslem 1 a zvyšující se vždy o 1 -1 2 3 4 atd. 
# Potom bych chtěla ponechat sloupec tittle a přejmenovat ho na nazev
# Dále zustane náš sloupec s ruian kod obce, který se bude jmenovat kod obce
# Poslední sloupec který ponecháme je categoryName, přejmenujeme jej na id kategorie obchod.
# V tomto sloupci přepíšeme všechny hodnoty podle následujícího schématu

# --- Konfigurace cest k souborům ---
# Ujistěte se, že tento soubor existuje ve stejném adresáři jako tento skript
INPUT_FILE = '10.slouceni_ruian_kod_obce_only.json'

OUTPUT_CLEANED_JSON_PATH = '11.final_cleaned_supermarkets_data.json'
OUTPUT_CLEANED_CSV_PATH = '11.final_cleaned_supermarkets_data.csv'

# Mapping pro 'id kategorie obchod' - aktualizováno dle vašeho posledního vstupu
CATEGORY_MAPPING = {
    1: ['Obchod s potravinami', 'Prodejna', 'Diskontní samoobsluha', 'Smíšené zboží', 'Obchody a nakupování','Supermarket', 'Tržnice'],
    2: ['Velkoobchod', 'Velkoobchod s potravinami','Hypermarket'],
    3: ['Prodejna ovoce a zeleniny', 'Tržnice s ovocem a zeleniny','Velkoobchod s ovocem a zeleninou'],
    4: ['Pekařství', 'Pekárna', 'Pekárenství', 'Prodejna pečiva – bagelů'],
    5: ['Řeznictví', 'Velkoobchod s masem', 'Prodejna uzenin', 'Uzenářství', 'Masna']

}
def clean_and_transform_data():
    print(f"Načítám data ze souboru: {INPUT_FILE}")
    try:
        df = pd.read_json(INPUT_FILE, encoding='utf-8')
        print(f"Načteno {len(df)} záznamů.")
    except FileNotFoundError:
        print(f"CHYBA: Vstupní soubor '{INPUT_FILE}' nebyl nalezen. Ujistěte se, že je ve stejném adresáři jako tento skript.")
        return
    except Exception as e:
        print(f"CHYBA při načítání souboru '{INPUT_FILE}': {e}")
        return

    # 1. Smazání řádků, kde není ruian_kod_obce vyplněn
    initial_rows = len(df)
    df_cleaned = df.dropna(subset=['ruian_kod_obce']).copy()
    rows_removed = initial_rows - len(df_cleaned)
    print(f"Původní počet řádků: {initial_rows}")
    print(f"Počet řádků bez vyplněného 'ruian_kod_obce' smazaných: {rows_removed}")
    print(f"Nový počet řádků po smazání: {len(df_cleaned)}")

    # 2. Přejmenování sloupců
    df_cleaned = df_cleaned.rename(columns={
        'title': 'název',
        'ruian_kod_obce': 'kod obce',
        'categoryName': 'id kategorie obchod'
    })

    # Ponechání požadovaných sloupců
    # Použijeme původní 'id' pro prozatímní výběr, ale bude přepsáno v dalším kroku
    df_final = df_cleaned[['id', 'název', 'kod obce', 'id kategorie obchod']].copy()

    # 3. Získání nových hodnot pro sloupec 'id'
    # Resetujeme index a použijeme ho jako nové id (+1, protože index začíná od 0)
    df_final.reset_index(drop=True, inplace=True)
    df_final['id'] = df_final.index + 1
    print("Sloupec 'id' přečíslován od 1.")

    # 4. Přepsání hodnot ve sloupci 'id kategorie obchod' podle mapování
    # Vytvoříme reverzní mapování pro snazší aplikaci
    reverse_category_mapping = {}
    for key, values in CATEGORY_MAPPING.items():
        for value in values:
            reverse_category_mapping[value] = key
    
    # Aplikujeme mapování. Hodnoty, které nejsou v mapování, se změní na NaN.
    df_final['id kategorie obchod'] = df_final['id kategorie obchod'].map(reverse_category_mapping)
    
    # Kontrola, zda nezůstaly nějaké nenamapované hodnoty
    unmapped_categories = df_final[df_final['id kategorie obchod'].isna()]['id kategorie obchod'].unique()
    if len(unmapped_categories) > 0:
        print(f"Upozornění: Následující kategorie nebyly namapovány na číselné ID (zůstaly NaN): {unmapped_categories}")

    print("Sloupec 'id kategorie obchod' aktualizován podle mapování.")

    # --- Pevné nastavení datových typů ---
    print("\nNastavuji datové typy sloupců...")
    try:
        df_final['id'] = df_final['id'].astype(int)
        # Typ 'string' pro textové sloupce je v pandas doporučován pro lepší práci s chybějícími hodnotami
        df_final['název'] = df_final['název'].astype(str)
        df_final['kod obce'] = df_final['kod obce'].astype(int)
        # 'Int64' (s velkým I) je celočíselný typ v pandas, který podporuje NaN hodnoty
        # Pokud chcete mít jistotu, že tam NaN nebudou, musely by se nenamapované hodnoty řešit jinak (např. -1)
        df_final['id kategorie obchod'] = df_final['id kategorie obchod'].astype('Int64') 
        print("Datové typy úspěšně nastaveny.")
    except Exception as e:
        print(f"CHYBA při nastavování datových typů: {e}")
        print("Ujistěte se, že všechny sloupce obsahují data kompatibilní s požadovaným typem (např. žádný text v číselném sloupci, pokud není zpracován).")


    # Uložení výsledného datasetu
    print(f"\nUkládám finalizovaná data do '{OUTPUT_CLEANED_JSON_PATH}' a '{OUTPUT_CLEANED_CSV_PATH}'...")
    try:
        df_final.to_json(OUTPUT_CLEANED_JSON_PATH, orient='records', indent=4, force_ascii=False)
        print(f"Finalizovaná data uložena do JSON souboru: '{OUTPUT_CLEANED_JSON_PATH}'")
    except Exception as e:
        print(f"CHYBA při ukládání finalizovaných dat do JSON: {e}")

    try:
        df_final.to_csv(OUTPUT_CLEANED_CSV_PATH, index=False, encoding='utf-8')
        print(f"Finalizovaná data uložena do CSV souboru: '{OUTPUT_CLEANED_CSV_PATH}'.")
    except Exception as e:
        print(f"CHYBA při ukládání finalizovaných dat do CSV: {e}")

    print("\nTransformace dat dokončena!")

# Spustit hlavní funkci
if __name__ == "__main__":
    clean_and_transform_data()