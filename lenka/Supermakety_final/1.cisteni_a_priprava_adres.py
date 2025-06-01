import pandas as pd
import re
import numpy as np



# Potřebujeme do kódu vložit identifikátor, nový sloupec id začínající číslem 1
# Potřebuji rozdělit adresu podle těchto parametrů - 
# Když budu číst adresu od zadu nejprve narazím na slovo Česko, potom je čárka, toto je stát, to nepotřebuji
# Po státu odděleném čárkou následuje město v různých podobách (city)
# jednoslovné, dvouslovné oddělené mezerou, někdy i více slovné 
# Co je ale důležité, že před tímto různým městem 
# by mělo být vě většině případů PSČ v této formě s mezerou 251 64 a před ním je čárka
# před čárkou zbývá ulice a č.p.
# Od předu by to bylo tedy první ulice a čp. v různých podobách, čárka, psč mezera město v různých podobách, čárka, stát
# V původních datech sloupec street, city a postalcode jsou, ale city často bývá null, 
# oproti tomu postal code bývá většinou v pořádku. 
# Šlo by použít, že by se procházel sloupec celé adresy (uvažujeme teď, že čteme adresu od předu)
# a když by se v ní narazilo na hodnotu, která je ve sloupci postalCode, tak by se zbystřilo a vědělo, 
# že za tímto číslem je mezera a za ní až do čárky město. To potřebujeme vytáhnout do nového sloupce(cityClean), 
# který budeme používat pro vyhledávání kodu obce. Anebo prostě jen číst řádek od předu, 
# vše do první řádky ignorujeme, po první  čárce je mezera postalCode (ve formátu s mezerou) mezera a město. 
# To potřebujeme.
# Nakonec je potřeba ještě u dané logiky v případě týkající se street a city odfiltrovat čísla, 
# která u nich někdy jsou, jako Praha 5, chceme jen Praha, nebo zůstává číslo popisné, které je v datech někdy nesčetněkrát

# --- Konfigurace cest k souborům ---
# Původní raw data
INPUT_RAW_FILE_PATH = 'dataset_crawler-google-places_2025-04-25_13-53-27-998.json'

# Výstupní soubor s ID a rozdělenou adresou (1. krok)
OUTPUT_CLEANED_FILE_PATH = '1.vybrane_supermarkety_s_id_a_rozdelenou_adresou.json'
# CSV pro rychlou kontrolu výsledků (1. krok)
OUTPUT_CHECK_CSV_PATH = '1.vybrane_supermarkety_pro_kontrolu.csv'

def clean_and_split_address():
    print(f"Načítám raw data z '{INPUT_RAW_FILE_PATH}'...")
    try:
        df = pd.read_json(INPUT_RAW_FILE_PATH, encoding='utf-8')
        print(f"Načteno {len(df)} záznamů.")
    except FileNotFoundError:
        print(f"CHYBA: Vstupní soubor '{INPUT_RAW_FILE_PATH}' nebyl nalezen. Ujistěte se, že je ve správné složce.")
        return
    except Exception as e:
        print(f"CHYBA při načítání dat: {e}")
        return

    # 1. Přidání nového sloupce 'id'
    df['id'] = range(1, len(df) + 1)
    print(f"Přidán sloupec 'id' s hodnotami od 1 do {len(df)}.")

    # Standardizujeme PSČ na formát XXXXX bez mezery pro lepší práci s ním
    df['zip_code_cleaned'] = df['postalCode'].astype(str).str.replace(' ', '').str.strip()

    # Nové sloupce pro vyčištěné části adresy
    df['city_cleaned'] = pd.NA
    df['street_cleaned'] = pd.NA

    print("Spouštím čištění a rozdělení adres...")

    for index, row in df.iterrows():
        address = str(row['address']).strip() if pd.notna(row['address']) else ''
        # Není třeba postal_code_cleaned zde, protože ho budeme extrahovat z address_temp

        if not address:
            continue

        # Krok 1: Odstraníme ", Česko" na konci adresy
        address_temp = re.sub(r',\s*Česko$', '', address).strip()

        # Krok 2: Rozdělíme adresu na části podle čárek
        parts = [p.strip() for p in address_temp.split(',')]

        # Logika pro extrakci street_cleaned, zip_code_cleaned a city_cleaned
        current_street = ""
        current_zip = ""
        current_city = ""

        # Projdeme části odzadu, abychom našli PSČ a město
        # (město je obvykle za PSČ)
        for i in range(len(parts) - 1, -1, -1):
            part = parts[i]
            
            # Hledáme PSČ ve tvaru "DDDDD" nebo "DDD DD"
            match_psc = re.search(r'\b(\d{3}\s*\d{2})\b', part)
            
            if match_psc:
                current_zip = match_psc.group(1).replace(' ', '').strip()
                df.at[index, 'zip_code_cleaned'] = current_zip
                
                # Zbytek části za PSČ by mohlo být město
                city_from_part = re.sub(r'\b\d{3}\s*\d{2}\b', '', part).strip()
                if city_from_part:
                    current_city = city_from_part
                    df.at[index, 'city_cleaned'] = current_city
                
                # Vše před touto částí s PSČ je ulice
                street_parts = parts[:i]
                current_street = ", ".join(street_parts).strip()
                df.at[index, 'street_cleaned'] = current_street
                break # Máme PSČ, ulici a město, můžeme ukončit hledání

        # Pokud jsme nenašli PSČ v adrese nebo město z adresy není OK
        if pd.isna(df.at[index, 'city_cleaned']):
            # Pokusíme se použít original 'city' nebo 'neighborhood'
            if pd.notna(row['city']) and str(row['city']).strip():
                df.at[index, 'city_cleaned'] = str(row['city']).strip()
            elif pd.notna(row['neighborhood']) and str(row['neighborhood']).strip():
                df.at[index, 'city_cleaned'] = str(row['neighborhood']).strip()
            
            # Pokud se PSČ nevyextrahovalo z adresy, ale máme ho v 'postalCode'
            if pd.isna(df.at[index, 'zip_code_cleaned']) and pd.notna(row['postalCode']) and str(row['postalCode']).strip():
                df.at[index, 'zip_code_cleaned'] = str(row['postalCode']).strip().replace(' ', '')
            
            # Pokud se ulice nevyextrahovala, vezmeme ji z 'street'
            if pd.isna(df.at[index, 'street_cleaned']) and pd.notna(row['street']):
                df.at[index, 'street_cleaned'] = str(row['street']).strip()
        
        # --- Zde přidáme dodatečné čištění pro city_cleaned a street_cleaned ---
        # Pořádně vyčistit city_cleaned od koncových čísel
        if pd.notna(df.at[index, 'city_cleaned']):
            df.at[index, 'city_cleaned'] = re.sub(r'\s*\d+$', '', str(df.at[index, 'city_cleaned'])).strip()

        # Pořádně vyčistit street_cleaned od čísel a čárek na konci
        if pd.notna(df.at[index, 'street_cleaned']):
            # Odstranit čísla popisná/orientační a případné lomítka na konci
            # např. "Liberecká 47", "Jiráskova 590/20"
            df.at[index, 'street_cleaned'] = re.sub(r'\s+\d+(?:[/\-]\d+)?[a-zA-Z]?$', '', str(df.at[index, 'street_cleaned'])).strip()
            # Odstranit případné čárky na konci
            df.at[index, 'street_cleaned'] = str(df.at[index, 'street_cleaned']).strip(',')


    # Převést extrahované PSČ na Int64Dtype, které podporuje NaN
    df['zip_code_cleaned'] = pd.to_numeric(df['zip_code_cleaned'], errors='coerce').astype(pd.Int64Dtype())

    # Jako poslední krok si vytáhnu jen několik sloupců pro potřeby kontroly dat v tomto pořadí 
    # id tittle categoryName address city cityClean street clean postalCode zipCode clean

    # 3. Vytažení jen potřebných sloupců
    final_df = df[['id', 'title', 'categoryName', 'address', 'city', 'city_cleaned', 
                   'street', 'street_cleaned', 'postalCode', 'zip_code_cleaned']].copy()
    
    print("\nČištění a rozdělení adres dokončeno.")
    print(f"Počet záznamů s vyplněným 'city_cleaned': {final_df['city_cleaned'].count()} z {len(final_df)}.")
    print(f"Počet záznamů s vyplněným 'zip_code_cleaned': {final_df['zip_code_cleaned'].count()} z {len(final_df)}.")


    # Uložení kompletních dat do JSON
    print(f"\nUkládám vyčištěná data do '{OUTPUT_CLEANED_FILE_PATH}'...")
    try:
        try:
            final_df.to_json(OUTPUT_CLEANED_FILE_PATH, orient='records', indent=4, ensure_ascii=False)
        except TypeError:
            print("Varování: Vaše verze Pandas nepodporuje 'ensure_ascii' v to_json(). Ukládám bez něj.")
            final_df.to_json(OUTPUT_CLEANED_FILE_PATH, orient='records', indent=4)
        print(f"Vyčištěná data uložena do JSON souboru: '{OUTPUT_CLEANED_FILE_PATH}'")
    except Exception as e:
        print(f"CHYBA při ukládání vyčištěných dat do JSON: {e}")

    # Uložení pro kontrolu do CSV
    print(f"\nUkládám data pro kontrolu do '{OUTPUT_CHECK_CSV_PATH}'...")
    try:
        final_df.to_csv(OUTPUT_CHECK_CSV_PATH, index=False, encoding='utf-8')
        print(f"Kontrolní CSV soubor uložen do: '{OUTPUT_CHECK_CSV_PATH}'.")
    except Exception as e:
        print(f"CHYBA při ukládání kontrolního CSV souboru: {e}")

    print("\nPříprava dat dokončena!")


if __name__ == "__main__":
    clean_and_split_address()