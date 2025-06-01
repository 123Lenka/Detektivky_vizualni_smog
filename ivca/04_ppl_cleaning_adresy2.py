import pandas as pd
import re

# Načti CSV soubor
df = pd.read_csv("unikatni_ppl_pokus2.csv")  # Změň podle názvu tvého souboru

# Funkce pro rozdělení sloupce 'ulice_cislo'
def rozdel_ulici_cislo(text):
    match = re.match(r"(.+?)\s+(\d+[\/\dA-Za-z]*)$", text.strip())
    if match:
        return pd.Series([match.group(1).strip(), match.group(2).strip()])
    else:
        return pd.Series([text.strip(), None])  # fallback když není číslo

# Funkce pro rozdělení sloupce 'psc_mesto'
def rozdel_psc_mesto(text):
    match = re.match(r"(\d{3})\s?(\d{2})\s+(.+)", text.strip())
    if match:
        return pd.Series([match.group(1) + match.group(2), match.group(3).strip()])
    else:
        return pd.Series([None, text.strip()])

# Aplikuj funkce na jednotlivé sloupce
df[['ulice', 'cislo']] = df['ulice_cislo'].apply(rozdel_ulici_cislo)
df[['psc', 'mesto']] = df['psc_mesto'].apply(rozdel_psc_mesto)

# Ulož výsledek do nového CSV souboru
df[['ulice', 'cislo', 'psc', 'mesto']].to_csv("adresy_rozdelene.csv", index=False)

print("Hotovo! Uloženo jako 'adresy_rozdelene.csv'.")