
import pandas as pd

# --- Cesty k souborům ---
INPUT_DOCTORS = r"C:\Users\Sabina\Sabi dokumenty\01 Czechitas\01 PYTHON\Python\Detektivky_vizualni_smog\sabi\data\doctors_pomocny_lines_missing_city_code_only.csv"
INPUT_ADDRESSES = r"C:\Users\Sabina\Sabi dokumenty\01 Czechitas\01 PYTHON\Python\combined_addresses_cz_cleaned.csv"
OUTPUT_FILE = "doctors_pomocny_doplnene_city_code.csv"
MAX_ROWS = None  # Např. 1000 pro omezený test

# --- Načtení souborů ---
doctors = pd.read_csv(INPUT_DOCTORS, dtype=str)
if MAX_ROWS:
    doctors = doctors.head(MAX_ROWS)

addresses = pd.read_csv(INPUT_ADDRESSES, dtype=str)

# --- Čištění ---
doctors["Obec_clean"] = doctors["Obec"].str.strip().str.lower()
doctors["Psc_clean"] = doctors["Psc"].str.strip()

addresses["city_clean"] = addresses["city"].str.strip().str.lower()
addresses["postal_code_clean"] = addresses["postal_code"].str.strip()

# --- Vybrat unikátní kombinace pro merge ---
addresses_unique = addresses.drop_duplicates(subset=["city_clean", "postal_code_clean"])[["city_clean", "postal_code_clean", "city_code"]]

# --- Sloučení ---
merged = doctors.merge(
    addresses_unique,
    how="left",
    left_on=["Obec_clean", "Psc_clean"],
    right_on=["city_clean", "postal_code_clean"]
)

# --- Kontrola, jestli sloupec vznikl ---
if "city_code" in merged.columns:
    merged["city_code_merged"] = merged["city_code"]
    print(f"✅ Spárováno {merged['city_code'].notna().sum()} z {len(merged)} řádků.")
else:
    merged["city_code_merged"] = None
    print("⚠️ Sloupec city_code nevznikl – nespárováno nic.")

# --- Uložení ---
merged.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
print(f"Uloženo do: {OUTPUT_FILE}")