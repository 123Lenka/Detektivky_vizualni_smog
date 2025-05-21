import pandas as pd

# Načti původní data
df_original = pd.read_csv(r"C:\Users\Sabina\Sabi dokumenty\01 Czechitas\01 PYTHON\Python\Detektivky_vizualni_smog\sabi\data\doctors_pomocny_kod_obce.csv", dtype=str)
df_doplnene = pd.read_csv(r"C:\Users\Sabina\Sabi dokumenty\01 Czechitas\01 PYTHON\Python\Detektivky_vizualni_smog\sabi\data\doctors_pomocny_doplnene_city_code.csv", dtype=str)

# Připrav jen sloupce potřebné ke spojení: Obec, Psc, city_code_merged
df_doplnit = df_doplnene[["Obec", "Psc", "city_code_merged"]].drop_duplicates()

# Spojení: přidáme city_code_merged jako nový sloupec
df_final = df_original.merge(
    df_doplnit,
    on=["Obec", "Psc"],
    how="left"
)

# Přejmenuj výsledek
df_final.rename(columns={"city_code_merged": "merged_city_code"}, inplace=True)

# Ulož jako nový soubor
df_final.to_csv("doctors_pomocny_kod_obce_merged_final.csv", index=False, encoding="utf-8-sig")
print("✅ Hotovo. merged_city_code přidán do doctors_pomocny_kod_obce_merged_final.csv")
