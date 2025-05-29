import pandas as pd

# Data pro tabulku kategorií
data = {
    'id': [1, 2, 3, 4, 5],
    'nazev': [
        'Obchod s potravinami',
        "Velkoobchod",
        'Prodejna ovoce a zeleniny',
        'Pekařství',
        'Řeznictví'
    ]
}

# Vytvoření DataFrame
df_kategorie = pd.DataFrame(data)

# Uložení do CSV souboru
output_file = 'kategorie_obchod.csv'
df_kategorie.to_csv(output_file, index=False, encoding='utf-8')

print(f"Soubor '{output_file}' byl úspěšně vytvořen.")
print("Obsah souboru:")
print(df_kategorie)