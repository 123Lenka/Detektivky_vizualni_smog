import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# Seznam PPL ID výdejních míst
ids = [
    4441996, 4442021, 4445468, 4445470, 4447573,
    4448494, 4449200, 4449211, 4450249, 4452846
]

results = []

for vid in ids:
    url = f"https://www.ppl.cz/vydejni-mista/_{vid}"
    print(f"Načítám ID: {vid} → {url}")

    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")

        # Najdi adresní blok
        address_div = soup.find("div", class_="detail__address")
        address_lines = address_div.find_all("div")

        if len(address_lines) >= 2:
            street_line = address_lines[0].text.strip()        # např. "Sadová 1008"
            city_line = address_lines[1].text.strip()          # např. "768 24 Hulín"

            # Zpracuj město a PSČ
            psc, city = city_line.split(" ", 1)

            # Rozdělení čísla orientačního/popisného
            parts = street_line.split()
            if parts[-1].isdigit():
                street = " ".join(parts[:-1])
                house_number = parts[-1]
            else:
                street = street_line
                house_number = ""

            results.append({
                "id": vid,
                "street": street,
                "house_number": house_number,
                "psc": psc,
                "city": city
            })
        else:
            raise ValueError("Adresní blok je neúplný")

        time.sleep(1)  # Ochrana proti přetížení

    except Exception as e:
        print(f"Chyba u ID {vid}: {e}")
        results.append({
            "id": vid,
            "street": None,
            "house_number": None,
            "psc": None,
            "city": None
        })

# Výstup do DataFrame
df = pd.DataFrame(results)
print(df)

# Volitelné uložení
df.to_csv("ppl_adresy.csv", index=False, encoding="utf-8-sig")
