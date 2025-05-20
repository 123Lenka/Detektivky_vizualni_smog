import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# Seznam ID z výdejních míst
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

        # Najdi blok s adresou (může se změnit, závisí na HTML)
        address_div = soup.find("div", class_="detail__address")
        city_psc = address_div.find_all("div")[1].text.strip()  # Např. "768 24 Hulín"

        # Rozděl na PSČ a město
        psc, city = city_psc.split(" ", 1)

        results.append({
            "id": vid,
            "psc": psc,
            "city": city
        })

        time.sleep(1)  # Šetrné načítání

    except Exception as e:
        print(f"Chyba u ID {vid}: {e}")
        results.append({
            "id": vid,
            "psc": None,
            "city": None
        })

# Výstup jako DataFrame
df = pd.DataFrame(results)
print(df)

# Volitelně uložit jako CSV
df.to_csv("vystup_mesta_ppl.csv", index=False, encoding="utf-8-sig")
