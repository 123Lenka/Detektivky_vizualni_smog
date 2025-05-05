"""
this script is used to clean the alzabox dataset
it reads data from the alzaboxes raw dataset (data/raw/alzaboxes_cz.xlsx) and cleans the data:
1. split the address ("Ulice a číslo") into street, number, 
    e.g. "Náměstí Svobody 85/16" will be split into "Náměstí Svobody" and "85/16"
2. if the number contains "/" then split it into two columns: number and number2
    e.g. "85/16" will be split into "85" and "16"
3. if street ends with "nám." or "nám." it will be replaced with "náměstí"
    e.g. "Komenského nám." will be replaced with "Komenského náměstí"
"""

import pandas as pd

def clean_alzabox_data(input_file, output_file):
    # Read the raw dataset
    df = pd.read_excel(input_file)

    # Split the address into street and number
    df[['Street', 'Number']] = df['Ulice a číslo'].str.extract(r'(.+?)\s+(\d+/\d+|\d+)')

    # Split the number into two columns if it contains '/'
    df[['Number', 'Number2']] = df['Number'].str.split('/', expand=True)

    # Replace 'nám.' or 'nám.' with 'náměstí' in the street column
    df['street'] = df['street'].str.replace(r'\bnám\.\s*', 'náměstí ', regex=True)

    # Remove leading and trailing spaces
    df['street'] = df['street'].str.strip()    

    # select only the relevant columns and rename them
    relevant_columns = {
        'Branch office': 'branch_office',
        'Název': 'name',
        'Město': 'city',
        'PSČ': 'postal_code',
        'Street': 'street',
        'Number': 'building_number',
        'Number2': 'orientation_number',
        'GeoX': 'longitude',
        'GeoY': 'latitude',
        'První spuštění': 'first_launch',
        'Otevřen od': 'opened_from',
        'Otevřen do': 'opened_to'
    }
    df = df[list(relevant_columns.keys())].rename(columns=relevant_columns)
    
    # Save the cleaned dataset
    df.to_csv(output_file, index=False)

# usage
if __name__ == "__main__":
    input_path = "data/raw/alzaboxes_cz.xlsx"
    output_path = "data/clean/alzaboxes_cleaned.csv"
    clean_alzabox_data(input_path, output_path)