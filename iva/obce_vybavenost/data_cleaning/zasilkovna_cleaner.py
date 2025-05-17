"""
this script is used to clean the zasilkovna dataset
it reads data from the zasilkovna raw dataset (data/raw/zasilkovna_data.xlsx) and cleans the data:
1. split and clean the address field into street, number and city
    e.g. "{'name': 'Z-BOX Veverská Bítýška, Hvozdecká 134'}" will be split into "Hvozdecká", "134" and "Veverská Bítýška"
2. if the number contains "/" then split it into two columns: number and number2
    e.g. "85/16" will be split into "85" and "16"
3. if street ends with "nám." it will be replaced with "náměstí"
    e.g. "Komenského nám." will be replaced with "Komenského náměstí"
4. parse coordinates column to longitude and latitude
    e.g. {'latitude': 49.36891, 'longitude': 12.8581} will be split into "49.36891" and "12.8581"
5. select only the relevant columns and rename them
6. save the cleaned dataset to a csv file
the script also handles errors in the address format and raises a ValueError if the format is not as expected
"""

import pandas as pd
import ast

def clean_zasilkovna_data(input_file, output_file):
    # Read the raw dataset
    df = pd.read_excel(input_file)

    # Extract and clean the address field
    def parse_address(address):
        try:
            address_dict = ast.literal_eval(address)
            full_address = address_dict.get('name', '')
            parts = full_address.split(', ')

            if len(parts) == 1:
                # if there is no comma, we assume that the address is in the format "Z-BOX city number"
                # e.g. "Z-BOX Benešov nad Černou 20"
                address_part = parts[0].replace('Z-BOX ', '').strip()
                if not address_part[-1].isdigit():
                    # if not, we assume that the number is empty
                    number = ''
                    street = address_part
                else:
                    street, number = address_part.rsplit(' ', 1)
                city = street
                return pd.Series([street, number, city], index=['street', 'number', 'city'])

            # if there are 2 or more parts, consider only the first two parts
            elif len(parts) >= 2:
                # if there are more than 2 parts, we assume that the address is in the format "Z-BOX city, street number"
                # e.g. "Z-BOX Brno, Hvozdecká 134"
                city = parts[0].replace('Z-BOX ', '').strip()
                street_part = parts[1].strip()

                # the address is sometimes accompanied by a note in brackets which we want to remove
                # e.g. in {'name': 'Z-BOX Kdyně, Na Kobyle 209 (automyčka Comfort wapka)'} we want to remove the "(automyčka Comfort wapka)"
                
                # remove the note in brackets
                if ' (' in street_part:
                    street_part = street_part.split(' (')[0]
                
                # split the street and number
                # also handle the case when there is no number, e.g. "Soumarská ulice" should be split into "Soumarská ulice" and ""
                # check if there is a number at the end of the street part
                if not street_part[-1].isdigit():
                    # if not, we assume that the number is empty
                    number = ''
                    street = street_part
                else:            
                    street, number = street_part.rsplit(' ', 1)

                return pd.Series([street, number, city], index=['street', 'number', 'city'])            
                             
        except Exception as ex:
            raise ValueError(f"Address {address} format is not as expected: {ex}")            

    def parse_coordinates(coordinates):
        try:
            coordinates_dict = ast.literal_eval(coordinates)
            latitude = coordinates_dict.get('latitude', None)
            longitude = coordinates_dict.get('longitude', None)
            return pd.Series([latitude, longitude])
        except Exception:
            raise ValueError("Coordinates format is not as expected")
            # return pd.Series([None, None])
    
    # Parse the coordinates column
    df[['latitude', 'longitude']] = df['coordinates'].apply(parse_coordinates)

    # Parse the address column
    df[['street', 'number', 'city']] = df['address'].apply(parse_address)

    # Split the number into two columns if it contains '/'
    df[['number', 'number2']] = df['number'].str.split('/', expand=True)

    # Replace 'nám.' or 'nám.' with 'náměstí' in the street column
    # this should cater for bothj cases when "nám." is at the end of the street name or in the middle
    # if this is at the beginning or in the middle it should preserve the space 
    df['street'] = df['street'].str.replace(r'\bnám\.\s*', 'náměstí ', regex=True)

    # Remove leading and trailing spaces
    df['street'] = df['street'].str.strip()    

    # Select only the relevant columns and rename them
    relevant_columns = {  
        'branchCode': 'branch_code',     
        'name': 'name',
        'city': 'city',        
        'street': 'street',
        'number': 'building_number',
        'number2': 'orientation_number',
        'longitude': 'longitude',
        'latitude': 'latitude'        
    }
    df = df[list(relevant_columns.keys())].rename(columns=relevant_columns)

    # Save the cleaned dataset
    df.to_csv(output_file, index=False)

# Example usage
if __name__ == "__main__":
    input_path = "data/raw/zasilkovna_data.xlsx"
    output_path = "data/clean/zasilkovna_cleaned.csv"
    clean_zasilkovna_data(input_path, output_path)
