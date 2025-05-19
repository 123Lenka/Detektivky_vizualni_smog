"""
this script is used to enrich the zasilkovna data with adddress data from the adresy_cr dataset
this is particularly useful to enrich zasilkovna data with city_code from adresy_cr dataset
the script reads the cleaned alzabox dataset (data/clean/adresy_cr/combined_addresses_cz_cleaned.csv) and the cleaned adresy_cr dataset (data/clean/adresy_cr/combined_adresy_cr_cleaned.csv)
it will attempt to merge in 2 steps:
1. first it will merge the zasilkovna dataset with the adresy_cr dataset on the city column only; 
   this would only work for addresses with one unique city_code per city
   thus we need to split the adddress_cr dataset into two datasets: one with unique city_code per city and one with multiple city_codes per city
2. then it will merge the zasilkovna dataset with the adresy_cr dataset on the street and city columns
   this merge would only be appplied to the addresses with multiple city_codes per city
"""

import pandas as pd
import numpy as np

def merge_on_city(zasilkovna_df: pd.DataFrame, addresses_df: pd.DataFrame) -> pd.DataFrame:
    """ Merge the two datasets on the city column """

    # select only the relevant columns from the addresses_df dataset
    city_addresses_df = addresses_df[['city_code', 'city_lower']]

    # select only the relevant columns from the zasilkovna dataset
    city_zasilkovna_df = zasilkovna_df[['branch_code','city_lower']]
    
    # drop duplicates
    city_addresses_df = city_addresses_df.drop_duplicates()

    # Merge the two datasets on the city column
    merged_df = pd.merge(city_zasilkovna_df, city_addresses_df, on='city_lower', how='inner')

    # Drop duplicates if any
    merged_df = merged_df.drop_duplicates()

    return merged_df

def merge_on_city_and_street(zasilkovna_df: pd.DataFrame, addresses_df: pd.DataFrame) -> pd.DataFrame:
    """ Merge the two datasets on the street and city columns """

    # select only the relevant columns from the zasilkovna dataset
    city_zasilkovna_df = zasilkovna_df[['branch_code','city_lower','street_lower']]

    # select only those from city_zasilkovna_df with street_lower
    city_zasilkovna_df = city_zasilkovna_df[city_zasilkovna_df['street_lower'] != '']

    # Merge the two datasets on the street and city columns
    merged_df = pd.merge(city_zasilkovna_df, addresses_df, on=['city_lower', 'street_lower'], how='inner')

    # Drop duplicates if any
    merged_df = merged_df.drop_duplicates()    

    return merged_df

def split_addresses_by_uniqueness(addresses_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split the addresses_df dataset into two datasets: one with unique city_code per city and one with multiple city_codes per city
    this is done by grouping the addresses_df dataset by city and counting the number of unique city_codes per city
    this is done to avoid duplicates in the merge process
    """
    
    # split the addresses_df dataset into two datasets: one with unique city_code per city and one with multiple city_codes per city
    # this is done by grouping the addresses_df dataset by city and counting the number of unique city_codes per city
    city_counts = addresses_df.groupby('city')['city_code'].nunique().reset_index()
    city_counts = city_counts.rename(columns={'city_code': 'unique_city_codes_count'})
    
    # merge the city_counts dataset with the addresses_df dataset
    addresses_df = pd.merge(addresses_df, city_counts, on='city', how='left')
    
    # split the addresses_df dataset into two datasets: one with unique city_code per city and one with multiple city_codes per city
    unique_city_codes_df = addresses_df[addresses_df['unique_city_codes_count'] == 1]
    multiple_city_codes_df = addresses_df[addresses_df['unique_city_codes_count'] > 1]
    
    # drop the unique_city_codes_count column
    unique_city_codes_df = unique_city_codes_df.drop(columns=['unique_city_codes_count'])
    multiple_city_codes_df = multiple_city_codes_df.drop(columns=['unique_city_codes_count'])

    return unique_city_codes_df, multiple_city_codes_df

def prepare_dataset(zasilkovna_df: pd.DataFrame, addresses_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Prepare the datasets for merging
    1. Select only the relevant columns from the zasilkovna dataset
    2. Select only the relevant columns from the addresses_df dataset
    3. Replace NaN values with None
    4. Cast the columns to string
    5. Remove leading and trailing spaces
    6. Apply case insensitive merge
    7. Remove city part number if exists; e.f. "Praha 1" -> "Praha"
    8. If street is the same as city, then set street to empty string
    """
    
    # Select only the relevant columns from the addresses_df dataset
    relevant_columns = ['city_code', 'city','street']
    addresses_df = addresses_df[relevant_columns]

    # drop duplicates
    addresses_df = addresses_df.drop_duplicates()

    # replace NaN values with None
    addresses_df = addresses_df.replace(np.nan, '')

    # cast the columns to string
    zasilkovna_df['street'] = zasilkovna_df['street'].astype(str)
    zasilkovna_df['city'] = zasilkovna_df['city'].astype(str)
    addresses_df['street'] = addresses_df['street'].astype(str)
    addresses_df['city'] = addresses_df['city'].astype(str)
    
    # Remove leading and trailing spaces
    zasilkovna_df['street'] = zasilkovna_df['street'].str.strip()
    zasilkovna_df['city'] = zasilkovna_df['city'].str.strip()
    addresses_df['street'] = addresses_df['street'].str.strip()
    addresses_df['city'] = addresses_df['city'].str.strip()
    
    # Apply case insensitive merge
    zasilkovna_df['street_lower'] = zasilkovna_df['street'].str.lower()
    addresses_df['street_lower'] = addresses_df['street'].str.lower()
    zasilkovna_df['city_lower'] = zasilkovna_df['city'].str.lower()
    addresses_df['city_lower'] = addresses_df['city'].str.lower()

    # remove city part number if exists; e.f. "Praha 1" -> "Praha"    
    zasilkovna_df['city_lower'] = zasilkovna_df['city_lower'].str.replace(r'\s\d+', '', regex=True)

    # if street is the same as city, then set street to empty string
    zasilkovna_df.loc[zasilkovna_df['street_lower'] == zasilkovna_df['city_lower'], 'street_lower'] = ''

    return zasilkovna_df, addresses_df


def enrich_zasilkovna_data(zasilkovna_file, adresy_file, output_file):
    """
    Enrich the zasilkovna dataset with address data from the adresy_cr dataset
    """
    
    # Read the cleaned zasilkovna dataset
    zasilkovna_df = pd.read_csv(zasilkovna_file)

    # Read the cleaned adresy_cr dataset
    addresses_df = pd.read_csv(adresy_file)

    # Prepare the datasets
    zasilkovna_df, addresses_df = prepare_dataset(zasilkovna_df, addresses_df)

    # split the addresses_df dataset into two datasets: one with unique city_code per city and one with multiple city_codes per city
    unique_city_codes_df, multiple_city_codes_df = split_addresses_by_uniqueness(addresses_df)
    
    # merge the zasilkovna dataset with the unique city_codes_df dataset on the city column
    city_merged_df = merge_on_city(zasilkovna_df, unique_city_codes_df)

    # merge the zasilkovna dataset with the multiple city_codes_df dataset on the street and city columns
    city_street_merged_df = merge_on_city_and_street(zasilkovna_df, multiple_city_codes_df)

    # merge the two datasets
    enriched_df = pd.concat([city_merged_df, city_street_merged_df], ignore_index=True)
    
    # Drop duplicates if any
    enriched_df = enriched_df.drop_duplicates()

    # left merge eith the original zasilkovna dataset to keep all the rows
    zasilkovna_merged_df = pd.merge(zasilkovna_df, enriched_df, on=['branch_code'], how='left')

    # drop all the columns with _y suffix
    zasilkovna_merged_df = zasilkovna_merged_df.loc[:, ~zasilkovna_merged_df.columns.str.endswith('_y')]

    # rename the columns with _x suffix to remove the suffix
    zasilkovna_merged_df = zasilkovna_merged_df.rename(columns=lambda x: x.replace('_x', ''))

    # Drop the temporary columns used for merging
    zasilkovna_merged_df = zasilkovna_merged_df.drop(columns=['street_lower', 'city_lower'])

    # Drop duplicates if any
    zasilkovna_merged_df = zasilkovna_merged_df.drop_duplicates()

    # print number of rows with missing addresses_df and those with matching addresses_df
    missing_addresses = zasilkovna_merged_df[zasilkovna_merged_df['city_code'].isnull()]
    missing_addreesses_count = len(missing_addresses)
    overall_count = len(zasilkovna_merged_df)
    print(f"Number of rows with missing addresses_df: {missing_addreesses_count} out of {overall_count}, which is {missing_addreesses_count/overall_count:.2%}")    

    # Save the enriched dataset
    zasilkovna_merged_df.to_csv(output_file, index=False)

# Example usage
if __name__ == "__main__":
    zasilkovna_path = "data/clean/zasilkovna_cleaned.csv"
    adresy_path = "data/clean/adresy_cr/combined_addresses_cz_cleaned.csv"
    output_path = "data/clean/zasilkovna_enriched.csv"
    enrich_zasilkovna_data(zasilkovna_path, adresy_path, output_path)