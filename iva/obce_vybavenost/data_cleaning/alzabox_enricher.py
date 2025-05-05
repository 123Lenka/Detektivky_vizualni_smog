"""
this script is used to enrich the alzabox data with adddress data from the adresy_cr dataset
the script reads the cleaned alzabox dataset (data/clean/alzaboxes_cleaned.csv) and the cleaned adresy_cr dataset (data/clean/adresy_cr/combined_adresy_cr_cleaned.csv)
it then merges the two datasets on the city and postal code columns
"""

import pandas as pd

def enrich_alzabox_data(alzabox_file, adresy_file, output_file):
    # Read the cleaned alzabox dataset
    alzabox_df = pd.read_csv(alzabox_file)

    # Read the cleaned adresy_cr dataset
    addresses_df = pd.read_csv(adresy_file)

    # Select only the relevant columns from the addresses_df dataset
    relevant_columns = ['city_code', 'city','postal_code']
    addresses_df = addresses_df[relevant_columns]
    addresses_df = addresses_df.drop_duplicates()
    
    # Apply case insensitive merge

    alzabox_df['city_lower'] = alzabox_df['city'].str.lower()
    addresses_df['city_lower'] = addresses_df['city'].str.lower()

    # remove city part number if exists; e.f. "Praha 1" -> "Praha"    
    alzabox_df['city_lower'] = alzabox_df['city_lower'].str.replace(r'\s\d+', '', regex=True)

    # Merge the two datasets on the city and postal_code columns
    enriched_df = pd.merge(alzabox_df, addresses_df, on=['city_lower', 'postal_code'], how='left')

    # Drop the temporary columns used for merging
    enriched_df = enriched_df.drop(columns=['city_lower',])

    # drop all the columns with _y suffix
    enriched_df = enriched_df.loc[:, ~enriched_df.columns.str.endswith('_y')]

    # rename the columns with _x suffix to remove the suffix
    enriched_df = enriched_df.rename(columns=lambda x: x.replace('_x', ''))

    # Drop duplicates if any
    enriched_df = enriched_df.drop_duplicates()

    # print number of rows with missing addresses_df and those with matching addresses_df
    missing_addresses = enriched_df[enriched_df['city_code'].isnull()]
    overall_count = len(enriched_df)    
    print(f"Number of rows with missing addresses_df: {len(missing_addresses)} out of {overall_count}, which is {len(missing_addresses)/overall_count:.2%}")

    # Save the enriched dataset
    enriched_df.to_csv(output_file, index=False)

# Example usage
if __name__ == "__main__":
    alzabox_path = "data/clean/alzaboxes_cleaned.csv"
    adresy_path = "data/clean/adresy_cr/combined_addresses_cz_cleaned.csv"
    output_path = "data/clean/alzaboxes_enriched.csv"
    enrich_alzabox_data(alzabox_path, adresy_path, output_path)