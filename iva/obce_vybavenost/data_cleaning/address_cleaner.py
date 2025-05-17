import os
import pandas as pd
import chardet


# Define the directory containing the CSV files
csv_directory = "data/raw/adresy_cr/"
# Define the output directory for cleaned CSV files
output_directory = "data/clean/adresy_cr/"
# Create the output directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)

# def detect_encoding(file_path):
#     """
#     Detect the encoding of a file.
#     """
#     with open(file_path, "rb") as f:
#         result = chardet.detect(f.read(10000))  # přečte prvních 10 kB
#         return result['encoding']


# read the csv files from the directory, convert from cp1252 to utf-8 encoding and join into one file
def read_and_convert_csv(file_path):
    # source_encoding = detect_encoding(file_path)
    source_encoding = 'windows-1250'  # works best with windows-1250 encoding
    print(f"Detected encoding for {file_path}: {source_encoding}")
    
    df = pd.read_csv(file_path, encoding=source_encoding)
    # Convert to utf-8 encoding
    df.to_csv(file_path, index=False, encoding='utf-8')
    return df

# Initialize an empty list to store DataFrames
dataframes = []
# Iterate through all files in the directory
for filename in os.listdir(csv_directory):
    if filename.endswith(".csv"):
        # Construct the full file path        
        file_path = os.path.join(os.getcwd(),csv_directory, filename)
        print(f"Processing {file_path}...")
        # Read and convert the CSV file
        df = read_and_convert_csv(file_path)
        # Append the DataFrame to the list
        dataframes.append(df)

# Concatenate all DataFrames into a single DataFrame
combined_df = pd.concat(dataframes, ignore_index=True)

# Save the combined DataFrame to a new CSV file
output_file_path = os.path.join(output_directory, "combined_adresses_cz.csv")
combined_df.to_csv(output_file_path, index=False, encoding='utf-8')
print(f"Combined CSV file saved to {output_file_path}")

# read again and select only the relevant columns and rename them
relevant_columns = {
    'Kód ADM': 'adm_code',
    'Kód obce': 'city_code',
    'Název obce': 'city',
    'Název části obce': 'city_part',
    'Kód části obce': 'city_part_code',
    'Název ulice': 'street',
    'Číslo domovní': 'building_number',
    'Číslo orientační': 'orientation_number',
    'PSČ': 'postal_code',
    'Souřadnice Y': 'latitude',
    'Souřadnice X': 'longitude',
    'Platí Od': 'valid_from'
}
combined_df = pd.read_csv(output_file_path, encoding='utf-8', header=0, sep=';')

# Select only the relevant columns and rename them
combined_df = combined_df[list(relevant_columns.keys())].rename(columns=relevant_columns)

# save the cleaned dataset
output_file_path = os.path.join(output_directory, "combined_addresses_cz_cleaned.csv")
combined_df.to_csv(output_file_path, index=False, encoding='utf-8')
print(f"Cleaned CSV file saved to {output_file_path}")

# also save the cleaned dataset in 100MB chunks to meet github file size limit
output_directory = "data/clean/"
chunk_size = 800000  # 1 million rows per chunk
chunk_count = 0
for i in range(0, len(combined_df), chunk_size):
    chunk = combined_df.iloc[i:i + chunk_size]
    chunk_file_path = os.path.join(output_directory, f"combined_addresses_cz_cleaned_chunk_{chunk_count}.csv")
    chunk.to_csv(chunk_file_path, index=False, encoding='utf-8')
    print(f"Cleaned CSV file chunk {chunk_count} saved to {chunk_file_path}")
    chunk_count += 1



