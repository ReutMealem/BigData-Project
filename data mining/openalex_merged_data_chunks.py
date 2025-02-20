import pandas as pd
import vaex
import ast 
import pickle
import sys
import os
sys.path.append('/home/reutme/Big_data/final_project/code/')
import params



def process_chunk(chunk, war_lookup):
    """
    Process a data chunk by adding war-related information.

    Parameters:
    - chunk (DataFrame): Pandas DataFrame containing author data.
    - war_lookup (dict): Dictionary with war data indexed by (Year, Country Code).

    Returns:
    - DataFrame: Processed DataFrame with additional columns:
        * war_exist: 1 if a war existed for the publication year and country, else 0.
        * year_in_war: Number of years since the start of the war, else 0.
        * war_name: Name of the war if applicable, else 'No War'.
    """
    
    print("Add war exist")
    # Vectorized lookup for war information
    chunk["war_exist"] = chunk.apply(
        lambda row: 1 if (row["publication_year"], row["country_code"]) in war_lookup else 0,
        axis=1
    )
    
    print("Add year in war")
    chunk["year_in_war"] = chunk.apply(
        lambda row: row["publication_year"] - war_lookup.get((row["publication_year"], row["country_code"]), {}).get("Min_Year", None)
        if (row["publication_year"], row["country_code"]) in war_lookup else 0,
        axis=1
    )
    
    print("Add war name")
    chunk["war_name"] = chunk.apply(
        lambda row: war_lookup.get((row["publication_year"], row["country_code"]), {}).get("War Name", "No War")
        if (row["publication_year"], row["country_code"]) in war_lookup else "No War",
        axis=1
    )
    
    return chunk

if __name__ == '__main__':
    task_id = int(sys.argv[1])

        
    # Path to the Parquet file
    file_path = "/home/reutme/Big_data/final_project/data/OpenAlex_merged_data.parquet"
    # Read the Parquet file into a Vaex dataframe
    df = vaex.open(file_path)
    author_data = df[(df['publication_year'] < 2023)]
    # Select only the required columns
    columns_to_keep = ['author_id','author_name', 'publication_year', 'institution_name' ,'country_code']
    author_data_cleaned = author_data[columns_to_keep]


    merged_wars_data_file = '/home/reutme/Big_data/final_project/data/merged_war_data_after_expand.csv'
    war_data = pd.read_csv(merged_wars_data_file)
    war_data_unique = war_data.drop_duplicates(subset=["Year", "Country Code"])
    war_lookup = war_data_unique.set_index(["Year", "Country Code"]).to_dict(orient="index")


    # Directory to save processed Parquet files
    output_dir = "/home/reutme/Big_data/final_project/data/processed_chunks_combine_war_openalex"
    os.makedirs(output_dir, exist_ok=True)

    year = params.years_dict[task_id]
    print(f"start with year {year}")
    print("start convert to pandas")
    chunk = params.get_year_df(author_data_cleaned, task_id).to_pandas_df()  # Convert Vaex chunk to Pandas
    print("finish convert to pandas")
    print("start add war information")
    processed_chunk = process_chunk(chunk, war_lookup)  # Process the chunk
    print("finish add war information")
    processed_chunk.to_parquet(os.path.join(output_dir, f"year_{year}.parquet"), index=False)  # Save as Parquet
    print(f"finish with year {year}")


