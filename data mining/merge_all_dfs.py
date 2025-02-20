from create_df_from_sql import *
import vaex
import os
import const


def main():


    # Define the base directory
    base_dir = const.data_base_dir

    # Subdirectories
    subdirectories = ["authors", "institutions", "work_inst_auth", "works"]

    # Initialize a dictionary to hold the dataframes
    dataframes = {}

    # Loop through each subdirectory and read all the parquet files
    for subdirectory in subdirectories:
        # Path to the subdirectory
        sub_dir_path = os.path.join(base_dir, subdirectory)
        
        print(f"start reading {sub_dir_path} files")
        # Collect all parquet file paths
        parquet_files = [os.path.join(sub_dir_path, file) for file in os.listdir(sub_dir_path) if file.endswith('.parquet')]
        print(f"amount of parquet files to read is {len(parquet_files)}")

        # Read all parquet files into a single Vaex dataframe
        if parquet_files:
            df = vaex.open_many(parquet_files)
            dataframes[subdirectory] = df
            print(f"finish reading {subdirectory} files")
        else:
            print(f"No parquet files found in {sub_dir_path}")

    # Access your dataframes using the keys
    for name, df in dataframes.items():
        print(f"Dataframe for {name} has {len(df)} rows and {len(df.columns)} columns.")
    
    df_wa = dataframes['work_inst_auth']
    df_w = dataframes['works']
    df_a = dataframes['authors']
    df_i = dataframes['institutions']

    df_merged = df_wa.join(df_w, on="work_id", how="left")

    # Join the result with df_a on "author_id"
    df_merged = df_merged.join(df_a, on="author_id", how="left")

    # Join the result with df_i on "institution_id"
    df_merged = df_merged.join(df_i, on="institution_id", how="left")

    print(f"Merge data frame 5 first rows: /n{df_merged.head(5)}")
    print(f"The shape of the data frame is {df_merged.shape}")

    # Optionally, save as a Parquet or CSV
    output_parquet = os.path.join(base_dir, "OpenAlex_merged_data.parquet")
    
    # Save as parquet
    df_merged.export_parquet(output_parquet)
    print(f"Merged DataFrame saved to {output_parquet}")


if __name__ == "__main__":
    main()

