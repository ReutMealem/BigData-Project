import psycopg2
import pandas as pd
import vaex
import sys
import os
base_dir = '/home/reutme/Big_data/final_project'
code_dir = os.path.join(base_dir, 'code')
sys.path.append(code_dir)
import params



def sql_query(db_params, query, save_file_name=None, batch_size=10000):
    """
    Execute a SQL query and fetch results in batches, converting them to Vaex DataFrames.

    Parameters:
    - db_params (dict): Database connection parameters.
    - query (str): SQL query to execute.
    - save_file_name (str, optional): Name for saving the output Parquet files.
    - batch_size (int): Number of rows to fetch per batch.

    Returns:
    - Vaex DataFrame or None if saved to file.
    """

    try:
        connection = psycopg2.connect(**db_params)
        print("Connection to the database established.")

        cursor = connection.cursor(name="bulk_fetch_cursor")  
        print("Cursor created successfully.")

        cursor.execute(query)
        print("Query executed successfully.")

        # Fetch rows in chunks and append to a list for Vaex conversion
        data = []
        idx = 0
        while True:
            rows = cursor.fetchmany(batch_size)

            if cursor.description is None:
                print("Error: Cursor description is None. Query execution might have failed.")
                return None

            # Create column names from the cursor description
            columns = [desc[0] for desc in cursor.description]

            if rows is None:  
                print("Error: fetchmany returned None instead of an empty list.")
                break

            if not rows:
                print("No more rows to fetch. Exiting.")

                if len(data) > 0:
                    df_vaex = vaex.from_arrays(**{col: [row[i] for row in data] for i, col in enumerate(columns)})
                    print(f"Total shape fetched: {df_vaex.shape}")

                    if save_file_name:
                        save_file_path = f'/home/reutme/Big_data/final_project/data/{save_file_name}_{idx}.parquet'
                        df_vaex.export_parquet(save_file_path)
                        print(f"Data saved to {save_file_name}_{idx} successfully!")
                    else:
                        return df_vaex
                break

            data.extend(rows)

            # Convert and save every X rows
            if len(data) % 10000000 == 0 or not rows:
                df_vaex = vaex.from_arrays(**{col: [row[i] for row in data] for i, col in enumerate(columns)})
                print(f"Total shape fetched: {df_vaex.shape}")

                if save_file_name:
                    save_file_path = f'/home/reutme/Big_data/final_project/data/{save_file_name}_{idx}.parquet'
                    df_vaex.export_parquet(save_file_path)
                    print(f"Data saved to {save_file_name}_{idx} successfully!")
                
                idx += 1
                data = []

    except Exception as e:
        print("Error occurred:", e)
        return None

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("Database connection closed.")

    return None



def sql_query_relatioship_table(db_params, query):
    """
    Execute a SQL query to fetch all results at once and convert them into a Vaex DataFrame.

    Parameters:
    - db_params (dict): Database connection parameters.
    - query (str): SQL query to execute.

    Returns:
    - Vaex DataFrame containing the fetched results.
    """

    try:
        connection = psycopg2.connect(**db_params)
        print("Connection to the database established.")

        cursor = connection.cursor(name="bulk_fetch_cursor")  
        print("Cursor created successfully.")

        cursor.execute(query)
        print("Query executed successfully.")

        # Fetch rows in chunks and append to a list for Vaex conversion

        rows = cursor.fetchall()

        if cursor.description is None:
            print("Error: Cursor description is None. Query execution might have failed.")
            return None

        # Create column names from the cursor description
        columns = [desc[0] for desc in cursor.description]

        if not rows:
            print("No more rows to fetch. Exiting.")

        df_vaex = vaex.from_arrays(**{col: [row[i] for row in rows] for i, col in enumerate(columns)})
        print(f"Total shape fetched: {df_vaex.shape}")
 
    except Exception as e:
        print("Error occurred:", e)
        return None

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("Database connection closed.")
    
    return df_vaex




def proccess_queries_chunks(work_id_list, save_file_name=None, chunk_size = 500000):
    """
    Process work ID chunks by querying authorship relationships and saving combined results.

    Parameters:
    - work_id_list (list): List of work IDs to be processed.
    - save_file_name (str, optional): Name for saving the combined output Parquet file.
    - chunk_size (int): Number of work IDs to process per chunk.

    Returns:
    - None: Saves the combined DataFrame as a Parquet file if `save_file_name` is provided.
    """

    dataframes = []

    for i in range(0, len(work_id_list), chunk_size):
        chunk = work_id_list[i:i + chunk_size]
        formatted_work_ids = ', '.join([f"'{work_id}'" for work_id in chunk])
        
        query_wa = f"""
            SELECT work_id, author_id, institution_id 
            FROM openalex.works_authorships as wa
            WHERE wa.work_id IS NOT NULL 
            AND wa.author_id IS NOT NULL 
            AND wa.institution_id IS NOT NULL
            AND wa.work_id IN ({formatted_work_ids});
        """
        df = sql_query_relatioship_table(params.db_params, query_wa)

        if df is not None:
            dataframes.append(df)
    
    # Concatenate all the DataFrames
    if dataframes:
        combined_df = vaex.concat(dataframes)
        print(f"Combined DataFrame shape: {combined_df.shape}")

        # Save the combined DataFrame if a file name is provided
        if save_file_name:
            save_file_path = f'/home/reutme/Big_data/final_project/data/{save_file_name}.parquet'
            combined_df.export_parquet(save_file_path)
            print(f"Data saved to {save_file_name}.parquet successfully!")
        
    else:
        print("No dataframes were created.")
        return None