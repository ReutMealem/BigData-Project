from create_df_from_sql import *

def main(task_id):

    query_w = """SELECT id AS work_id, publication_year, publication_date, cited_by_count 
                FROM openalex.works as w 
                WHERE w.publication_year > 1950;"""

    query_a = "SELECT id AS author_id, display_name AS author_name FROM openalex.authors;"
    
    query_i = "SELECT id AS institution_id, display_name AS institution_name, country_code FROM openalex.institutions;"

   
    queries_and_files = [
        (query_w, "works_df"),
        (query_a,"author_df"),
        (query_i, "inst_df")
    ]

    # Get the query and output file for the current task ID
    query, output_file = queries_and_files[task_id]

    # Run the SQL query and save the result
    sql_query(const.db_params, query, output_file, batch_size=500000)

if __name__ == "__main__":
    # Get the SLURM_ARRAY_TASK_ID from the command line
    task_id = int(sys.argv[1])
    main(task_id)


